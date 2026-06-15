use crate::parser::ast::{ArithmeticOperator, ComparisonOperator, Literal};
use crate::parser::ast::LogicalOperator as BoolOperator;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::processor::PhysicalOperator;
use crate::storage::undo_buffer::UndoBuffer;
use crate::{LightningError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalPlanner {
    pub db: Arc<crate::Database>,
    pub read_ts: u64,
    pub tx_id: u64,
    pub undo_buffer: Arc<UndoBuffer>,
    pub masks: HashMap<String, Arc<RwLock<crate::processor::operators::semi_mask::SemiMask>>>,
    /// Binder-computed column offsets (variable name → starting column)
    /// for each variable. Used to remap PropertyLookup indices in projection
    /// items after optimizer transforms (e.g. join reordering) alter the
    /// physical column layout.
    pub binder_column_offsets: std::collections::HashMap<String, usize>,
}

impl PhysicalPlanner {
    pub fn new(
        db: Arc<crate::Database>,
        read_ts: u64,
        tx_id: u64,
        undo_buffer: Arc<UndoBuffer>,
    ) -> Self {
        Self {
            db,
            read_ts,
            tx_id,
            undo_buffer,
            masks: HashMap::new(),
            binder_column_offsets: std::collections::HashMap::new(),
        }
    }

    pub fn plan(&mut self, op: LogicalOperator) -> Result<Box<dyn PhysicalOperator + Send + Sync>> {
        match op {
            LogicalOperator::Scan(table_name, var, mask_info, projected_idxs, pushdown_filter) => {
                let table = {
                    let storage = self.db.storage_manager.read();
                    storage
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            LightningError::Internal(format!("Table {table_name} not found"))
                        })?
                        .clone()
                };
                let num_rows = {
                    let cat = self.db.catalog.read();
                    cat.get_node_table(&table_name)
                        .map(|t| t.num_rows)
                        .or_else(|| cat.get_rel_table(&table_name).map(|t| t.num_rows))
                        .unwrap_or(0)
                };
                // Use the storage table's next_row_id as the effective row count.
                // The catalog's num_rows is only updated during DDL and bulk_insert, but
                // regular DML (CREATE) bypasses it. The storage manager always has the
                // correct count via next_row_id and stats.cardinality.
                let effective_num_rows = table.next_row_id.load(std::sync::atomic::Ordering::Acquire).max(num_rows);
                let mut scan = crate::processor::operators::scan::PhysicalScan::new(
                    table,
                    var.clone(),
                    self.db.buffer_manager.clone(),
                    effective_num_rows,
                )?;
                if let Some((mask_id, col_idx)) = mask_info {
                    let mask = self
                        .masks
                        .get(&mask_id)
                        .ok_or_else(|| crate::LightningError::Internal("Mask not found".into()))?
                        .clone();
                    scan = scan.with_mask(mask, col_idx);
                }
                if let Some(idxs) = projected_idxs {
                    scan = scan.with_projected_idxs(idxs);
                }
                if let Some(filter) = pushdown_filter {
                    let planned_filter = self.plan_expression(
                        &LogicalOperator::Scan(table_name.clone(), var.clone(), None, None, None),
                        &filter,
                    )?;

                    if let Some(candidates) =
                        self.extract_trigram_candidates(&planned_filter, &scan.table)
                    {
                        if scan.mask.is_none() {
                            let mask = Arc::new(RwLock::new(
                                crate::processor::operators::semi_mask::SemiMask::new(),
                            ));
                            {
                                let mut m = mask.write();
                                for id in candidates {
                                    m.insert(id);
                                }
                            }
                            scan = scan.with_mask(mask, None);
                        } else {
                            let mask_col = scan.mask_column_idx;
                            let existing_mask = scan.mask.as_ref().ok_or_else(|| {
                                crate::LightningError::Internal("Expected mask on semi-join scan".into())
                            })?.clone();
                            let mask = Arc::new(RwLock::new(
                                crate::processor::operators::semi_mask::SemiMask::new(),
                            ));
                            {
                                let existing = existing_mask.read();
                                let mut m = mask.write();
                                for id in candidates {
                                    if existing.contains(id) {
                                        m.insert(id);
                                    }
                                }
                            }
                            scan = scan.with_mask(mask, mask_col);
                        }
                    }

                    scan = scan.with_filter(planned_filter);
                }
                Ok(Box::new(scan))
            }
            LogicalOperator::IndexScan(table_name, var, pk_name, pk_value_expr, projected_idxs) => {
                let table = {
                    let storage = self.db.storage_manager.read();
                    storage
                        .get_table(&table_name)
                        .ok_or_else(|| {
                            LightningError::Internal(format!("Table {table_name} not found"))
                        })?
                        .clone()
                };
                let index = {
                    let storage = self.db.storage_manager.read();
                    storage
                        .get_index(&table_name)
                        .ok_or_else(|| {
                            LightningError::Internal(format!(
                                "No index found for table {table_name}"
                            ))
                        })?
                };
                let mut scan = crate::processor::operators::index_scan::PhysicalIndexScan::new(
                    table_name,
                    table,
                    index,
                    pk_value_expr,
                    self.db.buffer_manager.clone(),
                    self.read_ts,
                );
                if let Some(idxs) = projected_idxs {
                    scan = scan.with_projected_idxs(idxs);
                }
                Ok(Box::new(scan))
            }
            LogicalOperator::Filter(child, mut expr) => {
                // Remap PropertyLookup indices in filter expressions to match
                // the physical plan layout (same rationale as Projection).
                let child_positions = self.compute_variable_positions(&child).unwrap_or_default();
                Self::remap_property_lookup(
                    &mut expr,
                    &child_positions,
                    &self.binder_column_offsets,
                );
                tracing::debug!(
                    "FILTER child_positions={:?} expr={:?}",
                    child_positions,
                    expr
                );
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::filter::PhysicalFilter::new(planned_child, expr),
                ))
            }
            LogicalOperator::Projection(child, items) => {
                // Compute physical variable positions from the unplanned child
                // before consuming it. These are needed to remap PropertyLookup
                // indices from binder-relative to physical-plan-relative offsets.
                let child_positions = self.compute_variable_positions(&child).unwrap_or_default();
                let planned_child = self.plan(*child)?;

                let mut remapped = items;
                for item in &mut remapped {
                    Self::remap_property_lookup(
                        &mut item.expression,
                        &child_positions,
                        &self.binder_column_offsets,
                    );
                }

                Ok(Box::new(
                    crate::processor::operators::projection::PhysicalProjection::new(
                        planned_child,
                        remapped,
                    ),
                ))
            }
            LogicalOperator::Join(left, right, join_cond) => {
                let is_cross = matches!(
                    join_cond,
                    BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
                );

                if is_cross {
                    let planned_left = self.plan(*left)?;
                    let planned_right = self.plan(*right)?;
                    Ok(Box::new(
                        crate::processor::operators::hash_join::HashJoin::new_cross_join(
                            planned_left,
                            planned_right,
                        ),
                    ))
                } else {
                    // Compute positions from child operators before planning
                    let mut left_positions = std::collections::HashMap::new();
                    self.collect_variable_positions(&left, 0, &mut left_positions)?;
                    let mut right_positions = std::collections::HashMap::new();
                    self.collect_variable_positions(&right, 0, &mut right_positions)?;

                    // Collect all equality comparisons
                    let mut comparisons = Vec::new();
                    self.collect_eq_comparisons(&join_cond, &mut comparisons);

                    // Find the best cross-boundary comparison for the hash join key
                    let mut key_idx: Option<usize> = None;
                    for (i, (left_expr, right_expr)) in comparisons.iter().enumerate() {
                        let a_in_left = self.expr_belongs_to_side(left_expr, &left_positions);
                        let a_in_right = self.expr_belongs_to_side(left_expr, &right_positions);
                        let b_in_left = self.expr_belongs_to_side(right_expr, &left_positions);
                        let b_in_right = self.expr_belongs_to_side(right_expr, &right_positions);

                        // Canonical: left-expr ONLY in left, right-expr ONLY in right
                        if a_in_left && !a_in_right && b_in_right && !b_in_left {
                            key_idx = Some(i);
                            break;
                        }
                    }
                    // Fallback: any cross-boundary comparison (prefer canonical order)
                    if key_idx.is_none() {
                        for (i, (left_expr, right_expr)) in comparisons.iter().enumerate() {
                            let a_in_left = self.expr_belongs_to_side(left_expr, &left_positions);
                            let a_in_right = self.expr_belongs_to_side(left_expr, &right_positions);
                            let b_in_left = self.expr_belongs_to_side(right_expr, &left_positions);
                            let b_in_right = self.expr_belongs_to_side(right_expr, &right_positions);
                            if a_in_left && b_in_right {
                                key_idx = Some(i);
                                break;
                            }
                            if b_in_left && a_in_right {
                                key_idx = Some(i);
                                break;
                            }
                        }
                    }

                    let key_i = key_idx.unwrap_or(0);
                    let (key_left_expr, key_right_expr) = &comparisons[key_i];
                    let (left_key, right_key) = self.resolve_join_key_pair(
                        key_left_expr, key_right_expr, &left_positions, &right_positions,
                    )?;

                    // Build filter from remaining comparisons
                    // The hash join output = left columns + right columns
                    let left_ncols = self.compute_subtree_num_cols(&*left);
                    let mut combined_positions = left_positions.clone();
                    for (var, pos) in &right_positions {
                        combined_positions.insert(var.clone(), pos + left_ncols);
                    }

                    let mut filter_conds = Vec::new();
                    for (i, (l, r)) in comparisons.iter().enumerate() {
                        if i == key_i { continue; }
                        let mut cond = BoundExpression::Comparison(
                            Box::new((*l).clone()),
                            ComparisonOperator::Equal,
                            Box::new((*r).clone()),
                        );
                        // Remap PropertyLookup indices to physical positions
                        // in the hash join output (left cols + right cols)
                        Self::remap_property_lookup(
                            &mut cond,
                            &combined_positions,
                            &self.binder_column_offsets,
                        );
                        filter_conds.push(cond);
                    }

                    let planned_left = self.plan(*left)?;
                    let planned_right = self.plan(*right)?;

                    let join_op = crate::processor::operators::hash_join::HashJoin::new(
                        planned_left,
                        planned_right,
                        left_key,
                        right_key,
                    );

                    if filter_conds.is_empty() {
                        Ok(Box::new(join_op))
                    } else {
                        let filter = filter_conds.into_iter().reduce(|acc, cond| {
                            BoundExpression::Logical(
                                Box::new(acc),
                                crate::parser::ast::LogicalOperator::And,
                                Box::new(cond),
                            )
                        }).unwrap();
                        Ok(Box::new(
                            crate::processor::operators::filter::PhysicalFilter::new(Box::new(join_op), filter),
                        ))
                    }
                }
            }
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                aggregates,
                ..
            } => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::aggregate::Aggregate::new(
                        planned_child,
                        group_by_cols,
                        aggregates,
                    ),
                ))
            }
            LogicalOperator::Sort(child, items) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::sort::PhysicalSort::new(planned_child, items),
                ))
            }
            LogicalOperator::TopK(child, items, limit) => {
                let planned_child = self.plan(*child)?;
                let sort = Box::new(
                    crate::processor::operators::sort::PhysicalSort::new(planned_child, items),
                );
                Ok(Box::new(
                    crate::processor::operators::limit_skip::PhysicalLimit::new(
                        sort,
                        limit as usize,
                    ),
                ))
            }
            LogicalOperator::Unwind(child, expr, alias) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::unwind::PhysicalUnwind::new(
                        planned_child,
                        expr.clone(),
                        alias.clone(),
                    ),
                ))
            }
            LogicalOperator::Limit(child, limit) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::limit_skip::PhysicalLimit::new(
                        planned_child,
                        limit as usize,
                    ),
                ))
            }
            LogicalOperator::Skip(child, skip) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::limit_skip::PhysicalSkip::new(
                        planned_child,
                        skip as usize,
                    ),
                ))
            }
            LogicalOperator::CreateNode(child, pat) => {
                let planned_child = child.map(|c| self.plan(*c)).transpose()?;
                let storage = self.db.storage_manager.read();
                let table = storage.get_table(&pat.table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table '{}' not found for CREATE", pat.table_name))
                })?.clone();
                Ok(Box::new(
                    crate::processor::operators::dml::PhysicalCreate::new(
                        pat.table_name,
                        self.db.catalog.clone(),
                        self.db.storage_manager.clone(),
                        table,
                        pat.properties,
                        self.db.buffer_manager.clone(),
                        self.undo_buffer.clone(),
                        planned_child,
                        self.tx_id,
                    ),
                ))
            }
            LogicalOperator::CreateRel(child, pat) => {
                let (src_idx, dst_idx) = if let Some(ref child_op) = child {
                    let positions = self.compute_variable_positions(child_op)?;
                    let src_idx = positions.get(&pat.src_variable).copied().unwrap_or(0);
                    let dst_idx = positions.get(&pat.dst_variable).copied().unwrap_or(1);
                    (src_idx, dst_idx)
                } else {
                    (0, 1)
                };
                let planned_child = child.map(|c| self.plan(*c)).transpose()?;
                let storage = self.db.storage_manager.read();
                let table = storage.get_table(&pat.table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table '{}' not found for CREATE REL", pat.table_name))
                })?.clone();
                Ok(Box::new(
                    crate::processor::operators::dml::PhysicalCreateRel::new(
                        pat.table_name,
                        table,
                        src_idx,
                        dst_idx,
                        pat.properties,
                        self.db.buffer_manager.clone(),
                        self.undo_buffer.clone(),
                        planned_child,
                        self.tx_id,
                    ),
                ))
            }
            LogicalOperator::Delete(child, vars, detach) => {
                let planned_child = self.plan(*child)?;
                let table_name = &vars[0].1;
                let storage = self.db.storage_manager.read();
                let table = storage.get_table(table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table '{table_name}' not found for DELETE"))
                })?.clone();
                Ok(Box::new(
                    crate::processor::operators::dml::PhysicalDelete::new(
                        planned_child,
                        table,
                        self.db.buffer_manager.clone(),
                        self.undo_buffer.clone(),
                        self.tx_id,
                        detach,
                    ),
                ))
            }
            LogicalOperator::Set(child, assignments) => {
                let planned_child = self.plan(*child)?;
                let table_name = &assignments[0].table_name;
                let storage = self.db.storage_manager.read();
                let table = storage.get_table(table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table '{table_name}' not found for SET"))
                })?.clone();
                Ok(Box::new(
                    crate::processor::operators::dml::PhysicalSet::new(
                        planned_child,
                        assignments,
                        table,
                        self.db.buffer_manager.clone(),
                        self.undo_buffer.clone(),
                        self.tx_id,
                    ),
                ))
            }
            LogicalOperator::CreateConstraint {
                name,
                table_name,
                property,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_constraint(
                    name,
                    table_name,
                    property,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::RecursiveJoin {
                child,
                rel_table: rel_table_name,
                src_var: _,
                dst_node_table,
                bounds,
                mask_id: _,
                ..
            } => {
                let planned_child = self.plan(*child)?;
                let storage = self.db.storage_manager.read();
                let rel_table = storage.get_table(&rel_table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Rel table {rel_table_name} not found"))
                })?.clone();
                let dst_table = storage.get_table(&dst_node_table).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table {dst_node_table} not found"))
                })?.clone();
                drop(storage);

                let src_idx = 0;
                let min_d = bounds.and_then(|b| b.0).unwrap_or(1);
                let max_d = bounds.and_then(|b| b.1).unwrap_or(u32::MAX);

                Ok(Box::new(
                    crate::processor::operators::recursive_join::PhysicalRecursiveJoin::new(
                        planned_child,
                        rel_table,
                        dst_table,
                        self.db.buffer_manager.clone(),
                        0,
                        src_idx,
                        (min_d, max_d),
                        self.read_ts,
                    ),
                ))
            }
            LogicalOperator::DropConstraint(name) => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_drop_constraint(
                    name,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::CreateIndex {
                name,
                table_name,
                property,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_index(
                    name,
                    table_name,
                    property,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::DropIndex(name) => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_drop_index(
                    name,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::CreateVectorIndex {
                table_name,
                field: _,
                index_type: _,
                metric,
                dimension,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_vector_index(
                    table_name,
                    metric,
                    dimension,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::CreateFtsIndex {
                table_name,
                fields: _,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_fts_index(
                    table_name,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::AlterTable { name, operation } => {
                match operation {
                    crate::parser::ast::AlterOperation::AddColumn { name: col_name, data_type } => {
                        let data_type = crate::parser::ast::data_type_to_logical(&data_type);
                        Ok(Box::new(
                            crate::processor::operators::ddl::PhysicalDDL::new_alter_add_column(
                                name,
                                col_name,
                                data_type,
                                self.db.clone(),
                                self.undo_buffer.clone(),
                            ),
                        ))
                    }
                    crate::parser::ast::AlterOperation::DropColumn { name: col_name } => {
                        Ok(Box::new(
                            crate::processor::operators::ddl::PhysicalDDL::new_alter_drop_column(
                                name,
                                col_name,
                                self.db.clone(),
                                self.undo_buffer.clone(),
                            ),
                        ))
                    }
                    crate::parser::ast::AlterOperation::RenameTable { new_name } => {
                        Ok(Box::new(
                            crate::processor::operators::ddl::PhysicalDDL::new_alter_rename_table(
                                name,
                                new_name,
                                self.db.clone(),
                                self.undo_buffer.clone(),
                            ),
                        ))
                    }
                    crate::parser::ast::AlterOperation::RenameColumn { old_name, new_name } => {
                        Ok(Box::new(
                            crate::processor::operators::ddl::PhysicalDDL::new_alter_rename_column(
                                name,
                                old_name,
                                new_name,
                                self.db.clone(),
                                self.undo_buffer.clone(),
                            ),
                        ))
                    }
                }
            }
            LogicalOperator::CreateTableNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_node(
                    name,
                    columns,
                    primary_key,
                    if_not_exists,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::CreateTableRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            } => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_create_rel(
                    name,
                    from_table,
                    to_table,
                    columns,
                    if_not_exists,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::DropTable(name, if_exists) => Ok(Box::new(
                crate::processor::operators::ddl::PhysicalDDL::new_drop(
                    name,
                    if_exists,
                    self.db.clone(),
                    self.undo_buffer.clone(),
                ),
            )),
            LogicalOperator::Union(left, right, is_all) => {
                let l = self.plan(*left)?;
                let r = self.plan(*right)?;
                Ok(Box::new(
                    crate::processor::operators::union::PhysicalUnion::new(l, r, is_all),
                ))
            }
            LogicalOperator::SingleRow => Ok(Box::new(
                crate::processor::operators::scan::PhysicalSingleRow::new(),
            )),
            LogicalOperator::Call(call) => Ok(Box::new(
                crate::processor::operators::call::PhysicalCall::new(call),
            )),
            LogicalOperator::Transaction(action) => Ok(Box::new(
                crate::processor::operators::transaction::PhysicalTransaction::new(action),
            )),
            LogicalOperator::Profile(child) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::profile::PhysicalProfile::new(planned_child),
                ))
            }
            LogicalOperator::Explain(child) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::profile::PhysicalProfile::new(planned_child)
                        .with_explain_analyze(),
                ))
            }
            LogicalOperator::Checkpoint => Ok(Box::new(
                crate::processor::operators::checkpoint::PhysicalCheckpoint::new(self.db.clone()),
            )),
            LogicalOperator::Vacuum => {
                let db = self.db.clone();
                Ok(Box::new(
                    crate::processor::operators::checkpoint::PhysicalVacuum::new(db),
                ))
            }
            LogicalOperator::AllShortestPaths {
                child,
                rel_table_name,
                src_var_name,
                dst_var_name,
                path_var_name,
                max_depth,
            } => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::gds::all_shortest_paths::PhysicalASP::new(
                        planned_child,
                        rel_table_name,
                        src_var_name,
                        dst_var_name,
                        path_var_name,
                        max_depth,
                    ),
                ))
            }
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => {
                let planned_child = self.plan(*child)?;
                let storage = self.db.storage_manager.read();
                let table = storage.get_table(&pattern.table_name).ok_or_else(|| {
                    crate::LightningError::Internal(format!("Table '{}' not found for MERGE", pattern.table_name))
                })?.clone();
                let num_rows = {
                    let cat = self.db.catalog.read();
                    cat.get_node_table(&pattern.table_name).ok_or_else(|| {
                        crate::LightningError::Internal(format!("Table '{}' not found in catalog for MERGE", pattern.table_name))
                    })?.num_rows
                };
                let effective_num_rows = num_rows;
                let table_name = pattern.table_name.clone();
                Ok(Box::new(
                    crate::processor::operators::dml::PhysicalMerge::new(
                        table_name,
                        table,
                        pattern,
                        on_create_assignments,
                        on_match_assignments,
                        self.db.buffer_manager.clone(),
                        self.undo_buffer.clone(),
                        Some(planned_child),
                        self.tx_id,
                        self.read_ts,
                        effective_num_rows,
                    ),
                ))
            }
            _ => Err(LightningError::Internal(format!(
                "Operator not implemented in PhysicalPlanner: {op:?}"
            ))),
        }
    }

    fn get_table_num_columns(&self, table_name: &str) -> usize {
        let cat = self.db.catalog.read();
        if let Some(node_table) = cat.get_node_table(table_name) {
            // add_node_table already prepends _id into the properties list,
            // so properties.len() includes _id + user columns — matching the
            // storage table's column count exactly.
            node_table.properties.len()
        } else if let Some(rel_table) = cat.get_rel_table(table_name) {
            // Rel tables have _src and _dst prepended by add_rel_table into
            // the properties list, so no adjustment needed.
            rel_table.properties.len()
        } else {
            2
        }
    }

    fn compute_variable_positions(
        &self,
        op: &LogicalOperator,
    ) -> Result<std::collections::HashMap<String, usize>> {
        let mut positions = std::collections::HashMap::new();
        self.collect_variable_positions(op, 0, &mut positions)?;
        Ok(positions)
    }

    fn collect_variable_positions(
        &self,
        op: &LogicalOperator,
        start_col: usize,
        positions: &mut std::collections::HashMap<String, usize>,
    ) -> Result<usize> {
        match op {
            LogicalOperator::Scan(table_name, var, ..) => {
                let num_cols = self.get_table_num_columns(table_name);
                positions.insert(var.clone(), start_col);
                Ok(num_cols)
            }
            LogicalOperator::Filter(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Join(left, right, ..) => {
                let left_cols = self.collect_variable_positions(left, start_col, positions)?;
                self.collect_variable_positions(right, start_col + left_cols, positions)
            }
            LogicalOperator::RecursiveJoin {
                child,
                dst_node_table,
                dst_var,
                ..
            } => {
                let child_cols = self.collect_variable_positions(child, start_col, positions)?;
                let dst_cols = self.get_table_num_columns(dst_node_table);
                positions.insert(dst_var.clone(), start_col + child_cols);
                Ok(child_cols + dst_cols)
            }
            LogicalOperator::Projection(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Aggregate { child, .. } => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Limit(child, ..)
            | LogicalOperator::TopK(child, ..)
            | LogicalOperator::Sort(child, ..)
            | LogicalOperator::Skip(child, ..)
            | LogicalOperator::Flatten(child)
            | LogicalOperator::Distinct(child, ..)
            | LogicalOperator::Accumulate(child)
            | LogicalOperator::Profile(child)
            | LogicalOperator::Explain(child) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::CreateNode(child_opt, ..) => {
                if let Some(child) = child_opt {
                    self.collect_variable_positions(child, start_col, positions)
                } else {
                    Ok(0)
                }
            }
            LogicalOperator::Set(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Delete(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::SemiMasker(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Unwind(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Subquery(child) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::UnwindDedup(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::OptionalMatch(child, inner) => {
                let child_cols = self.collect_variable_positions(child, start_col, positions)?;
                self.collect_variable_positions(inner, start_col + child_cols, positions)
            }
            LogicalOperator::With(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Intersect {
                probe_child,
                build_children,
                ..
            } => {
                let mut col_offset = start_col;
                col_offset +=
                    self.collect_variable_positions(probe_child, col_offset, positions)?;
                for build_child in build_children {
                    col_offset +=
                        self.collect_variable_positions(build_child, col_offset, positions)?;
                }
                Ok(col_offset - start_col)
            }
            LogicalOperator::AllShortestPaths { src_var_name, dst_var_name, path_var_name, .. } => {
                positions.insert(src_var_name.clone(), start_col);
                positions.insert(dst_var_name.clone(), start_col + 1);
                positions.insert(path_var_name.clone(), start_col + 2);
                Ok(3)
            }
            LogicalOperator::Merge { child, .. } => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::Union(left, right, _) => {
                let left_cols = self.collect_variable_positions(left, start_col, positions)?;
                self.collect_variable_positions(right, start_col + left_cols, positions)
            }
            LogicalOperator::SemiJoin(child, ..) => {
                self.collect_variable_positions(child, start_col, positions)
            }
            LogicalOperator::IndexScan(table_name, _var, _pk_name, _pk_val, projected_idxs) => {
                if let Some(idxs) = projected_idxs {
                    Ok(idxs.len())
                } else {
                    let cat = self.db.catalog.read();
                    if let Some(t) = cat.get_node_table(table_name) {
                        // IndexScan includes the internal _id column in its physical
                        // output (same as Scan), so the column count must match.
                        Ok(t.properties.len() + 1)
                    } else if let Some(t) = cat.get_rel_table(table_name) {
                        Ok(t.properties.len())
                    } else {
                        Ok(0)
                    }
                }
            }
            LogicalOperator::CreateRel(child_opt, _) => {
                if let Some(child) = child_opt {
                    self.collect_variable_positions(child, start_col, positions)
                } else {
                    Ok(0)
                }
            }
            LogicalOperator::Call(_) => Ok(0),
            LogicalOperator::CountRelTable { .. }
            | LogicalOperator::CreateSequence { .. }
            | LogicalOperator::CreateMacro { .. }
            | LogicalOperator::CreateConstraint { .. }
             | LogicalOperator::DropConstraint(..)
              | LogicalOperator::CreateIndex { .. }
              | LogicalOperator::CreateVectorIndex { .. }
              | LogicalOperator::CreateFtsIndex { .. }
              | LogicalOperator::DropIndex(..)
             | LogicalOperator::CreateTableNode { .. }
            | LogicalOperator::CreateTableRel { .. }
            | LogicalOperator::DropTable(..)
            | LogicalOperator::AlterTable { .. }
            | LogicalOperator::CopyFrom { .. }
            | LogicalOperator::CopyTo { .. }
            | LogicalOperator::Transaction(_)
            | LogicalOperator::Checkpoint
            | LogicalOperator::Vacuum
            | LogicalOperator::SingleRow => Ok(0),
        }
    }

    fn compute_subtree_num_cols(&self, op: &LogicalOperator) -> usize {
        match op {
            LogicalOperator::Scan(table_name, ..) => self.get_table_num_columns(table_name),
            LogicalOperator::IndexScan(table_name, ..) => {
                let cat = self.db.catalog.read();
                if let Some(t) = cat.get_node_table(table_name) {
                    t.properties.len() + 1
                } else if let Some(t) = cat.get_rel_table(table_name) {
                    t.properties.len()
                } else {
                    0
                }
            }
            LogicalOperator::Join(left, right, ..) => {
                self.compute_subtree_num_cols(left) + self.compute_subtree_num_cols(right)
            }
            LogicalOperator::Filter(child, ..)
            | LogicalOperator::Projection(child, ..)
            | LogicalOperator::Sort(child, ..)
            | LogicalOperator::Limit(child, ..)
            | LogicalOperator::TopK(child, ..)
            | LogicalOperator::Aggregate { child, .. }
            | LogicalOperator::SemiMasker(child, ..)
            | LogicalOperator::Flatten(child)
            | LogicalOperator::Accumulate(child)
            | LogicalOperator::Profile(child)
            | LogicalOperator::Explain(child) => self.compute_subtree_num_cols(child),
            LogicalOperator::RecursiveJoin { child, dst_node_table, .. } => {
                self.compute_subtree_num_cols(child) + self.get_table_num_columns(dst_node_table)
            }
            LogicalOperator::AllShortestPaths { .. } => 3,
            LogicalOperator::Union(left, ..) => self.compute_subtree_num_cols(left),
            LogicalOperator::Intersect { probe_child, .. } => self.compute_subtree_num_cols(probe_child),
            _ => 0,
        }
    }

    /// Recursively collect equality comparisons from a (possibly compound) expression.
    fn collect_eq_comparisons<'a>(
        &self,
        expr: &'a BoundExpression,
        out: &mut Vec<(&'a BoundExpression, &'a BoundExpression)>,
    ) {
        match expr {
            BoundExpression::Comparison(left, ComparisonOperator::Equal, right) => {
                out.push((left.as_ref(), right.as_ref()));
            }
            BoundExpression::Logical(left, crate::parser::ast::LogicalOperator::And, right) => {
                self.collect_eq_comparisons(left, out);
                self.collect_eq_comparisons(right, out);
            }
            _ => {}
        }
    }

    /// Resolve join key indices handling potential left/right swapping from join reordering.
    /// After JoinReordering swaps subtrees based on cardinality, the join condition's
    /// expression sides may not correspond to the actual left/right children. This method
    /// detects which variable each expression references and maps to the correct side.
    fn resolve_join_key_pair(
        &self,
        expr_a: &BoundExpression,
        expr_b: &BoundExpression,
        left_positions: &std::collections::HashMap<String, usize>,
        right_positions: &std::collections::HashMap<String, usize>,
    ) -> Result<(usize, usize)> {
        let a_in_left = self.expr_belongs_to_side(expr_a, left_positions);
        let b_in_left = self.expr_belongs_to_side(expr_b, left_positions);
        let a_in_right = self.expr_belongs_to_side(expr_a, right_positions);
        let b_in_right = self.expr_belongs_to_side(expr_b, right_positions);

        let (left_key_expr, right_key_expr) = if a_in_left && b_in_right {
            (expr_a, expr_b)
        } else if b_in_left && a_in_right {
            (expr_b, expr_a)
        } else {
            if a_in_left {
                (expr_a, expr_b)
            } else {
                (expr_b, expr_a)
            }
        };

        let left_key = self.resolve_key_index(left_key_expr, left_positions, "left")?;
        let right_key = self.resolve_key_index(right_key_expr, right_positions, "right")?;
        Ok((left_key, right_key))
    }

    fn expr_belongs_to_side(
        &self,
        expr: &BoundExpression,
        positions: &std::collections::HashMap<String, usize>,
    ) -> bool {
        match expr {
            BoundExpression::PropertyLookup(var, _, _) => positions.contains_key(var),
            BoundExpression::Comparison(left, _, right)
            | BoundExpression::Arithmetic(left, _, right)
            | BoundExpression::Logical(left, _, right) => {
                self.expr_belongs_to_side(left, positions)
                    || self.expr_belongs_to_side(right, positions)
            }
            BoundExpression::Not(inner) => self.expr_belongs_to_side(inner, positions),
            BoundExpression::Function(_, args, _) => {
                args.iter().any(|a| self.expr_belongs_to_side(a, positions))
            }
            _ => false,
        }
    }

    /// Resolve a PropertyLookup expression to its column index, checking
    /// both left and right variable positions.
    ///
    /// The join condition from the logical planner uses table-relative
    /// indices (0 for _id/_from, 1 for _to, etc.).  We add the physical
    /// base position of the variable to get the absolute column index.
    fn resolve_key_index(
        &self,
        expr: &BoundExpression,
        positions: &std::collections::HashMap<String, usize>,
        side: &str,
    ) -> Result<usize> {
        match expr {
            BoundExpression::PropertyLookup(var, idx, _) => {
                if let Some(base) = positions.get(var) {
                    return Ok(base + idx);
                }
                Err(LightningError::Internal(format!(
                    "{} join key variable '{}' not found in either subtree",
                    side, var,
                )))
            }
            _ => Err(LightningError::Internal(format!(
                "{} join key must be a PropertyLookup",
                side,
            ))),
        }
    }

    /// Recursively walk a BoundExpression tree and remap all PropertyLookup
    /// index values from binder-relative offsets to physical-plan-relative offsets.
    ///
    /// The binder assigns each MATCH variable a sequential starting column (e.g.
    /// a=0, r=11, b=16). PropertyLookup indices embed these offsets:
    ///   `idx = binder_column_offsets[var] + property_index_in_table`
    ///
    /// After optimizer transforms (join reordering, etc.), the physical column
    /// layout may place variables at different positions. This function reads
    /// the CHILD operator's variable positions and corrects each lookup:
    ///   `new_idx = child_phys_positions[var] + property_index_in_table`
    ///            = child_phys_positions[var] + (idx - binder_column_offsets[var])
    fn remap_property_lookup(
        expr: &mut BoundExpression,
        child_phys_positions: &std::collections::HashMap<String, usize>,
        binder_column_offsets: &std::collections::HashMap<String, usize>,
    ) {
        match expr {
            BoundExpression::PropertyLookup(var, idx, _) => {
                let binder_base = binder_column_offsets.get(var).copied().unwrap_or(0);
                let phys_base = child_phys_positions.get(var).copied().unwrap_or(binder_base);
                let prop_index = idx.saturating_sub(binder_base);
                *idx = phys_base + prop_index;
            }
            BoundExpression::Variable(..)
            | BoundExpression::Literal(_)
            | BoundExpression::Parameter(_)
            | BoundExpression::NextVal(_) => {}
            BoundExpression::Not(inner) => {
                Self::remap_property_lookup(inner, child_phys_positions, binder_column_offsets);
            }
            BoundExpression::Arithmetic(left, _, right)
            | BoundExpression::Comparison(left, _, right)
            | BoundExpression::Logical(left, _, right) => {
                Self::remap_property_lookup(left, child_phys_positions, binder_column_offsets);
                Self::remap_property_lookup(right, child_phys_positions, binder_column_offsets);
            }
            BoundExpression::Function(_, args, _) | BoundExpression::List(args, _) => {
                for arg in args {
                    Self::remap_property_lookup(arg, child_phys_positions, binder_column_offsets);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    Self::remap_property_lookup(e, child_phys_positions, binder_column_offsets);
                }
                for (w, t) in when_then {
                    Self::remap_property_lookup(w, child_phys_positions, binder_column_offsets);
                    Self::remap_property_lookup(t, child_phys_positions, binder_column_offsets);
                }
                if let Some(e) = else_expression {
                    Self::remap_property_lookup(e, child_phys_positions, binder_column_offsets);
                }
            }
            BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    Self::remap_property_lookup(arg, child_phys_positions, binder_column_offsets);
                }
            }
            BoundExpression::Lambda(_, body) => {
                Self::remap_property_lookup(body, child_phys_positions, binder_column_offsets);
            }
            BoundExpression::Exists(_) | BoundExpression::CountSubquery(_) => {
                // These contain match clauses, not expressions — nothing to remap
            }
            BoundExpression::Map(items, _) => {
                for (_, val) in items {
                    Self::remap_property_lookup(val, child_phys_positions, binder_column_offsets);
                }
            }
        }
    }

    pub fn plan_expression(
        &self,
        op: &LogicalOperator,
        expr: &BoundExpression,
    ) -> Result<BoundExpression> {
        // 1. Recursively plan child expressions (bottom-up)
        let expr = self.plan_expression_children(op, expr)?;
        // 2. Apply simplification / constant folding
        Ok(self.simplify_expression(expr))
    }

    /// Recursively walk the expression tree and plan child sub-expressions first.
    fn plan_expression_children(
        &self,
        op: &LogicalOperator,
        expr: &BoundExpression,
    ) -> Result<BoundExpression> {
        match expr {
            BoundExpression::Arithmetic(left, arith_op, right) => {
                let l = self.plan_expression(op, left)?;
                let r = self.plan_expression(op, right)?;
                if let (BoundExpression::Literal(Literal::Number(a)),
                        BoundExpression::Literal(Literal::Number(b))) = (&l, &r)
                {
                    let result = match arith_op {
                        ArithmeticOperator::Add => a + b,
                        ArithmeticOperator::Subtract => a - b,
                        ArithmeticOperator::Multiply => a * b,
                        ArithmeticOperator::Divide => {
                            if *b == 0.0 {
                                return Err(LightningError::Internal(
                                    "Division by zero in constant expression".into(),
                                ));
                            }
                            a / b
                        }
                        ArithmeticOperator::Modulo => a % b,
                    };
                    return Ok(BoundExpression::Literal(Literal::Number(result)));
                }
                Ok(BoundExpression::Arithmetic(Box::new(l), *arith_op, Box::new(r)))
            }
            BoundExpression::Comparison(left, cmp_op, right) => {
                let l = self.plan_expression(op, left)?;
                let r = self.plan_expression(op, right)?;
                if let (BoundExpression::Literal(Literal::Number(a)),
                        BoundExpression::Literal(Literal::Number(b))) = (&l, &r)
                {
                    let result = match cmp_op {
                        ComparisonOperator::Equal => a == b,
                        ComparisonOperator::NotEqual => a != b,
                        ComparisonOperator::LessThan => a < b,
                        ComparisonOperator::LessThanOrEqual => a <= b,
                        ComparisonOperator::GreaterThan => a > b,
                        ComparisonOperator::GreaterThanOrEqual => a >= b,
                    };
                    return Ok(BoundExpression::Literal(Literal::Boolean(result)));
                }
                if let (BoundExpression::Literal(Literal::String(a)),
                        BoundExpression::Literal(Literal::String(b))) = (&l, &r)
                {
                    let result = match cmp_op {
                        ComparisonOperator::Equal => a == b,
                        ComparisonOperator::NotEqual => a != b,
                        _ => false,
                    };
                    return Ok(BoundExpression::Literal(Literal::Boolean(result)));
                }
                Ok(BoundExpression::Comparison(Box::new(l), *cmp_op, Box::new(r)))
            }
            BoundExpression::Logical(left, log_op, right) => {
                let l = self.plan_expression(op, left)?;
                let r = self.plan_expression(op, right)?;
                if let (BoundExpression::Literal(Literal::Boolean(a)),
                        BoundExpression::Literal(Literal::Boolean(b))) = (&l, &r)
                {
                    let result = match log_op {
                        BoolOperator::And => *a && *b,
                        BoolOperator::Or => *a || *b,
                        BoolOperator::Xor => *a ^ *b,
                        _ => false,
                    };
                    return Ok(BoundExpression::Literal(Literal::Boolean(result)));
                }
                Ok(BoundExpression::Logical(Box::new(l), *log_op, Box::new(r)))
            }
            BoundExpression::Not(inner) => {
                let inner = self.plan_expression(op, inner)?;
                if let BoundExpression::Not(inner_inner) = &inner {
                    return Ok(*inner_inner.clone());
                }
                if let BoundExpression::Literal(Literal::Boolean(b)) = &inner {
                    return Ok(BoundExpression::Literal(Literal::Boolean(!b)));
                }
                Ok(BoundExpression::Not(Box::new(inner)))
            }
            BoundExpression::Function(name, args, return_type) => {
                let mut planned_args = Vec::with_capacity(args.len());
                for arg in args {
                    planned_args.push(self.plan_expression(op, arg)?);
                }
                Ok(BoundExpression::Function(name.clone(), planned_args, return_type.clone()))
            }
            BoundExpression::List(items, t) => {
                let mut planned = Vec::with_capacity(items.len());
                for item in items {
                    planned.push(self.plan_expression(op, item)?);
                }
                Ok(BoundExpression::List(planned, t.clone()))
            }
            BoundExpression::Case { expression, when_then, else_expression, return_type } => {
                let expr = expression
                    .as_ref()
                    .map(|e| self.plan_expression(op, e))
                    .transpose()?;
                let mut planned_when_then = Vec::with_capacity(when_then.len());
                for (w, t) in when_then {
                    let w = self.plan_expression(op, w)?;
                    let t = self.plan_expression(op, t)?;
                    planned_when_then.push((w, t));
                }
                let else_expr = else_expression
                    .as_ref()
                    .map(|e| self.plan_expression(op, e))
                    .transpose()?;
                Ok(BoundExpression::Case {
                    expression: expr.map(Box::new),
                    when_then: planned_when_then,
                    else_expression: else_expr.map(Box::new),
                    return_type: return_type.clone(),
                })
            }
            BoundExpression::Map(items, t) => {
                let mut planned = Vec::with_capacity(items.len());
                for (k, v) in items {
                    planned.push((k.clone(), self.plan_expression(op, v)?));
                }
                Ok(BoundExpression::Map(planned, t.clone()))
            }
            BoundExpression::Aggregate(name, args, filter) => {
                let mut planned = Vec::with_capacity(args.len());
                for arg in args {
                    planned.push(self.plan_expression(op, arg)?);
                }
                Ok(BoundExpression::Aggregate(name.clone(), planned, filter.clone()))
            }
            BoundExpression::Lambda(var, body) => {
                let body = self.plan_expression(op, body)?;
                Ok(BoundExpression::Lambda(var.clone(), Box::new(body)))
            }
            BoundExpression::Literal(_)
            | BoundExpression::Variable(_, _)
            | BoundExpression::PropertyLookup(_, _, _)
            | BoundExpression::Parameter(_)
            | BoundExpression::NextVal(_)
            | BoundExpression::Exists(_)
            | BoundExpression::CountSubquery(_) => Ok(expr.clone()),
        }
    }

    /// Apply algebraic simplification rules (predicate simplification, etc.)
    fn simplify_expression(&self, expr: BoundExpression) -> BoundExpression {
        match expr {
            // NOT (a > b) → a <= b
            BoundExpression::Not(inner) => match *inner {
                BoundExpression::Comparison(a, ComparisonOperator::GreaterThan, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::LessThanOrEqual, b)
                }
                BoundExpression::Comparison(a, ComparisonOperator::GreaterThanOrEqual, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::LessThan, b)
                }
                BoundExpression::Comparison(a, ComparisonOperator::LessThan, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::GreaterThanOrEqual, b)
                }
                BoundExpression::Comparison(a, ComparisonOperator::LessThanOrEqual, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::GreaterThan, b)
                }
                BoundExpression::Comparison(a, ComparisonOperator::Equal, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::NotEqual, b)
                }
                BoundExpression::Comparison(a, ComparisonOperator::NotEqual, b) => {
                    BoundExpression::Comparison(a, ComparisonOperator::Equal, b)
                }
                BoundExpression::Not(inner_inner) => self.simplify_expression(*inner_inner),
                other => BoundExpression::Not(Box::new(self.simplify_expression(other))),
            },
            // Logical short-circuit: x AND true → x, x OR false → x, etc.
            BoundExpression::Logical(left, op, right) => {
                let l = self.simplify_expression(*left);
                let r = self.simplify_expression(*right);
                match (&l, op, &r) {
                    (BoundExpression::Literal(Literal::Boolean(true)), BoolOperator::And, _) => r,
                    (_, BoolOperator::And, BoundExpression::Literal(Literal::Boolean(true))) => l,
                    (BoundExpression::Literal(Literal::Boolean(false)), BoolOperator::And, _) => {
                        BoundExpression::Literal(Literal::Boolean(false))
                    }
                    (_, BoolOperator::And, BoundExpression::Literal(Literal::Boolean(false))) => {
                        BoundExpression::Literal(Literal::Boolean(false))
                    }
                    (BoundExpression::Literal(Literal::Boolean(false)), BoolOperator::Or, _) => r,
                    (_, BoolOperator::Or, BoundExpression::Literal(Literal::Boolean(false))) => l,
                    (BoundExpression::Literal(Literal::Boolean(true)), BoolOperator::Or, _) => {
                        BoundExpression::Literal(Literal::Boolean(true))
                    }
                    (_, BoolOperator::Or, BoundExpression::Literal(Literal::Boolean(true))) => {
                        BoundExpression::Literal(Literal::Boolean(true))
                    }
                    _ => BoundExpression::Logical(Box::new(l), op, Box::new(r)),
                }
            }
            BoundExpression::Arithmetic(l, op, r) => {
                let l = self.simplify_expression(*l);
                let r = self.simplify_expression(*r);
                BoundExpression::Arithmetic(Box::new(l), op, Box::new(r))
            }
            BoundExpression::Comparison(l, op, r) => {
                let l = self.simplify_expression(*l);
                let r = self.simplify_expression(*r);
                BoundExpression::Comparison(Box::new(l), op, Box::new(r))
            }
            BoundExpression::Function(name, args, return_type) => {
                let args: Vec<_> = args.into_iter().map(|a| self.simplify_expression(a)).collect();
                BoundExpression::Function(name, args, return_type)
            }
            other => other,
        }
    }

    fn extract_trigram_candidates(
        &self,
        expr: &BoundExpression,
        table: &crate::storage::storage_manager::Table,
    ) -> Option<Vec<u64>> {
        table.flush_trigram_workers();

        match expr {
            BoundExpression::Function(name, args, _)
                if name.to_uppercase() == "CONTAINS" && args.len() == 2 =>
            {
                let col_name = if let BoundExpression::PropertyLookup(_, prop_idx, _) = &args[0] {
                    if *prop_idx < table.columns.len() {
                        Some(&table.columns[*prop_idx].name)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let pattern =
                    if let BoundExpression::Literal(crate::parser::ast::Literal::String(s)) =
                        &args[1]
                    {
                        Some(s)
                    } else {
                        None
                    };

                if let (Some(c), Some(p)) = (col_name, pattern) {
                    let indexes = table.trigram_indexes.read();
                    if let Some(idx) = indexes.get(c) {
                        let result = idx.query_with_adaptive_threshold(p);
                        if let Some(r) = result {
                            if r.use_index {
                                return Some(r.candidates);
                            }
                        }
                    }
                }
                None
            }
            BoundExpression::Logical(left, op, right) => match op {
                crate::parser::ast::LogicalOperator::And => {
                    let l_cand = self.extract_trigram_candidates(left, table);
                    let r_cand = self.extract_trigram_candidates(right, table);
                    match (l_cand, r_cand) {
                        (Some(l), Some(r)) => {
                            let mut l_set: std::collections::HashSet<_> = l.into_iter().collect();
                            let mut res = Vec::new();
                            for id in r {
                                if l_set.remove(&id) {
                                    res.push(id);
                                }
                            }
                            res.sort_unstable();
                            Some(res)
                        }
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    }
                }
                crate::parser::ast::LogicalOperator::Or => {
                    let l_cand = self.extract_trigram_candidates(left, table);
                    let r_cand = self.extract_trigram_candidates(right, table);
                    if let (Some(l), Some(r)) = (l_cand, r_cand) {
                        let mut res = l;
                        res.extend(r);
                        res.sort_unstable();
                        res.dedup();
                        Some(res)
                    } else {
                        None
                    }
                }
                crate::parser::ast::LogicalOperator::Xor => {
                    let l_cand = self.extract_trigram_candidates(left, table);
                    let r_cand = self.extract_trigram_candidates(right, table);
                    if let (Some(l), Some(r)) = (l_cand, r_cand) {
                        // XOR = (A âˆª B) - (A âˆ© B): keep candidates that are in
                        // exactly one of the two sets. Previously this computed
                        // A âˆª B (same as OR), which was incorrect for XOR semantics.
                        use std::collections::HashSet;
                        let l_set: HashSet<u64> = l.into_iter().collect();
                        let r_set: HashSet<u64> = r.into_iter().collect();
                        let xor: Vec<u64> = l_set
                            .symmetric_difference(&r_set)
                            .copied()
                            .collect();
                        Some(xor)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }
}

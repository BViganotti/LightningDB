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
            LogicalOperator::Filter(child, expr) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::filter::PhysicalFilter::new(planned_child, expr),
                ))
            }
            LogicalOperator::Projection(child, items) => {
                let planned_child = self.plan(*child)?;
                Ok(Box::new(
                    crate::processor::operators::projection::PhysicalProjection::new(
                        planned_child,
                        items,
                    ),
                ))
            }
            LogicalOperator::Join(left, right, join_cond) => {
                let planned_left = self.plan(*left)?;
                let planned_right = self.plan(*right)?;
                let is_cross_join = matches!(
                    join_cond,
                    BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
                );
                if is_cross_join {
                    Ok(Box::new(
                        crate::processor::operators::hash_join::HashJoin::new_cross_join(
                            planned_left,
                            planned_right,
                        ),
                    ))
                } else {
                    Ok(Box::new(
                        crate::processor::operators::hash_join::HashJoin::new(
                            planned_left,
                            planned_right,
                            0,
                            0,
                        ),
                    ))
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
                let _planned_child = self.plan(*child)?;
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
            node_table.properties.len()
        } else if let Some(rel_table) = cat.get_rel_table(table_name) {
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
                src_var,
                dst_var,
                ..
            } => {
                let child_cols = self.collect_variable_positions(child, start_col, positions)?;
                positions.insert(src_var.clone(), start_col);
                positions.insert(dst_var.clone(), start_col + 1);
                Ok(child_cols + 2)
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
            LogicalOperator::AllShortestPaths { child, .. } => {
                self.collect_variable_positions(child, start_col, positions)
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
                        Ok(t.properties.len())
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

    pub fn plan_expression(
        &self,
        _op: &LogicalOperator,
        _expr: &BoundExpression,
    ) -> Result<BoundExpression> {
        Ok(_expr.clone())
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
                        let mut res = l;
                        res.extend(r);
                        res.sort_unstable();
                        res.dedup();
                        Some(res)
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

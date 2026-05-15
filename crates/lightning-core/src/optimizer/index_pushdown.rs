use crate::catalog::Catalog;
use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct IndexPushDown {
    catalog: Arc<RwLock<Catalog>>,
}

impl IndexPushDown {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    fn apply_recursive(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Filter(child, cond) => {
                let pushed_child = self.apply_recursive(*child)?;

                // Look for Filter(Scan(T, var), pk_col = literal)
                if let LogicalOperator::Scan(table_name, var, _, proj, _) = &pushed_child {
                    let cat = self.catalog.read();
                    if let Some(table_entry) = cat.get_node_table(table_name) {
                        if let Some(pk_name) = &table_entry.primary_key {
                            // Check if condition is pk_col = literal
                            if let BoundExpression::Comparison(
                                left,
                                crate::parser::ast::ComparisonOperator::Equal,
                                right,
                            ) = &cond
                            {
                                match (&**left, &**right) {
                                    (
                                        BoundExpression::PropertyLookup(v, _, _),
                                        BoundExpression::Literal(_),
                                    ) if v == var => {
                                        // Check if this property lookup is for the PK column
                                        // Actually property lookup uses index. We need to check if that index matches PK index.
                                        // For simplicity, let's assume if it matches the name.
                                        // Wait, PropertyLookup only has index. We need to check the catalog.
                                        if let Some(pk_idx) = table_entry
                                            .properties
                                            .iter()
                                            .position(|p| p.name == *pk_name)
                                        {
                                            if let BoundExpression::PropertyLookup(
                                                _,
                                                lookup_idx,
                                                _,
                                            ) = &**left
                                            {
                                                if *lookup_idx == pk_idx {
                                                    return Ok(LogicalOperator::IndexScan(
                                                        table_name.clone(),
                                                        var.clone(),
                                                        pk_name.clone(),
                                                        *right.clone(),
                                                        proj.clone(),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                    (
                                        BoundExpression::Literal(_),
                                        BoundExpression::PropertyLookup(v, _, _),
                                    ) if v == var => {
                                        if let Some(pk_idx) = table_entry
                                            .properties
                                            .iter()
                                            .position(|p| p.name == *pk_name)
                                        {
                                            if let BoundExpression::PropertyLookup(
                                                _,
                                                lookup_idx,
                                                _,
                                            ) = &**right
                                            {
                                                if *lookup_idx == pk_idx {
                                                    return Ok(LogicalOperator::IndexScan(
                                                        table_name.clone(),
                                                        var.clone(),
                                                        pk_name.clone(),
                                                        *left.clone(),
                                                        proj.clone(),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Ok(LogicalOperator::Filter(Box::new(pushed_child), cond))
            }
            LogicalOperator::Projection(child, items) => Ok(LogicalOperator::Projection(
                Box::new(self.apply_recursive(*child)?),
                items,
            )),
            LogicalOperator::Join(left, right, cond) => Ok(LogicalOperator::Join(
                Box::new(self.apply_recursive(*left)?),
                Box::new(self.apply_recursive(*right)?),
                cond,
            )),
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            } => Ok(LogicalOperator::Aggregate {
                child: Box::new(self.apply_recursive(*child)?),
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            }),
            LogicalOperator::Sort(child, items) => Ok(LogicalOperator::Sort(
                Box::new(self.apply_recursive(*child)?),
                items,
            )),
            LogicalOperator::Limit(child, l) => Ok(LogicalOperator::Limit(
                Box::new(self.apply_recursive(*child)?),
                l,
            )),
            LogicalOperator::Skip(child, s) => Ok(LogicalOperator::Skip(
                Box::new(self.apply_recursive(*child)?),
                s,
            )),
            LogicalOperator::CreateNode(child, p) => {
                let new_child = if let Some(c) = child {
                    Some(Box::new(self.apply_recursive(*c)?))
                } else {
                    None
                };
                Ok(LogicalOperator::CreateNode(new_child, p))
            }
            LogicalOperator::CreateRel(child, p) => {
                let new_child = if let Some(c) = child {
                    Some(Box::new(self.apply_recursive(*c)?))
                } else {
                    None
                };
                Ok(LogicalOperator::CreateRel(new_child, p))
            }
            LogicalOperator::Delete(child, v, detach) => Ok(LogicalOperator::Delete(
                Box::new(self.apply_recursive(*child)?),
                v,
                detach,
            )),
            LogicalOperator::Set(child, a) => Ok(LogicalOperator::Set(
                Box::new(self.apply_recursive(*child)?),
                a,
            )),
            LogicalOperator::SemiMasker(child, v, m) => Ok(LogicalOperator::SemiMasker(
                Box::new(self.apply_recursive(*child)?),
                v,
                m,
            )),
            LogicalOperator::RecursiveJoin {
                child,
                rel_table,
                rel_var,
                src_var,
                dst_node_table,
                dst_var,
                bounds,
                ..
            } => {
                let pushed_child = self.apply_recursive(*child)?;
                Ok(LogicalOperator::RecursiveJoin {
                    child: Box::new(pushed_child),
                    rel_table,
                    rel_var,
                    src_var,
                    dst_node_table,
                    dst_var,
                    bounds,
                    mask_id: None,
                })
            }
            LogicalOperator::Unwind(child, expr, alias) => Ok(LogicalOperator::Unwind(
                Box::new(self.apply_recursive(*child)?),
                expr,
                alias,
            )),
            LogicalOperator::Intersect {
                probe_child,
                build_children,
                key_vars,
                intersect_var,
            } => {
                let pushed_probe = self.apply_recursive(*probe_child)?;
                let mut pushed_builds = Vec::new();
                for build in build_children {
                    pushed_builds.push(self.apply_recursive(build)?);
                }
                Ok(LogicalOperator::Intersect {
                    probe_child: Box::new(pushed_probe),
                    build_children: pushed_builds,
                    key_vars,
                    intersect_var,
                })
            }
            LogicalOperator::Flatten(child) => Ok(LogicalOperator::Flatten(Box::new(
                self.apply_recursive(*child)?,
            ))),
            LogicalOperator::UnwindDedup(child, expr) => Ok(LogicalOperator::UnwindDedup(
                Box::new(self.apply_recursive(*child)?),
                expr,
            )),
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => Ok(LogicalOperator::Merge {
                child: Box::new(self.apply_recursive(*child)?),
                pattern: pattern.clone(),
                on_create_assignments: on_create_assignments.clone(),
                on_match_assignments: on_match_assignments.clone(),
            }),
            LogicalOperator::Union(left, right, is_all) => Ok(LogicalOperator::Union(
                Box::new(self.apply_recursive(*left)?),
                Box::new(self.apply_recursive(*right)?),
                is_all,
            )),
            LogicalOperator::SingleRow => Ok(plan),
            LogicalOperator::Scan(..) | LogicalOperator::IndexScan(..) => Ok(plan),
            _ => Ok(plan), // Handle unexpected or unimplemented operators by returning as is
        }
    }
}

impl Rule for IndexPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.apply_recursive(plan)
    }
}

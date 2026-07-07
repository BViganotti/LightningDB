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

                // Look for Filter(Scan(T, var), col = literal) — PK or secondary index
                if let LogicalOperator::Scan(table_name, var, _, proj, _) = &pushed_child {
                    let cat = self.catalog.read();
                    if let Some(table_entry) = cat.get_node_table(table_name) {
                        let pk_name_opt = table_entry.primary_key.clone();
                        let pk_idx_opt = pk_name_opt
                            .as_ref()
                            .and_then(|pk| table_entry.properties.iter().position(|p| p.name == *pk));

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
                                    if let BoundExpression::PropertyLookup(_, lookup_idx, _) =
                                        &**left
                                    {
                                        let is_pk = pk_idx_opt == Some(*lookup_idx);
                                        let prop_name = table_entry.properties.get(*lookup_idx).map(|p| &p.name);
                                        if is_pk {
                                            if !expr_has_outer_variables(right) {
                                                return Ok(LogicalOperator::IndexScan(
                                                    table_name.clone(),
                                                    var.clone(),
                                                    table_name.clone(),
                                                    *right.clone(),
                                                    proj.clone(),
                                                ));
                                            }
                                        } else if let Some(prop) = prop_name {
                                            if let Some(secondary_name) = table_entry.secondary_indexes.get(prop) {
                                                if !expr_has_outer_variables(right) {
                                                    return Ok(LogicalOperator::IndexScan(
                                                        table_name.clone(),
                                                        var.clone(),
                                                        secondary_name.clone(),
                                                        *right.clone(),
                                                        proj.clone(),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                                (
                                    BoundExpression::Literal(_),
                                    BoundExpression::PropertyLookup(v, _, _),
                                ) if v == var => {
                                    if let BoundExpression::PropertyLookup(_, lookup_idx, _) =
                                        &**right
                                    {
                                        let is_pk = pk_idx_opt == Some(*lookup_idx);
                                        let prop_name = table_entry.properties.get(*lookup_idx).map(|p| &p.name);
                                        if is_pk {
                                            if !expr_has_outer_variables(left) {
                                                return Ok(LogicalOperator::IndexScan(
                                                    table_name.clone(),
                                                    var.clone(),
                                                    table_name.clone(),
                                                    *left.clone(),
                                                    proj.clone(),
                                                ));
                                            }
                                        } else if let Some(prop) = prop_name {
                                            if let Some(secondary_name) = table_entry.secondary_indexes.get(prop) {
                                                if !expr_has_outer_variables(left) {
                                                    return Ok(LogicalOperator::IndexScan(
                                                        table_name.clone(),
                                                        var.clone(),
                                                        secondary_name.clone(),
                                                        *left.clone(),
                                                        proj.clone(),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {}
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
                mask_id,
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
                    mask_id,
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

/// Check if a BoundExpression references outer-scope variables (not simple literals).
/// IndexScan pk_value expressions must be pure literals; correlated subquery references
/// produce incorrect results when constant-folded by the physical planner.
fn expr_has_outer_variables(expr: &BoundExpression) -> bool {
    match expr {
        BoundExpression::Literal(_) => false,
        BoundExpression::Variable(_, _) => true,
        BoundExpression::PropertyLookup(_, _, _) | BoundExpression::UnwindProperty(..) => true,
        BoundExpression::Function(_, args, _) => args.iter().any(expr_has_outer_variables),
        BoundExpression::Aggregate(_, args, _) => args.iter().any(expr_has_outer_variables),
        BoundExpression::Arithmetic(left, _, right) => {
            expr_has_outer_variables(left) || expr_has_outer_variables(right)
        }
        BoundExpression::Comparison(left, _, right) => {
            expr_has_outer_variables(left) || expr_has_outer_variables(right)
        }
        BoundExpression::Logical(left, _, right) => {
            expr_has_outer_variables(left) || expr_has_outer_variables(right)
        }
        BoundExpression::Not(inner) => expr_has_outer_variables(inner),
        BoundExpression::Exists(_) | BoundExpression::CountSubquery(_) => true,
        BoundExpression::List(items, _) => items.iter().any(expr_has_outer_variables),
        BoundExpression::Map(entries, _) => entries.iter().any(|(_, v)| expr_has_outer_variables(v)),
        BoundExpression::Lambda(_, body) => expr_has_outer_variables(body),
        BoundExpression::Case { expression, when_then, else_expression, .. } => {
            expression.as_ref().is_some_and(|e| expr_has_outer_variables(e))
                || when_then.iter().any(|(w, t)| expr_has_outer_variables(w) || expr_has_outer_variables(t))
                || else_expression.as_ref().is_some_and(|e| expr_has_outer_variables(e))
        }
        _ => false,
    }
}

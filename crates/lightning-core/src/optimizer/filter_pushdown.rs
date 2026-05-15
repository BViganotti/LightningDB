use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::HashSet;

pub struct FilterPushDown;

impl Default for FilterPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterPushDown {
    pub fn new() -> Self {
        Self
    }

    fn extract_variables(expr: &BoundExpression, vars: &mut HashSet<String>) {
        match expr {
            BoundExpression::Variable(name, _) => {
                vars.insert(name.clone());
            }
            BoundExpression::PropertyLookup(name, _, _) => {
                vars.insert(name.clone());
            }
            BoundExpression::Comparison(left, _, right)
            | BoundExpression::Arithmetic(left, _, right)
            | BoundExpression::Logical(left, _, right) => {
                Self::extract_variables(left, vars);
                Self::extract_variables(right, vars);
            }
            BoundExpression::Not(expr) => {
                Self::extract_variables(expr, vars);
            }
            BoundExpression::Function(_, args, _)
            | BoundExpression::List(args, _)
            | BoundExpression::Aggregate(_, args, _) => {
                for arg in args {
                    Self::extract_variables(arg, vars);
                }
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                if let Some(e) = expression {
                    Self::extract_variables(e, vars);
                }
                for (w, t) in when_then {
                    Self::extract_variables(w, vars);
                    Self::extract_variables(t, vars);
                }
                if let Some(e) = else_expression {
                    Self::extract_variables(e, vars);
                }
            }
            BoundExpression::Lambda(_, body) => {
                Self::extract_variables(body, vars);
            }
            BoundExpression::Parameter(_)
            | BoundExpression::Literal(_)
            | BoundExpression::NextVal(_) => {}
            BoundExpression::Exists(steps) => {
                for (m, w) in steps {
                    for element in &m.elements {
                        match element {
                            crate::planner::binder::BoundMatchElement::Node(_, var, _)
                            | crate::planner::binder::BoundMatchElement::Rel(_, var, _, _, _) => {
                                vars.insert(var.clone());
                            }
                        }
                    }
                    if let Some(bw) = w {
                        Self::extract_variables(&bw.expression, vars);
                    }
                }
            }
        }
    }

    fn provided_variables(op: &LogicalOperator, vars: &mut HashSet<String>) {
        match op {
            LogicalOperator::Scan(_, var, ..) | LogicalOperator::IndexScan(_, var, ..) => {
                vars.insert(var.clone());
            }
            LogicalOperator::Unwind(child, _, alias) => {
                Self::provided_variables(child, vars);
                vars.insert(alias.clone());
            }
            LogicalOperator::SingleRow => {}
            LogicalOperator::Filter(child, _) => {
                Self::provided_variables(child, vars);
            }
            LogicalOperator::Projection(_child, items) => {
                for item in items {
                    vars.insert(item.alias.clone());
                }
            }
            LogicalOperator::Join(left, right, _) => {
                Self::provided_variables(left, vars);
                Self::provided_variables(right, vars);
            }
            LogicalOperator::Aggregate { child, .. } => {
                Self::provided_variables(child, vars);
            }
            LogicalOperator::CreateNode(child, pat) => {
                if let Some(c) = child {
                    Self::provided_variables(c, vars);
                }
                if let Some(var) = &pat.variable {
                    vars.insert(var.clone());
                }
            }
            LogicalOperator::CreateRel(child, pat) => {
                if let Some(c) = child {
                    Self::provided_variables(c, vars);
                }
                if let Some(var) = &pat.variable {
                    vars.insert(var.clone());
                }
            }
            LogicalOperator::Delete(child, ..)
            | LogicalOperator::Set(child, _)
            | LogicalOperator::Sort(child, _)
            | LogicalOperator::Limit(child, _)
            | LogicalOperator::Skip(child, _)
            | LogicalOperator::Flatten(child)
            | LogicalOperator::UnwindDedup(child, _)
            | LogicalOperator::Merge { child, .. }
            | LogicalOperator::Union(child, ..)
            | LogicalOperator::OptionalMatch(child, ..)
            | LogicalOperator::With(child, ..)
            | LogicalOperator::SemiJoin(child, ..)
            | LogicalOperator::Profile(child)
            | LogicalOperator::Explain(child)
            | LogicalOperator::Accumulate(child)
            | LogicalOperator::Distinct(child, _) => {
                Self::provided_variables(child, vars);
            }
            LogicalOperator::CountRelTable { alias, .. } => {
                vars.insert(alias.clone());
            }
            LogicalOperator::RecursiveJoin {
                child,
                src_var,
                dst_var,
                rel_var,
                ..
            } => {
                Self::provided_variables(child, vars);
                vars.insert(src_var.clone());
                vars.insert(dst_var.clone());
                vars.insert(rel_var.clone());
            }
            LogicalOperator::AllShortestPaths {
                child,
                src_var_name,
                dst_var_name,
                path_var_name,
                ..
            } => {
                Self::provided_variables(child, vars);
                vars.insert(src_var_name.clone());
                vars.insert(dst_var_name.clone());
                vars.insert(path_var_name.clone());
            }
            _ => {}
        }
    }

    fn push_down(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Filter(child, condition) => {
                let pushed_child = self.push_down(*child)?;

                let mut condition_vars = HashSet::new();
                Self::extract_variables(&condition, &mut condition_vars);

                match pushed_child {
                    LogicalOperator::Scan(table, var, mask, projected, filter) => {
                        // Push filter into scan operator
                        // The scan operator will use padded batches to handle column indices
                        let new_filter = match filter {
                            Some(existing) => {
                                // Combine existing filter with new condition
                                BoundExpression::Logical(
                                    Box::new(existing),
                                    crate::parser::ast::LogicalOperator::And,
                                    Box::new(condition),
                                )
                            }
                            None => condition,
                        };
                        Ok(LogicalOperator::Scan(
                            table,
                            var,
                            mask,
                            projected,
                            Some(new_filter),
                        ))
                    }
                    LogicalOperator::Join(left, right, join_cond) => {
                        let mut left_vars = HashSet::new();
                        Self::provided_variables(&left, &mut left_vars);

                        let mut right_vars = HashSet::new();
                        Self::provided_variables(&right, &mut right_vars);

                        let can_push_left = condition_vars.is_subset(&left_vars);
                        let can_push_right = condition_vars.is_subset(&right_vars);

                        if can_push_left {
                            let new_left = LogicalOperator::Filter(left, condition);
                            Ok(LogicalOperator::Join(
                                Box::new(self.push_down(new_left)?),
                                right,
                                join_cond,
                            ))
                        } else if can_push_right {
                            let new_right = LogicalOperator::Filter(right, condition);
                            Ok(LogicalOperator::Join(
                                left,
                                Box::new(self.push_down(new_right)?),
                                join_cond,
                            ))
                        } else {
                            Ok(LogicalOperator::Filter(
                                Box::new(LogicalOperator::Join(left, right, join_cond)),
                                condition,
                            ))
                        }
                    }
                    LogicalOperator::Intersect {
                        probe_child,
                        build_children,
                        key_vars,
                        intersect_var,
                    } => {
                        let mut probe_vars = HashSet::new();
                        Self::provided_variables(&probe_child, &mut probe_vars);
                        if condition_vars.is_subset(&probe_vars) {
                            let new_probe = LogicalOperator::Filter(probe_child, condition);
                            Ok(LogicalOperator::Intersect {
                                probe_child: Box::new(self.push_down(new_probe)?),
                                build_children,
                                key_vars,
                                intersect_var,
                            })
                        } else {
                            Ok(LogicalOperator::Filter(
                                Box::new(LogicalOperator::Intersect {
                                    probe_child,
                                    build_children,
                                    key_vars,
                                    intersect_var,
                                }),
                                condition,
                            ))
                        }
                    }
                    LogicalOperator::Union(left, right, is_all) => {
                        let left_pushed = LogicalOperator::Filter(left, condition.clone());
                        let right_pushed = LogicalOperator::Filter(right, condition);
                        Ok(LogicalOperator::Union(
                            Box::new(self.push_down(left_pushed)?),
                            Box::new(self.push_down(right_pushed)?),
                            is_all,
                        ))
                    }
                    LogicalOperator::SemiJoin(left, right, join_cond, is_anti) => {
                        let mut left_vars = HashSet::new();
                        Self::provided_variables(&left, &mut left_vars);
                        if condition_vars.is_subset(&left_vars) {
                            let new_left = LogicalOperator::Filter(left, condition);
                            Ok(LogicalOperator::SemiJoin(
                                Box::new(self.push_down(new_left)?),
                                right,
                                join_cond,
                                is_anti,
                            ))
                        } else {
                            Ok(LogicalOperator::Filter(
                                Box::new(LogicalOperator::SemiJoin(
                                    left, right, join_cond, is_anti,
                                )),
                                condition,
                            ))
                        }
                    }
                    _ => Ok(LogicalOperator::Filter(Box::new(pushed_child), condition)),
                }
            }
            LogicalOperator::Projection(child, items) => Ok(LogicalOperator::Projection(
                Box::new(self.push_down(*child)?),
                items,
            )),
            LogicalOperator::Join(left, right, cond) => Ok(LogicalOperator::Join(
                Box::new(self.push_down(*left)?),
                Box::new(self.push_down(*right)?),
                cond,
            )),
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            } => Ok(LogicalOperator::Aggregate {
                child: Box::new(self.push_down(*child)?),
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            }),
            LogicalOperator::Sort(child, items) => Ok(LogicalOperator::Sort(
                Box::new(self.push_down(*child)?),
                items,
            )),
            LogicalOperator::TopK(child, items, limit) => Ok(LogicalOperator::TopK(
                Box::new(self.push_down(*child)?),
                items,
                limit,
            )),
            LogicalOperator::Limit(child, limit) => Ok(LogicalOperator::Limit(
                Box::new(self.push_down(*child)?),
                limit,
            )),
            LogicalOperator::Skip(child, skip) => Ok(LogicalOperator::Skip(
                Box::new(self.push_down(*child)?),
                skip,
            )),
            LogicalOperator::CreateNode(child, pat) => {
                let new_child = child.map(|c| self.push_down(*c)).transpose()?.map(Box::new);
                Ok(LogicalOperator::CreateNode(new_child, pat))
            }
            LogicalOperator::CreateRel(child, pat) => {
                let new_child = child.map(|c| self.push_down(*c)).transpose()?.map(Box::new);
                Ok(LogicalOperator::CreateRel(new_child, pat))
            }
            LogicalOperator::Delete(child, vars, detach) => Ok(LogicalOperator::Delete(
                Box::new(self.push_down(*child)?),
                vars,
                detach,
            )),
            LogicalOperator::Set(child, assignments) => Ok(LogicalOperator::Set(
                Box::new(self.push_down(*child)?),
                assignments,
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
            } => Ok(LogicalOperator::RecursiveJoin {
                child: Box::new(self.push_down(*child)?),
                rel_table,
                rel_var,
                src_var,
                dst_node_table,
                dst_var,
                bounds,
                mask_id,
            }),
            LogicalOperator::Unwind(child, expr, alias) => Ok(LogicalOperator::Unwind(
                Box::new(self.push_down(*child)?),
                expr,
                alias,
            )),
            LogicalOperator::SemiMasker(child, node_var, mask_id) => Ok(
                LogicalOperator::SemiMasker(Box::new(self.push_down(*child)?), node_var, mask_id),
            ),
            LogicalOperator::Flatten(child) => {
                Ok(LogicalOperator::Flatten(Box::new(self.push_down(*child)?)))
            }
            LogicalOperator::UnwindDedup(child, expr) => Ok(LogicalOperator::UnwindDedup(
                Box::new(self.push_down(*child)?),
                expr,
            )),
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => Ok(LogicalOperator::Merge {
                child: Box::new(self.push_down(*child)?),
                pattern,
                on_create_assignments,
                on_match_assignments,
            }),
            LogicalOperator::Intersect {
                probe_child,
                build_children,
                key_vars,
                intersect_var,
            } => Ok(LogicalOperator::Intersect {
                probe_child: Box::new(self.push_down(*probe_child)?),
                build_children: build_children
                    .into_iter()
                    .map(|c| self.push_down(c))
                    .collect::<Result<Vec<_>>>()?,
                key_vars,
                intersect_var,
            }),
            LogicalOperator::Union(left, right, is_all) => Ok(LogicalOperator::Union(
                Box::new(self.push_down(*left)?),
                Box::new(self.push_down(*right)?),
                is_all,
            )),
            LogicalOperator::AllShortestPaths {
                child,
                rel_table_name,
                src_var_name,
                dst_var_name,
                path_var_name,
                max_depth,
            } => Ok(LogicalOperator::AllShortestPaths {
                child: Box::new(self.push_down(*child)?),
                rel_table_name,
                src_var_name,
                dst_var_name,
                path_var_name,
                max_depth,
            }),
            LogicalOperator::OptionalMatch(child, branch) => Ok(LogicalOperator::OptionalMatch(
                Box::new(self.push_down(*child)?),
                Box::new(self.push_down(*branch)?),
            )),
            LogicalOperator::With(child, items, where_expr) => Ok(LogicalOperator::With(
                Box::new(self.push_down(*child)?),
                items,
                where_expr,
            )),
            LogicalOperator::Profile(child) => {
                Ok(LogicalOperator::Profile(Box::new(self.push_down(*child)?)))
            }
            LogicalOperator::Explain(child) => {
                Ok(LogicalOperator::Explain(Box::new(self.push_down(*child)?)))
            }
            LogicalOperator::Accumulate(child) => Ok(LogicalOperator::Accumulate(Box::new(
                self.push_down(*child)?,
            ))),
            LogicalOperator::Distinct(child, columns) => Ok(LogicalOperator::Distinct(
                Box::new(self.push_down(*child)?),
                columns,
            )),
            _ => Ok(plan),
        }
    }
}

impl Rule for FilterPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.push_down(plan)
    }
}

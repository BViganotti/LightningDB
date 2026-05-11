use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::HashSet;

pub struct AggKeyDependencyOptimizer;

impl AggKeyDependencyOptimizer {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                aggregates,
                ..
            } => {
                let pushed_child = self.rewrite(*child)?;

                // We need to identify which group_by_cols are redundant.
                // Redundant columns are those that depend on a primary key that is also in group_by_cols.
                // In our current LogicalOperator, we only have indices (usize).
                // To do this properly, we need to know WHICH table and WHICH property each index refers to.

                // This information is usually available in the Projection child.
                if let LogicalOperator::Projection(_, ref items) = pushed_child {
                    let mut primary_vars = HashSet::new();
                    // First pass: identify primary variables in group_by
                    for &idx in &group_by_cols {
                        if idx < items.len() {
                            let item = &items[idx];
                            if let crate::planner::binder::BoundExpression::PropertyLookup(
                                var_name,
                                prop_idx,
                                _,
                            ) = &item.expression
                            {
                                // Assume 0 is always internal ID or Primary Key for now as a heuristic
                                if *prop_idx == 0 || item.alias.ends_with("._id") {
                                    primary_vars.insert(var_name.clone());
                                }
                            }
                        }
                    }

                    let mut new_group_by = Vec::new();
                    let mut dependent_group_by = Vec::new();

                    for idx in group_by_cols {
                        let mut is_dependent = false;
                        if idx < items.len() {
                            let item = &items[idx];
                            if let crate::planner::binder::BoundExpression::PropertyLookup(
                                v_name,
                                p_idx,
                                _,
                            ) = &item.expression
                            {
                                if *p_idx != 0 && primary_vars.contains(v_name) {
                                    is_dependent = true;
                                }
                            }
                        }

                        if is_dependent {
                            dependent_group_by.push(idx);
                        } else {
                            new_group_by.push(idx);
                        }
                    }

                    Ok(LogicalOperator::Aggregate {
                        child: Box::new(pushed_child),
                        group_by_cols: new_group_by,
                        dependent_group_by_cols: dependent_group_by,
                        aggregates,
                    })
                } else {
                    Ok(LogicalOperator::Aggregate {
                        child: Box::new(pushed_child),
                        group_by_cols,
                        dependent_group_by_cols: Vec::new(),
                        aggregates,
                    })
                }
            }
            _ => {
                if let Some(child) = op.get_child() {
                    // Generic recursion for non-aggregate operators
                    // This is tricky because we need to rebuild the operator.
                    // For now, only applying to the top-level or specific branches.
                    Ok(op)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

impl Rule for AggKeyDependencyOptimizer {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

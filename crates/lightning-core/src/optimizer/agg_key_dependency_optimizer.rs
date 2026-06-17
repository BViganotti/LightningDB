use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::HashSet;

pub struct AggKeyDependencyOptimizer;

impl Default for AggKeyDependencyOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

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
                    // Only consider a GROUP BY column as a primary key if it
                    // references property index 0 (the internal _id column).
                    // Property index 0 is guaranteed to be unique per table,
                    // so any other column from the same variable is functionally
                    // dependent on it.
                    let primary_vars: HashSet<String> = group_by_cols
                        .iter()
                        .filter_map(|&idx| {
                            if idx < items.len() {
                                if let crate::planner::binder::BoundExpression::PropertyLookup(
                                    var_name,
                                    prop_idx,
                                    _,
                                ) = &items[idx].expression
                                {
                                    if *prop_idx == 0 {
                                        return Some(var_name.clone());
                                    }
                                }
                            }
                            None
                        })
                        .collect();

                    let mut new_group_by = Vec::new();
                    let mut dependent_group_by = Vec::new();

                    for idx in group_by_cols {
                        let mut is_dependent = false;
                        if idx < items.len() {
                            if let crate::planner::binder::BoundExpression::PropertyLookup(
                                v_name,
                                p_idx,
                                _,
                            ) = &items[idx].expression
                            {
                                // Only mark as dependent when:
                                // 1. The same variable's PK (prop 0) is in group_by
                                // 2. This is a non-PK property of the same variable
                                // This avoids incorrect transitive dependencies
                                // through join columns across different tables.
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
                if let Some(child) = op.clone().get_child().cloned() {
                    let rewritten = self.rewrite(child)?;
                    let mut new_op = op.clone();
                    new_op.set_child(rewritten);
                    Ok(new_op)
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

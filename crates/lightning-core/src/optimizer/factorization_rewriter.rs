use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::HashSet;

pub struct FactorizationRewriter;

impl Default for FactorizationRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl FactorizationRewriter {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;

                // If the join condition is constant true and there are common variables?
                // Actually, factorization is most useful for Cartesian products in subqueries.

                let mut left_vars = HashSet::new();
                left_rewritten.get_variables(&mut left_vars);

                let mut right_vars = HashSet::new();
                right_rewritten.get_variables(&mut right_vars);

                // If they are disjoint, we can potentially factorize (Ladybug specific)
                // In Lightning, we just keep the Join for now, but mark it for factorization in physical plan

                Ok(LogicalOperator::Join(
                    Box::new(left_rewritten),
                    Box::new(right_rewritten),
                    cond,
                ))
            }
            _ => {
                if let Some(child) = op.get_child() {
                    let mut op_with_child = op.clone();
                    op_with_child.set_child(self.rewrite(child.clone())?);
                    Ok(op_with_child)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

impl Rule for FactorizationRewriter {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

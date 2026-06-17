//! #58: This optimizer is currently dead code — it traverses the plan tree but
//! never modifies it. The factorization rewrite (factorizing Cartesian products
//! in correlated subqueries) requires the physical planner to support a
//! `FactorizedJoin` operator. Until that operator exists, this rule is a no-op.
//! Tracked in: DEEP_AUDIT_FULL_2024.md item #58.

use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct FactorizationRewriter;

impl Default for FactorizationRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl FactorizationRewriter {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;
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

#[allow(dead_code)]
impl Rule for FactorizationRewriter {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

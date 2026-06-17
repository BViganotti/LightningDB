//! #58: This optimizer is currently dead code — it traverses the plan tree but
//! never modifies it. Foreign join push-down would push Join operations into
//! remote storage backends (e.g., joining two tables on a foreign data source).
//! Lightning's scan operators don't yet support remote push-down. Until they do,
//! this rule is a no-op.
//! Tracked in: DEEP_AUDIT_FULL_2024.md item #58.

use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct ForeignJoinPushDown;

impl Default for ForeignJoinPushDown {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ForeignJoinPushDown {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                let pushed_left = self.rewrite(*left)?;
                let pushed_right = self.rewrite(*right)?;
                Ok(LogicalOperator::Join(
                    Box::new(pushed_left),
                    Box::new(pushed_right),
                    cond,
                ))
            }
            _ => {
                if let Some(child) = op.clone().get_child().cloned() {
                    let new_child = self.rewrite(child)?;
                    let mut new_op = op.clone();
                    new_op.set_child(new_child);
                    Ok(new_op)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

#[allow(dead_code)]
impl Rule for ForeignJoinPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

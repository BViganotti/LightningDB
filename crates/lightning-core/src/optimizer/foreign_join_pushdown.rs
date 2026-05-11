use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct ForeignJoinPushDown;

impl ForeignJoinPushDown {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                let pushed_left = self.rewrite(*left)?;
                let pushed_right = self.rewrite(*right)?;

                // Pattern for Foreign Join Push-Down:
                // If both tables are ForeignScans to the same source, they can be joined there.
                // Currently, lightning scans are generic.
                // In the future, we will check if the storage source supports push-down joins.

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

impl Rule for ForeignJoinPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

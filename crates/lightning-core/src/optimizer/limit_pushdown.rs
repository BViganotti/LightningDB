use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct LimitPushDown {}

impl Default for LimitPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl LimitPushDown {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            // Push Limit past Projection: LIMIT(PROJECTION(x)) → PROJECTION(LIMIT(x))
            // This is safe because Projection doesn't change row count.
            LogicalOperator::Limit(child, limit) => {
                let pushed_child = self.push_down(*child)?;
                match pushed_child {
                    LogicalOperator::Projection(grandchild, items) => {
                        Ok(LogicalOperator::Projection(
                            Box::new(LogicalOperator::Limit(grandchild, limit)),
                            items,
                        ))
                    }
                    _ => Ok(LogicalOperator::Limit(Box::new(pushed_child), limit)),
                }
            }
            LogicalOperator::Sort(child, order_by) => {
                Ok(LogicalOperator::Sort(
                    Box::new(self.push_down(*child)?),
                    order_by,
                ))
            }
            LogicalOperator::Join(left, right, cond) => {
                Ok(LogicalOperator::Join(
                    Box::new(self.push_down(*left)?),
                    Box::new(self.push_down(*right)?),
                    cond,
                ))
            }
            LogicalOperator::Union(left, right, is_all) => {
                Ok(LogicalOperator::Union(
                    Box::new(self.push_down(*left)?),
                    Box::new(self.push_down(*right)?),
                    is_all,
                ))
            }
            _ => {
                if let Some(child) = op.get_child() {
                    let mut op_with_child = op.clone();
                    op_with_child.set_child(self.push_down(child.clone())?);
                    Ok(op_with_child)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

impl Rule for LimitPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.push_down(plan)
    }
}

use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct LimitPushDown {}

impl LimitPushDown {
    pub fn new() -> Self {
        Self {}
    }

    fn push_down(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Sort(child, order_by) => {
                // If child is a Limit, we can't easily push down without Top-K
                Ok(LogicalOperator::Sort(
                    Box::new(self.push_down(*child)?),
                    order_by,
                ))
            }
            LogicalOperator::Limit(child, limit) => {
                // Push limit into child if child supports it (e.g. Scan)
                // For now, simple recursive
                Ok(LogicalOperator::Limit(
                    Box::new(self.push_down(*child)?),
                    limit,
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

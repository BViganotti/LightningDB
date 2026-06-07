use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct OrderByPushDown;

impl Default for OrderByPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderByPushDown {
    pub fn new() -> Self {
        Self
    }

    fn push_down(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Sort(child, items) => {
                let pushed_child = self.push_down(*child)?;
                // Simple optimization: if child is already a Sort, maybe combine or eliminate.
                // For now, just push down through Projection/Filter if safe.
                match pushed_child {
                    LogicalOperator::Projection(grandchild, p_items) => {
                        // We can push Sort below Projection if all Sort items refer to columns in the grandchild.
                        // This involves remapping indices, which is complex.
                        // For now, keep it as is.
                        Ok(LogicalOperator::Sort(
                            Box::new(LogicalOperator::Projection(grandchild, p_items)),
                            items,
                        ))
                    }
                    _ => Ok(LogicalOperator::Sort(Box::new(pushed_child), items)),
                }
            }
            _ => {
                // Generic recursion should be handled by the optimizer loop or explicitly.
                Ok(plan)
            }
        }
    }
}

impl Rule for OrderByPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.push_down(plan)
    }
}

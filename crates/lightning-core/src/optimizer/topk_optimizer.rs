use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct TopKOptimizer;

impl Default for TopKOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl TopKOptimizer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let plan = self.push_down(plan)?;
        Ok(plan)
    }

    fn push_down(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Limit(child, limit) => {
                let pushed_child = self.push_down(*child)?;
                if let LogicalOperator::Sort(grandchild, sort_items) = pushed_child {
                    Ok(LogicalOperator::TopK(grandchild, sort_items, limit))
                } else {
                    Ok(LogicalOperator::Limit(Box::new(pushed_child), limit))
                }
            }
            LogicalOperator::Filter(child, expr) => Ok(LogicalOperator::Filter(
                Box::new(self.push_down(*child)?),
                expr,
            )),
            LogicalOperator::Projection(child, items) => Ok(LogicalOperator::Projection(
                Box::new(self.push_down(*child)?),
                items,
            )),
            LogicalOperator::Sort(child, items) => Ok(LogicalOperator::Sort(
                Box::new(self.push_down(*child)?),
                items,
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
            _ => Ok(plan),
        }
    }
}

impl crate::optimizer::Rule for TopKOptimizer {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.optimize(plan)
    }
}

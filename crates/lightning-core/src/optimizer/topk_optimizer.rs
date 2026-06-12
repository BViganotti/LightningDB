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

    /// Look through Projection/Filter layers to find a Sort operator.
    fn find_sort_through_passthrough(&self, plan: &LogicalOperator) -> Option<Vec<crate::planner::binder::BoundOrderByItem>> {
        match plan {
            LogicalOperator::Sort(_, items) => Some(items.clone()),
            LogicalOperator::Projection(child, _) | LogicalOperator::Filter(child, _) => {
                self.find_sort_through_passthrough(child)
            }
            _ => None,
        }
    }

    /// Extract the Sort's child, preserving any Projection/Filter layers above it.
    fn extract_sort_child(&self, plan: &LogicalOperator) -> Box<LogicalOperator> {
        match plan {
            LogicalOperator::Sort(child, _) => Box::new(child.as_ref().clone()),
            LogicalOperator::Projection(child, items) => {
                Box::new(LogicalOperator::Projection(
                    self.extract_sort_child(child),
                    items.clone(),
                ))
            }
            LogicalOperator::Filter(child, expr) => {
                Box::new(LogicalOperator::Filter(
                    self.extract_sort_child(child),
                    expr.clone(),
                ))
            }
            _ => Box::new(plan.clone()),
        }
    }

    fn push_down(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Limit(child, limit) => {
                let pushed_child = self.push_down(*child)?;
                // Look through Projection and Filter to find Sort for TopK fusion
                if let Some(sort_items) = self.find_sort_through_passthrough(&pushed_child) {
                    // Found Limit → [Projection/Filter]* → Sort pattern.
                    // Extract the Sort's child and build TopK.
                    let sort_child = self.extract_sort_child(&pushed_child);
                    Ok(LogicalOperator::TopK(sort_child, sort_items, limit))
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
                let child_opt = plan.get_child().map(|c| c.clone());
                if let Some(child) = child_opt {
                    let mut plan_clone = plan.clone();
                    plan_clone.set_child(self.push_down(child)?);
                    Ok(plan_clone)
                } else {
                    Ok(plan)
                }
            }
        }
    }
}

impl crate::optimizer::Rule for TopKOptimizer {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.optimize(plan)
    }
}

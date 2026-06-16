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

    /// Extract the Sort's child, preserving any Projection/Filter layers that
    /// MUST stay above the Sort (because Sort's ORDER BY expressions reference
    /// entity-table column indices, not projected-output column indices).
    /// Returns (sort_items, child_below_sort) or None if no Sort found.
    fn extract_sort_and_child(&self, plan: &LogicalOperator) -> Option<(Vec<crate::planner::binder::BoundOrderByItem>, Box<LogicalOperator>)> {
        match plan {
            LogicalOperator::Sort(child, items) => {
                Some((items.clone(), Box::new(child.as_ref().clone())))
            }
            LogicalOperator::Projection(..) | LogicalOperator::Filter(..) => {
                // These pass through — don't descend, just return None.
                // Projection/Filter must stay ABOVE Sort because Sort's ORDER BY
                // uses entity-table PropertyLookup indices, not projected indices.
                None
            }
            _ => None,
        }
    }

    fn push_down(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Limit(child, limit) => {
                let pushed_child = self.push_down(*child)?;
                // Look through Sort to find TopK fusion.
                // Projection/Filter are NOT passed through because they must stay
                // above Sort (their output columns differ from Sort's input columns).
                if let Some((sort_items, sort_child)) = self.extract_sort_and_child(&pushed_child) {
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

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
                match pushed_child {
                    // Eliminate redundant Sort: Sort(Sort(x, items1), items2) → Sort(x, items2)
                    LogicalOperator::Sort(grandchild, _) => {
                        Ok(LogicalOperator::Sort(grandchild, items))
                    }
                    // Push Sort past Projection when safe (all sort columns are direct references)
                    LogicalOperator::Projection(grandchild, p_items) => {
                        // Check if all sort items are simple PropertyLookup that
                        // exist in the grandchild's output
                        let can_push = items.iter().all(|item| {
                            matches!(&item.expression, crate::planner::binder::BoundExpression::PropertyLookup(..))
                        });
                        if can_push {
                            // Push Sort below Projection
                            Ok(LogicalOperator::Projection(
                                Box::new(LogicalOperator::Sort(grandchild, items)),
                                p_items,
                            ))
                        } else {
                            Ok(LogicalOperator::Sort(
                                Box::new(LogicalOperator::Projection(grandchild, p_items)),
                                items,
                            ))
                        }
                    }
                    _ => Ok(LogicalOperator::Sort(Box::new(pushed_child), items)),
                }
            }
            _ => {
                if let Some(child) = plan.clone().get_child().cloned() {
                    let pushed_child = self.push_down(child)?;
                    let mut new_plan = plan.clone();
                    new_plan.set_child(pushed_child);
                    Ok(new_plan)
                } else {
                    Ok(plan)
                }
            }
        }
    }
}

impl Rule for OrderByPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.push_down(plan)
    }
}

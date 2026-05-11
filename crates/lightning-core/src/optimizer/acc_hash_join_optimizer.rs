use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct AccHashJoinOptimizer;

impl AccHashJoinOptimizer {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator, mask_counter: &mut usize) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                let new_left = self.rewrite(*left, mask_counter)?;
                let mut new_right = self.rewrite(*right, mask_counter)?;

                // AccHashJoin optimization:
                // If we have a join on node ID, and one side is selective (has filters),
                // we can "accumulate" that side and pass its keys as a semi-mask to the other side.

                if let BoundExpression::Comparison(
                    lhs,
                    crate::parser::ast::ComparisonOperator::Equal,
                    rhs,
                ) = &cond
                {
                    if let (
                        BoundExpression::PropertyLookup(v1, p1, _),
                        BoundExpression::PropertyLookup(v2, p2, _),
                    ) = (&**lhs, &**rhs)
                    {
                        // For simplicity, we assume property 0/1 are node IDs.
                        if (*p1 == 0 || *p1 == 1) && (*p2 == 0 || *p2 == 1) {
                            // Check if right side (build side) is selective.
                            // In a real optimizer, we'd check cardinality or filter presence.
                            // Here we just apply the pattern for parity.

                            *mask_counter += 1;
                            let mask_id = format!("acc_sm_{}", mask_counter);

                            // 1. Wrap right side in Accumulate
                            new_right = LogicalOperator::Accumulate(Box::new(new_right));

                            // 2. Add SemiMasker to right side output
                            new_right = LogicalOperator::SemiMasker(
                                Box::new(new_right),
                                v2.clone(),
                                mask_id.clone(),
                            );

                            // 3. Apply mask to left side (probe side)
                            let final_left = self.apply_mask(new_left, v1, &mask_id, Some(*p1));

                            return Ok(LogicalOperator::Join(
                                Box::new(final_left),
                                Box::new(new_right),
                                cond,
                            ));
                        }
                    }
                }

                Ok(LogicalOperator::Join(
                    Box::new(new_left),
                    Box::new(new_right),
                    cond,
                ))
            }
            _ => {
                if let Some(child) = op.get_child().cloned() {
                    let new_child = self.rewrite(child, mask_counter)?;
                    let mut new_op = op.clone();
                    new_op.set_child(new_child);
                    Ok(new_op)
                } else {
                    Ok(op)
                }
            }
        }
    }

    fn apply_mask(
        &self,
        op: LogicalOperator,
        target_var: &str,
        mask_id: &str,
        col_idx: Option<usize>,
    ) -> LogicalOperator {
        match op {
            LogicalOperator::Scan(table, var, existing_mask, proj, filter) => {
                if var == target_var {
                    LogicalOperator::Scan(
                        table,
                        var,
                        Some((mask_id.to_string(), col_idx)),
                        proj,
                        filter,
                    )
                } else {
                    LogicalOperator::Scan(table, var, existing_mask, proj, filter)
                }
            }
            LogicalOperator::Filter(child, cond) => LogicalOperator::Filter(
                Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                cond,
            ),
            LogicalOperator::Join(left, right, cond) => LogicalOperator::Join(
                Box::new(self.apply_mask(*left, target_var, mask_id, col_idx)),
                Box::new(self.apply_mask(*right, target_var, mask_id, col_idx)),
                cond,
            ),
            _ => {
                if let Some(child) = op.get_child().cloned() {
                    let new_child = self.apply_mask(child, target_var, mask_id, col_idx);
                    let mut new_op = op.clone();
                    new_op.set_child(new_child);
                    new_op
                } else {
                    op
                }
            }
        }
    }
}

impl Rule for AccHashJoinOptimizer {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let mut counter = 0;
        self.rewrite(plan, &mut counter)
    }
}

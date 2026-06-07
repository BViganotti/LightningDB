use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct SemiJoinPushDown;

impl Default for SemiJoinPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl SemiJoinPushDown {
    pub fn new() -> Self {
        Self
    }

    fn apply_mask(
        &self,
        plan: LogicalOperator,
        target_var: &str,
        mask_id: &str,
        col_idx: Option<usize>,
    ) -> LogicalOperator {
        match plan {
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
            LogicalOperator::Projection(child, items) => LogicalOperator::Projection(
                Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                items,
            ),
            LogicalOperator::Join(left, right, cond) => LogicalOperator::Join(
                Box::new(self.apply_mask(*left, target_var, mask_id, col_idx)),
                Box::new(self.apply_mask(*right, target_var, mask_id, col_idx)),
                cond,
            ),
            LogicalOperator::SemiMasker(child, var, m_id) => LogicalOperator::SemiMasker(
                Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                var,
                m_id,
            ),
            LogicalOperator::Unwind(child, expr, alias) => LogicalOperator::Unwind(
                Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                expr,
                alias,
            ),
            LogicalOperator::Intersect {
                probe_child,
                build_children,
                key_vars,
                intersect_var,
            } => LogicalOperator::Intersect {
                probe_child: Box::new(self.apply_mask(*probe_child, target_var, mask_id, col_idx)),
                build_children: build_children
                    .into_iter()
                    .map(|b| self.apply_mask(b, target_var, mask_id, col_idx))
                    .collect(),
                key_vars,
                intersect_var,
            },
            LogicalOperator::Flatten(child) => LogicalOperator::Flatten(Box::new(
                self.apply_mask(*child, target_var, mask_id, col_idx),
            )),
            LogicalOperator::UnwindDedup(child, expr) => LogicalOperator::UnwindDedup(
                Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                expr,
            ),
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => LogicalOperator::Merge {
                child: Box::new(self.apply_mask(*child, target_var, mask_id, col_idx)),
                pattern: pattern.clone(),
                on_create_assignments: on_create_assignments.clone(),
                on_match_assignments: on_match_assignments.clone(),
            },
            LogicalOperator::Union(left, right, is_all) => LogicalOperator::Union(
                Box::new(self.apply_mask(*left, target_var, mask_id, col_idx)),
                Box::new(self.apply_mask(*right, target_var, mask_id, col_idx)),
                is_all,
            ),
            _ => plan,
        }
    }

    fn push_down(
        &self,
        plan: LogicalOperator,
        mask_counter: &mut usize,
    ) -> Result<LogicalOperator> {
        match plan {
            LogicalOperator::Join(left, right, cond) => {
                let mut new_left = self.push_down(*left, mask_counter)?;
                let mut new_right = self.push_down(*right, mask_counter)?;

                if let BoundExpression::Comparison(
                    lhs,
                    crate::parser::ast::ComparisonOperator::Equal,
                    rhs,
                ) = &cond
                {
                    if let (
                        BoundExpression::PropertyLookup(v1, p1, _),
                        BoundExpression::PropertyLookup(v2, _, _),
                    ) = (&**lhs, &**rhs)
                    {
                        *mask_counter += 1;
                        let mask_id = format!("sm_{mask_counter}");
                        let left_mask_idx = if *p1 == 0 || *p1 == 1 {
                            Some(*p1)
                        } else {
                            None
                        };
                        new_left = self.apply_mask(new_left, v1, &mask_id, left_mask_idx);
                        new_right = LogicalOperator::SemiMasker(
                            Box::new(new_right),
                            v2.clone(),
                            mask_id.clone(),
                        );
                    }
                }

                Ok(LogicalOperator::Join(
                    Box::new(new_left),
                    Box::new(new_right),
                    cond,
                ))
            }
            LogicalOperator::Filter(child, cond) => Ok(LogicalOperator::Filter(
                Box::new(self.push_down(*child, mask_counter)?),
                cond,
            )),
            LogicalOperator::Projection(child, items) => Ok(LogicalOperator::Projection(
                Box::new(self.push_down(*child, mask_counter)?),
                items,
            )),
            LogicalOperator::SemiMasker(child, var, mask_id) => Ok(LogicalOperator::SemiMasker(
                Box::new(self.push_down(*child, mask_counter)?),
                var,
                mask_id,
            )),
            LogicalOperator::Unwind(child, expr, alias) => Ok(LogicalOperator::Unwind(
                Box::new(self.push_down(*child, mask_counter)?),
                expr,
                alias,
            )),
            LogicalOperator::Sort(child, items) => Ok(LogicalOperator::Sort(
                Box::new(self.push_down(*child, mask_counter)?),
                items,
            )),
            LogicalOperator::Limit(child, limit) => Ok(LogicalOperator::Limit(
                Box::new(self.push_down(*child, mask_counter)?),
                limit,
            )),
            LogicalOperator::Skip(child, skip) => Ok(LogicalOperator::Skip(
                Box::new(self.push_down(*child, mask_counter)?),
                skip,
            )),
            LogicalOperator::Intersect {
                probe_child,
                build_children,
                key_vars,
                intersect_var,
            } => {
                let new_probe = self.push_down(*probe_child, mask_counter)?;
                let mut new_builds = Vec::new();
                for build in build_children {
                    new_builds.push(self.push_down(build, mask_counter)?);
                }
                Ok(LogicalOperator::Intersect {
                    probe_child: Box::new(new_probe),
                    build_children: new_builds,
                    key_vars,
                    intersect_var,
                })
            }
            LogicalOperator::Flatten(child) => Ok(LogicalOperator::Flatten(Box::new(
                self.push_down(*child, mask_counter)?,
            ))),
            LogicalOperator::UnwindDedup(child, expr) => Ok(LogicalOperator::UnwindDedup(
                Box::new(self.push_down(*child, mask_counter)?),
                expr,
            )),
            LogicalOperator::Merge {
                child,
                pattern,
                on_create_assignments,
                on_match_assignments,
            } => Ok(LogicalOperator::Merge {
                child: Box::new(self.push_down(*child, mask_counter)?),
                pattern: pattern.clone(),
                on_create_assignments: on_create_assignments.clone(),
                on_match_assignments: on_match_assignments.clone(),
            }),
            LogicalOperator::Union(left, right, is_all) => Ok(LogicalOperator::Union(
                Box::new(self.push_down(*left, mask_counter)?),
                Box::new(self.push_down(*right, mask_counter)?),
                is_all,
            )),
            _ => Ok(plan),
        }
    }
}

impl Rule for SemiJoinPushDown {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let mut counter = 0;
        self.push_down(plan, &mut counter)
    }
}

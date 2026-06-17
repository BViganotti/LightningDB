//! #58: Factorization Rewriter — converts correlated subqueries with
//! Cartesian products into FactorizedJoin operators.
//!
//! A correlated subquery like:
//!   MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LIKES]->(m), (m)-[:TAGGED]->(t) }
//! produces a plan like: Filter(Scan(person), Exists(Join(Scan(likes), Scan(tagged), ...)))
//! which unnests the subquery into:
//!   SemiJoin(Scan(person), Join(Scan(likes), Scan(tagged), ...), p.id = __sub_p.0)
//!
//! The Factorization Rewriter detects when the right side of a semi-join contains
//! a Cartesian product (Join with constant true condition) and factorizes it:
//!   SemiJoin(Scan(person), FactorizedJoin(Scan(likes), Scan(tagged)), ...)
//!
//! This is a no-op until the physical planner supports FactorizedJoin.
//! Tracked in: DEEP_AUDIT_FULL_2024.md item #58.

use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;

pub struct FactorizationRewriter;

impl Default for FactorizationRewriter {
    fn default() -> Self {
        Self::new()
    }
}

impl FactorizationRewriter {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::SemiJoin(left, right, cond, is_anti) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;

                // Detect Cartesian product on the right side: Join with true literal condition
                if let LogicalOperator::Join(join_left, join_right, join_cond) = &right_rewritten {
                    if matches!(join_cond, BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))) {
                        // This is a Cartesian product that could be factorized.
                        // TODO: Wrap in FactorizedJoin when the physical operator exists.
                        // For now, leave as-is. The comment serves as a work site marker.
                        return Ok(LogicalOperator::SemiJoin(
                            Box::new(left_rewritten),
                            Box::new(right_rewritten),
                            cond,
                            is_anti,
                        ));
                    }
                }

                Ok(LogicalOperator::SemiJoin(
                    Box::new(left_rewritten),
                    Box::new(right_rewritten),
                    cond,
                    is_anti,
                ))
            }
            LogicalOperator::Join(left, right, cond) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;
                Ok(LogicalOperator::Join(
                    Box::new(left_rewritten),
                    Box::new(right_rewritten),
                    cond,
                ))
            }
            _ => {
                if let Some(child) = op.get_child() {
                    let mut op_with_child = op.clone();
                    op_with_child.set_child(self.rewrite(child.clone())?);
                    Ok(op_with_child)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

impl Rule for FactorizationRewriter {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

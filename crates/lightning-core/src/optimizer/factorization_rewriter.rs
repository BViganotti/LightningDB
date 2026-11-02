//! #58: Factorization Rewriter — converts correlated subqueries with
//! Cartesian products into FactorizedJoin operators.
//!
//! A correlated subquery like:
//!   MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LIKES]->(m), (m)-[:TAGGED]->(t) }
//! produces a plan like: Filter(Scan(person), Exists(Join(Scan(likes), Scan(tagged), ...)))
//! which unnests the subquery into:
//!   SemiJoin(Scan(person), Join(Scan(likes), Scan(tagged), ...), p.id = __sub_p.0)
//!
//! The factorization this rule originally targeted — a *spurious* Cartesian
//! product from a node re-referenced across comma-separated patterns (e.g.
//! `MATCH (p)-[:LIKES]->(m), (m)-[:TAGGED]->(t)`, which used to re-scan `m` and
//! cross-join it) — is now prevented upstream: the logical planner tracks
//! variables already bound by earlier match elements and joins on the existing
//! binding instead of re-scanning. See `LogicalPlanner` bound-variable tracking
//! and `explain_plan_test`.
//!
//! Genuinely disconnected patterns (`MATCH (a:A), (b:B)`) are a real cross
//! product and are left as-is: turning those into a "factorized" join would only
//! pay off if the consuming operators understood a factorized representation
//! (avoiding materialization), which is a separate, larger change. So this rule
//! is intentionally a no-op for now.

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
                if let LogicalOperator::Join(_join_left, _join_right, join_cond) = &right_rewritten
                {
                    if matches!(
                        join_cond,
                        BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
                    ) {
                        // A genuine cross product on the right of a semi-join.
                        // Left as-is (see the module docs).
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

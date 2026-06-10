use crate::optimizer::Rule;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::collections::HashSet;

pub struct SubqueryUnnesting;

impl Default for SubqueryUnnesting {
    fn default() -> Self {
        Self::new()
    }
}

impl SubqueryUnnesting {
    pub fn new() -> Self {
        Self
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Filter(child, expr) => {
                let rewritten_child = self.rewrite(*child)?;
                self.unnest_subquery(rewritten_child, expr)
            }
            LogicalOperator::Join(left, right, cond) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;

                match right_rewritten {
                    LogicalOperator::Subquery(sub_child) => {
                        let mut left_vars = HashSet::new();
                        left_rewritten.get_variables(&mut left_vars);

                        let processed_sub_child = self.rewrite(*sub_child)?;

                        Ok(LogicalOperator::Join(
                            Box::new(left_rewritten),
                            Box::new(LogicalOperator::Subquery(Box::new(processed_sub_child))),
                            cond,
                        ))
                    }
                    other => {
                        Ok(LogicalOperator::Join(
                            Box::new(left_rewritten),
                            Box::new(other),
                            cond,
                        ))
                    }
                }
            }
            LogicalOperator::SemiJoin(left, right, cond, is_anti) => {
                let left_rewritten = self.rewrite(*left)?;
                let right_rewritten = self.rewrite(*right)?;
                Ok(LogicalOperator::SemiJoin(
                    Box::new(left_rewritten),
                    Box::new(right_rewritten),
                    cond,
                    is_anti,
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

impl Rule for SubqueryUnnesting {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

impl SubqueryUnnesting {
    fn unnest_subquery(
        &self,
        child: LogicalOperator,
        expr: BoundExpression,
    ) -> Result<LogicalOperator> {
        match expr {
            BoundExpression::Exists(steps) => {
                if let Some((sub_match, sub_where)) = steps.first() {
                    self.create_semi_join(child, sub_match.clone(), sub_where.clone(), false)
                } else {
                    Ok(child)
                }
            }
            BoundExpression::Function(name, args, _) if name == "NOT" => {
                if let Some(BoundExpression::Exists(steps)) = args.first() {
                    if let Some((sub_match, sub_where)) = steps.first() {
                        self.create_semi_join(child, sub_match.clone(), sub_where.clone(), true)
                    } else {
                        Ok(child)
                    }
                } else {
                    Ok(LogicalOperator::Filter(
                        Box::new(child),
                        BoundExpression::Function(name, args, lightning_types::LogicalType::Any),
                    ))
                }
            }
            BoundExpression::Logical(l, op, r)
                if op == crate::parser::ast::LogicalOperator::And =>
            {
                // Recursively unnest AND
                let left_unnested = self.unnest_subquery(child, *l)?;
                self.unnest_subquery(left_unnested, *r)
            }
            _ => Ok(LogicalOperator::Filter(Box::new(child), expr)),
        }
    }

    fn create_semi_join(
        &self,
        child: LogicalOperator,
        sub_match: crate::planner::binder::BoundMatchClause,
        sub_where: Option<crate::planner::binder::BoundWhereClause>,
        is_anti: bool,
    ) -> Result<LogicalOperator> {
        let mut left_vars = HashSet::new();
        child.get_variables(&mut left_vars);

        // Correctly handle Statement instead of Query for unnesting
        let sub_plan = crate::planner::logical_plan::LogicalPlanner::plan(
            crate::planner::binder::BoundStatement::Query(Some(sub_match), sub_where, vec![]),
        )?;

        let mut right_vars = HashSet::new();
        sub_plan.get_variables(&mut right_vars);

        let common: Vec<_> = left_vars.intersection(&right_vars).collect();
        let cond = if let Some(var) = common.first() {
            BoundExpression::Comparison(
                Box::new(BoundExpression::PropertyLookup(
                    var.to_string(),
                    0,
                    lightning_types::LogicalType::Any,
                )),
                crate::parser::ast::ComparisonOperator::Equal,
                Box::new(BoundExpression::PropertyLookup(
                    var.to_string(),
                    0,
                    lightning_types::LogicalType::Any,
                )),
            )
        } else {
            BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
        };
        Ok(LogicalOperator::SemiJoin(
            Box::new(child),
            Box::new(sub_plan),
            cond,
            is_anti,
        ))
    }
}

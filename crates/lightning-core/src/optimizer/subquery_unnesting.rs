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
            // Handle NOT EXISTS — check both Function("NOT", ...) and Not(Exists(...))
            BoundExpression::Not(inner) => {
                if let BoundExpression::Exists(steps) = inner.as_ref() {
                    if let Some((sub_match, sub_where)) = steps.first() {
                        self.create_semi_join(child, sub_match.clone(), sub_where.clone(), true)
                    } else {
                        Ok(child)
                    }
                } else {
                    Ok(LogicalOperator::Filter(
                        Box::new(child),
                        BoundExpression::Not(inner),
                    ))
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
            BoundExpression::Logical(l, crate::parser::ast::LogicalOperator::And, r) => {
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
        mut sub_match: crate::planner::binder::BoundMatchClause,
        mut sub_where: Option<crate::planner::binder::BoundWhereClause>,
        is_anti: bool,
    ) -> Result<LogicalOperator> {
        let mut left_vars = HashSet::new();
        child.get_variables(&mut left_vars);

        // Rename correlated variables in the subquery to the __sub_ prefix so the
        // SemiJoin's equality condition's right-hand PropertyLookup("__sub_n", ...)
        // matches the actual variable names in the subquery plan.
        for element in &mut sub_match.elements {
            if let crate::planner::binder::BoundMatchElement::Node(ref table_name, ref var_name, ref props) = element {
                if left_vars.contains(var_name) {
                    let new_name = format!("__sub_{var_name}");
                    *element = crate::planner::binder::BoundMatchElement::Node(
                        table_name.clone(), new_name, props.clone(),
                    );
                }
            }
        }
        if let Some(ref mut where_clause) = sub_where {
            for var in &left_vars {
                where_clause.expression = rename_var_in_expr(&where_clause.expression, var, &format!("__sub_{var}"));
            }
        }

        // Correctly handle Statement instead of Query for unnesting
        let sub_plan = crate::planner::logical_plan::LogicalPlanner::plan(
            crate::planner::binder::BoundStatement::Query(Some(sub_match), sub_where, vec![]),
        )?;

        let mut right_vars = HashSet::new();
        sub_plan.get_variables(&mut right_vars);

        let common: Vec<_> = left_vars.intersection(&right_vars).collect();
        let cond = if common.is_empty() {
            BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
        } else {
            // Build equality conditions for ALL common correlated variables
            let mut conditions: Vec<BoundExpression> = Vec::new();
            for var in &common {
                conditions.push(BoundExpression::Comparison(
                    Box::new(BoundExpression::PropertyLookup(
                        var.to_string(),
                        0,
                        lightning_types::LogicalType::Any,
                    )),
                    crate::parser::ast::ComparisonOperator::Equal,
                    Box::new(BoundExpression::PropertyLookup(
                        format!("__sub_{var}"),
                        0,
                        lightning_types::LogicalType::Any,
                    )),
                ));
            }
            // Conjoin all conditions
            conditions.into_iter().reduce(|acc, cond| {
                BoundExpression::Logical(
                    Box::new(acc),
                    crate::parser::ast::LogicalOperator::And,
                    Box::new(cond),
                )
            }).unwrap_or(BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true)))
        };
        Ok(LogicalOperator::SemiJoin(
            Box::new(child),
            Box::new(sub_plan),
            cond,
            is_anti,
        ))
    }
}

fn rename_var_in_expr(expr: &BoundExpression, old_name: &str, new_name: &str) -> BoundExpression {
    match expr {
        BoundExpression::Variable(name, typ) if name == old_name => {
            BoundExpression::Variable(new_name.to_string(), typ.clone())
        }
        BoundExpression::PropertyLookup(name, idx, typ) if name == old_name => {
            BoundExpression::PropertyLookup(new_name.to_string(), *idx, typ.clone())
        }
        BoundExpression::Literal(lit) => BoundExpression::Literal(lit.clone()),
        BoundExpression::Variable(name, typ) => BoundExpression::Variable(name.clone(), typ.clone()),
        BoundExpression::PropertyLookup(name, idx, typ) => {
            BoundExpression::PropertyLookup(name.clone(), *idx, typ.clone())
        }
        BoundExpression::Comparison(left, op, right) => BoundExpression::Comparison(
            Box::new(rename_var_in_expr(left, old_name, new_name)),
            *op,
            Box::new(rename_var_in_expr(right, old_name, new_name)),
        ),
        BoundExpression::Arithmetic(left, op, right) => BoundExpression::Arithmetic(
            Box::new(rename_var_in_expr(left, old_name, new_name)),
            *op,
            Box::new(rename_var_in_expr(right, old_name, new_name)),
        ),
        BoundExpression::Logical(left, op, right) => BoundExpression::Logical(
            Box::new(rename_var_in_expr(left, old_name, new_name)),
            *op,
            Box::new(rename_var_in_expr(right, old_name, new_name)),
        ),
        BoundExpression::Function(name, args, typ) => BoundExpression::Function(
            name.clone(),
            args.iter().map(|a| rename_var_in_expr(a, old_name, new_name)).collect(),
            typ.clone(),
        ),
        BoundExpression::Not(inner) => {
            BoundExpression::Not(Box::new(rename_var_in_expr(inner, old_name, new_name)))
        }
        BoundExpression::Exists(steps) => BoundExpression::Exists(steps.clone()),
        BoundExpression::CountSubquery(steps) => BoundExpression::CountSubquery(steps.clone()),
        BoundExpression::Aggregate(name, args, typ) => BoundExpression::Aggregate(
            name.clone(),
            args.iter().map(|a| rename_var_in_expr(a, old_name, new_name)).collect(),
            typ.clone(),
        ),
        BoundExpression::Case { expression, when_then, else_expression, return_type } => {
            BoundExpression::Case {
                expression: expression.as_ref().map(|e| Box::new(rename_var_in_expr(e, old_name, new_name))),
                when_then: when_then.iter().map(|(w, t)| {
                    (rename_var_in_expr(w, old_name, new_name), rename_var_in_expr(t, old_name, new_name))
                }).collect(),
                else_expression: else_expression.as_ref().map(|e| Box::new(rename_var_in_expr(e, old_name, new_name))),
                return_type: return_type.clone(),
            }
        }
        BoundExpression::List(items, typ) => BoundExpression::List(
            items.iter().map(|i| rename_var_in_expr(i, old_name, new_name)).collect(),
            typ.clone(),
        ),
        BoundExpression::Map(entries, typ) => BoundExpression::Map(
            entries.iter().map(|(k, v)| (k.clone(), rename_var_in_expr(v, old_name, new_name))).collect(),
            typ.clone(),
        ),
        BoundExpression::Lambda(params, body) => {
            BoundExpression::Lambda(params.clone(), Box::new(rename_var_in_expr(body, old_name, new_name)))
        }
        _ => expr.clone(),
    }
}

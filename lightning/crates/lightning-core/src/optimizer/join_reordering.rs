use crate::catalog::Catalog;
use crate::optimizer::cardinality_estimator::CardinalityEstimator;
use crate::optimizer::Rule;
use crate::parser::ast::LogicalOperator as AstLogicalOperator;
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct JoinReordering {
    estimator: CardinalityEstimator,
}

impl JoinReordering {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            estimator: CardinalityEstimator::new(catalog),
        }
    }

    fn extract_join_clique(
        &self,
        op: LogicalOperator,
        relations: &mut Vec<LogicalOperator>,
        conditions: &mut Vec<BoundExpression>,
    ) {
        match op {
            LogicalOperator::Join(left, right, cond) => {
                self.extract_join_clique(*left, relations, conditions);
                self.extract_join_clique(*right, relations, conditions);
                if !self.is_true_literal(&cond) {
                    conditions.push(cond);
                }
            }
            _ => {
                relations.push(op);
            }
        }
    }

    fn is_true_literal(&self, expr: &BoundExpression) -> bool {
        matches!(
            expr,
            BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
        )
    }

    fn get_vars(&self, expr: &BoundExpression, vars: &mut HashSet<String>) {
        match expr {
            BoundExpression::PropertyLookup(var, _, _) => {
                vars.insert(var.clone());
            }
            BoundExpression::Logical(left, _, right)
            | BoundExpression::Comparison(left, _, right)
            | BoundExpression::Arithmetic(left, _, right) => {
                self.get_vars(left, vars);
                self.get_vars(right, vars);
            }
            BoundExpression::Function(_, args, _) => {
                for arg in args {
                    self.get_vars(arg, vars);
                }
            }
            BoundExpression::Variable(v, _) => {
                vars.insert(v.clone());
            }
            _ => {}
        }
    }

    fn get_plan_vars(&self, op: &LogicalOperator, vars: &mut HashSet<String>) {
        match op {
            LogicalOperator::Scan(_, var, _, _, _)
            | LogicalOperator::IndexScan(_, var, _, _, _) => {
                vars.insert(var.clone());
            }
            LogicalOperator::Filter(child, cond) => {
                self.get_plan_vars(child, vars);
                self.get_vars(cond, vars);
            }
            LogicalOperator::Projection(child, items) => {
                self.get_plan_vars(child, vars);
                for item in items {
                    self.get_vars(&item.expression, vars);
                }
            }
            LogicalOperator::Join(left, right, cond) => {
                self.get_plan_vars(left, vars);
                self.get_plan_vars(right, vars);
                self.get_vars(cond, vars);
            }
            LogicalOperator::Aggregate { child, .. }
            | LogicalOperator::Sort(child, _)
            | LogicalOperator::Limit(child, _)
            | LogicalOperator::Skip(child, _)
            | LogicalOperator::Flatten(child)
            | LogicalOperator::UnwindDedup(child, _) => {
                self.get_plan_vars(child, vars);
            }
            _ => {}
        }
    }

    fn solve_dp(
        &self,
        relations: Vec<LogicalOperator>,
        total_conditions: &[BoundExpression],
    ) -> Result<LogicalOperator> {
        let n = relations.len();
        if n == 1 {
            return Ok(relations.into_iter().next().ok_or_else(|| {
                crate::LightningError::Internal("Expected at least one relation in join reordering".into())
            })?);
        }

        let relation_vars: Vec<HashSet<String>> = relations
            .iter()
            .map(|rel| {
                let mut v = HashSet::new();
                self.get_plan_vars(rel, &mut v);
                v
            })
            .collect();

        // DP Table: mask -> (Plan, Cardinality, Cost, UsedCondIndices)
        let mut dp: HashMap<u32, (LogicalOperator, u64, u64, HashSet<usize>)> = HashMap::new();

        for (i, rel) in relations.into_iter().enumerate() {
            let card = self.estimator.estimate(&rel);
            dp.insert(1 << i, (rel, card, 0, HashSet::new()));
        }

        for size in 2..=n {
            for subset_mask in 1..(1u32 << n) {
                if subset_mask.count_ones() as usize != size {
                    continue;
                }

                let mut best_for_subset: Option<(LogicalOperator, u64, u64, HashSet<usize>)> = None;

                let mut subset_vars = HashSet::new();
                for i in 0..n {
                    if (subset_mask & (1 << i)) != 0 {
                        subset_vars.extend(relation_vars[i].iter().cloned());
                    }
                }

                for left_mask in 1..subset_mask {
                    if (left_mask & subset_mask) != left_mask {
                        continue;
                    }
                    let right_mask = subset_mask ^ left_mask;
                    if left_mask > right_mask {
                        continue;
                    }

                    if let (
                        Some((lhs, l_card, l_cost, l_used)),
                        Some((rhs, r_card, r_cost, r_used)),
                    ) = (dp.get(&left_mask), dp.get(&right_mask))
                    {
                        let mut current_used = l_used.clone();
                        current_used.extend(r_used.iter().cloned());

                        let mut bridge_conds = Vec::new();
                        for (idx, cond) in total_conditions.iter().enumerate() {
                            if current_used.contains(&idx) {
                                continue;
                            }

                            let mut cond_vars = HashSet::new();
                            self.get_vars(cond, &mut cond_vars);

                            if cond_vars.iter().all(|v| subset_vars.contains(v)) {
                                bridge_conds.push(cond.clone());
                                current_used.insert(idx);
                            }
                        }

                        let join_cond = if bridge_conds.is_empty() {
                            BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true))
                        } else {
                            let mut it = bridge_conds.into_iter();
                            let mut res = it.next().ok_or_else(|| {
                                crate::LightningError::Internal("Expected at least one bridge condition in join clique".into())
                            })?;
                            for next in it {
                                res = BoundExpression::Logical(
                                    Box::new(res),
                                    AstLogicalOperator::And,
                                    Box::new(next),
                                );
                            }
                            res
                        };

                        let (mut left, mut right) = (lhs.clone(), rhs.clone());
                        let (mut lc, mut rc) = (*l_card, *r_card);
                        if lc < rc {
                            std::mem::swap(&mut left, &mut right);
                            std::mem::swap(&mut lc, &mut rc);
                        }

                        let plan =
                            LogicalOperator::Join(Box::new(left), Box::new(right), join_cond);
                        let card = self.estimator.estimate(&plan);
                        let cost = card + l_cost + r_cost;

                        if best_for_subset.as_ref().map_or(true, |best| cost < best.2) {
                            best_for_subset = Some((plan, card, cost, current_used));
                        }
                    }
                }

                if let Some(best) = best_for_subset {
                    dp.insert(subset_mask, best);
                }
            }
        }

        let full_mask = (1u32 << n) - 1;
        let mut dp = dp;
        let res = dp
            .remove(&full_mask)
            .map(|(plan, _, _, _)| plan)
            .ok_or_else(|| crate::LightningError::Internal("Join reordering failed".into()))?;
        Ok(res)
    }
}

impl Rule for JoinReordering {
    fn apply(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Join(..) => {
                let mut relations = Vec::new();
                let mut conditions = Vec::new();
                self.extract_join_clique(op, &mut relations, &mut conditions);

                let mut optimized_relations = Vec::new();
                for rel in relations {
                    optimized_relations.push(self.apply(rel)?);
                }

                if optimized_relations.len() <= 1 {
                    return Ok(optimized_relations.remove(0));
                }

                self.solve_dp(optimized_relations, &conditions)
            }
            LogicalOperator::Filter(child, cond) => {
                Ok(LogicalOperator::Filter(Box::new(self.apply(*child)?), cond))
            }
            LogicalOperator::Projection(child, items) => Ok(LogicalOperator::Projection(
                Box::new(self.apply(*child)?),
                items,
            )),
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            } => Ok(LogicalOperator::Aggregate {
                child: Box::new(self.apply(*child)?),
                group_by_cols,
                dependent_group_by_cols,
                aggregates,
            }),
            LogicalOperator::Sort(child, items) => {
                Ok(LogicalOperator::Sort(Box::new(self.apply(*child)?), items))
            }
            _ => Ok(op),
        }
    }
}

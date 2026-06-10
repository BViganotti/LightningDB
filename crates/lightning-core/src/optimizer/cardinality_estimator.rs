use crate::catalog::Catalog;
use crate::parser::ast::{ComparisonOperator, LogicalOperator as AstLogicalOperator};
use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CardinalityEstimator {
    catalog: Arc<RwLock<Catalog>>,
}

impl CardinalityEstimator {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    pub fn estimate(&self, op: &LogicalOperator) -> u64 {
        match op {
            LogicalOperator::Scan(table_name, _, _, _, _) => {
                let catalog = self.catalog.read();
                if let Some(table) = catalog.get_node_table(table_name) {
                    table.stats.cardinality
                } else if let Some(rel) = catalog.get_rel_table(table_name) {
                    rel.stats.cardinality
                } else {
                    1000 // Fallback
                }
            }
            LogicalOperator::IndexScan(table_name, _, _, _, _) => {
                let catalog = self.catalog.read();
                if let Some(_table) = catalog.get_node_table(table_name) {
                    1
                } else {
                    1000
                }
            }
            LogicalOperator::Filter(child, cond) => {
                let child_card = self.estimate(child);
                let selectivity = self.estimate_selectivity(cond, child);
                (child_card as f64 * selectivity) as u64
            }
            LogicalOperator::Join(left, right, cond) => {
                let left_card = self.estimate(left);
                let right_card = self.estimate(right);

                if let BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true)) = cond {
                    left_card.saturating_mul(right_card)
                } else {
                    std::cmp::max(left_card, right_card)
                }
            }
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                ..
            } => {
                let child_card = self.estimate(child);
                if group_by_cols.is_empty() {
                    1
                } else {
                    (child_card as f64).sqrt() as u64
                }
            }
            LogicalOperator::Limit(_, limit) => *limit,
            LogicalOperator::Skip(child, skip) => self.estimate(child).saturating_sub(*skip),
            LogicalOperator::Union(left, right, _) => self.estimate(left) + self.estimate(right),
            _ => 1000,
        }
    }

    fn estimate_selectivity(&self, expr: &BoundExpression, child: &LogicalOperator) -> f64 {
        match expr {
            BoundExpression::Comparison(left, op, right) => match (left.as_ref(), right.as_ref()) {
                (
                    BoundExpression::PropertyLookup(var, prop_idx, _),
                    BoundExpression::Literal(_),
                ) => self.get_property_selectivity(child, var, *prop_idx, op),
                _ => 0.1,
            },
            BoundExpression::Logical(left, op, right) => match op {
                AstLogicalOperator::And => {
                    self.estimate_selectivity(left, child) * self.estimate_selectivity(right, child)
                }
                AstLogicalOperator::Or => {
                    let s1 = self.estimate_selectivity(left, child);
                    let s2 = self.estimate_selectivity(right, child);
                    s1 + s2 - (s1 * s2)
                }
                AstLogicalOperator::Not => {
                    0.5 // Fallback selectivity for Not
                }
                AstLogicalOperator::Xor => {
                    let s1 = self.estimate_selectivity(left, child);
                    let s2 = self.estimate_selectivity(right, child);
                    s1 + s2 - 2.0 * s1 * s2
                }
            },
            BoundExpression::Not(expr) => 1.0 - self.estimate_selectivity(expr, child),
            BoundExpression::Parameter(_) => 0.1, // Fixed selectivity for parameters for now
            BoundExpression::Lambda(_, _) => 0.1,
            _ => 0.1,
        }
    }

    fn get_property_selectivity(
        &self,
        child: &LogicalOperator,
        _var: &str,
        prop_idx: usize,
        op: &ComparisonOperator,
    ) -> f64 {
        if let Some(table_name) = self.find_table_for_var(child, _var) {
            let catalog = self.catalog.read();
            let stats_opt = if let Some(table) = catalog.get_node_table(&table_name) {
                table.stats.column_stats.get(prop_idx)
            } else if let Some(rel) = catalog.get_rel_table(&table_name) {
                rel.stats.column_stats.get(prop_idx)
            } else {
                None
            };

            if let Some(stats) = stats_opt {
                match op {
                    ComparisonOperator::Equal => {
                        if stats.distinct_count > 0 {
                            1.0 / stats.distinct_count as f64
                        } else {
                            0.1
                        }
                    }
                    ComparisonOperator::NotEqual => {
                        if stats.distinct_count > 0 {
                            1.0 - (1.0 / stats.distinct_count as f64)
                        } else {
                            0.9
                        }
                    }
                    ComparisonOperator::GreaterThan
                    | ComparisonOperator::LessThan
                    | ComparisonOperator::GreaterThanOrEqual
                    | ComparisonOperator::LessThanOrEqual => 0.33,
                }
            } else {
                0.1
            }
        } else {
            0.1
        }
    }

    fn find_table_for_var(&self, child: &LogicalOperator, var: &str) -> Option<String> {
        match child {
            LogicalOperator::Scan(table_name, name, _, _, _) => {
                if name == var {
                    Some(table_name.clone())
                } else {
                    None
                }
            }
            LogicalOperator::IndexScan(table_name, name, _, _, _) => {
                if name == var {
                    Some(table_name.clone())
                } else {
                    None
                }
            }
            LogicalOperator::CountRelTable {
                rel_table, alias, ..
            } => {
                if alias == var {
                    Some(rel_table.clone())
                } else {
                    None
                }
            }
            LogicalOperator::Filter(child, _)
            | LogicalOperator::Projection(child, _)
            | LogicalOperator::SemiMasker(child, _, _)
            | LogicalOperator::Unwind(child, _, _)
            | LogicalOperator::Aggregate { child, .. }
            | LogicalOperator::Flatten(child)
            | LogicalOperator::UnwindDedup(child, _)
            | LogicalOperator::Sort(child, _)
            | LogicalOperator::Limit(child, _)
            | LogicalOperator::Skip(child, _)
            | LogicalOperator::Accumulate(child)
            | LogicalOperator::Distinct(child, _) => self.find_table_for_var(child, var),
            LogicalOperator::Join(left, right, _) => self
                .find_table_for_var(left, var)
                .or_else(|| self.find_table_for_var(right, var)),
            _ => None,
        }
    }
}

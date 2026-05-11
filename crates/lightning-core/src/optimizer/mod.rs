use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use std::sync::Arc;

pub mod acc_hash_join_optimizer;
pub mod agg_key_dependency_optimizer;
pub mod cardinality_estimator;
pub mod count_rel_table_optimizer;
pub mod factorization_rewriter;
pub mod filter_pushdown;
pub mod foreign_join_pushdown;
pub mod index_pushdown;
pub mod join_reordering;
pub mod limit_pushdown;
pub mod order_by_pushdown;
pub mod projection_pushdown;
pub mod semijoin_pushdown;
pub mod subquery_unnesting;
pub mod topk_optimizer;

pub trait Rule {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator>;
}

pub struct Optimizer {
    rules: Vec<Box<dyn Rule>>,
}

impl Optimizer {
    pub fn new(_catalog: std::sync::Arc<parking_lot::RwLock<crate::catalog::Catalog>>) -> Self {
        Self {
            rules: vec![Box::new(filter_pushdown::FilterPushDown::new())],
        }
    }

    pub fn optimize(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        for rule in &self.rules {
            plan = rule.apply(plan)?;
        }
        Ok(plan)
    }
}

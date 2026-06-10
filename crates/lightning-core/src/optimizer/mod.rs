use crate::catalog::Catalog;
use crate::planner::logical_plan::LogicalOperator;
use crate::Result;
use parking_lot::RwLock;
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
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        let cat1 = Arc::clone(&catalog);
        let cat2 = Arc::clone(&catalog);
        Self {
            rules: vec![
                Box::new(subquery_unnesting::SubqueryUnnesting::new()),
                Box::new(filter_pushdown::FilterPushDown::new()),
                // Box::new(index_pushdown::IndexPushDown::new(cat2)),
                Box::new(join_reordering::JoinReordering::new(cat1)),
                Box::new(topk_optimizer::TopKOptimizer::new()),
                Box::new(limit_pushdown::LimitPushDown::new()),
                Box::new(order_by_pushdown::OrderByPushDown::new()),
                // NOTE: projection_pushdown disabled — needs cross-operator
                //   expression index remapping in all expression-bearing ops.
                // NOTE: semijoin_pushdown + acc_hash_join_optimizer disabled —
                //   physical planner mask lifecycle issues with rel table scans.
                // NOTE: agg_key_dependency_optimizer disabled — incorrect group-by
                //   dependency analysis in edge cases.
                // NOTE: count_rel_table_optimizer disabled — wrong COUNT results
                //   for single-relationship tables.
            ],
        }
    }

    pub fn optimize(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        let max_iters = 5;
        for _iter in 0..max_iters {
            let before = plan.node_count();
            for rule in &self.rules {
                plan = rule.apply(plan)?;
            }
            if plan.node_count() == before {
                break;
            }
        }
        Ok(plan)
    }
}

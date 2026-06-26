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
    #[allow(clippy::new_ret_no_self)]
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self::with_binder_offsets(catalog, std::collections::HashMap::new())
    }
    pub fn with_binder_offsets(
        catalog: Arc<RwLock<Catalog>>,
        binder_column_offsets: std::collections::HashMap<String, usize>,
    ) -> Self {
        let _cat_jr = Arc::clone(&catalog);
        let cat_ipd = Arc::clone(&catalog);
        let cat_crt = Arc::clone(&catalog);
        let bco = binder_column_offsets;
            Self {
                rules: vec![
                    Box::new(subquery_unnesting::SubqueryUnnesting::new()),
                    Box::new(filter_pushdown::FilterPushDown::new()),
                    Box::new(index_pushdown::IndexPushDown::new(cat_ipd)),
                    // JoinReordering disabled: it can reorder joins in ways that
                    // break the physical planner's variable position computation.
                    // Box::new(join_reordering::JoinReordering::new(cat_jr)),
                    Box::new(topk_optimizer::TopKOptimizer::new()),
                    Box::new(limit_pushdown::LimitPushDown::new()),
                    Box::new(order_by_pushdown::OrderByPushDown::new()),
                    Box::new(projection_pushdown::ProjectionPushDown::new(bco)),
                    Box::new(agg_key_dependency_optimizer::AggKeyDependencyOptimizer::new()),
                    Box::new(count_rel_table_optimizer::CountRelTableOptimizer::new(cat_crt)),
                    //
                    // SemiJoinPushDown and AccHashJoinOptimizer disabled because they emit
                    // SemiMasker/Accumulate nodes that create masks in a way that can break
                    // existing query plans (masks reference variables not yet bound at the
                    // point of the Scan). Future work: fix mask variable resolution so these
                    // optimizers can apply correctly.
                    // Box::new(semijoin_pushdown::SemiJoinPushDown::new()),
                    // Box::new(acc_hash_join_optimizer::AccHashJoinOptimizer::new()),
                    Box::new(factorization_rewriter::FactorizationRewriter::new()),
                    Box::new(foreign_join_pushdown::ForeignJoinPushDown::new()),
                ],
            }
    }

    pub fn optimize(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        let max_iters = 5;
        for _iter in 0..max_iters {
            let before_count = plan.node_count();
            for rule in &self.rules {
                plan = rule.apply(plan)?;
            }
            // Fixed-point: stop when no rule changed the plan
            if plan.node_count() == before_count {
                break;
            }
        }
        Ok(plan)
    }
}

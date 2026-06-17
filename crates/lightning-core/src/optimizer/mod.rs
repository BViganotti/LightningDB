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
                Box::new(index_pushdown::IndexPushDown::new(cat2)),
                Box::new(join_reordering::JoinReordering::new(cat1)),
                Box::new(topk_optimizer::TopKOptimizer::new()),
                Box::new(limit_pushdown::LimitPushDown::new()),
                Box::new(order_by_pushdown::OrderByPushDown::new()),
                // #59: ProjectionPushDown — pushes column projections closer to
                //   Scan/IndexScan to reduce column count early. Includes full
                //   expression index remapping for all operator types.
                //   DEEP_AUDIT_FULL_2024.md item #59.
                Box::new(projection_pushdown::ProjectionPushDown::new()),
                // #59: ProjectionPushDown — pushes column projections closer to
                //   Scan/IndexScan to reduce column count early. Includes full
                //   expression index remapping for all operator types.
                //   DEEP_AUDIT_FULL_2024.md item #59.
                //
                // #59: SemiJoinPushDown disabled — physical planner mask
                //   lifecycle issues: SemiMasker adds mask columns to the
                //   probe side but downstream operators (Projection, Sort)
                //   don't account for the extra mask column, producing
                //   wrong column offsets. Rel table scans also lose the
                //   mask during property resolution.
                //   DEEP_AUDIT_FULL_2024.md item #59.
                //
                // #59: AccHashJoinOptimizer disabled — same mask lifecycle
                //   issue as SemiJoinPushDown. Accumulator hash join
                //   introduces a build-side mask that isn't propagated
                //   through subsequent operators correctly.
                //   DEEP_AUDIT_FULL_2024.md item #59.
                //
                // #59: AggKeyDependencyOptimizer disabled — incorrect
                //   group-by dependency analysis when an aggregate key
                //   transitively depends on another through a join column.
                //   Produces wrong GROUP BY keys, causing wrong aggregation
                //   results. DEEP_AUDIT_FULL_2024.md item #59.
                //
                // #59: CountRelTableOptimizer disabled — produces wrong
                //   COUNT results for single-relationship tables because
                //   it replaces a full scan+count with a table-level row
                //   count that doesn't account for relationship filtering.
                //   DEEP_AUDIT_FULL_2024.md item #59.
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

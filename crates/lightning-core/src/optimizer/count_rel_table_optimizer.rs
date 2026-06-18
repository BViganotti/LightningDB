use crate::catalog::Catalog;
use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::processor::aggregate::AggregateFunction;
use crate::Result;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct CountRelTableOptimizer {
    catalog: Arc<RwLock<Catalog>>,
}

impl CountRelTableOptimizer {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    fn is_rel_table(&self, table_name: &str) -> bool {
        let cat = self.catalog.read();
        cat.get_rel_table(table_name).is_some()
    }

    fn rewrite(&self, op: LogicalOperator) -> Result<LogicalOperator> {
        match op {
            LogicalOperator::Aggregate {
                child,
                group_by_cols,
                aggregates,
                ..
            } => {
                let pushed_child = self.rewrite(*child)?;

                if group_by_cols.is_empty() && aggregates.len() == 1 {
                    let (func, _) = &aggregates[0];
                    if *func == AggregateFunction::Count {
                        match &pushed_child {
                            LogicalOperator::Scan(_rel_table, _rel_alias, _, _, Some(_)) => {
                                // Scan has a filter — cannot use pre-computed total count
                                // since filtered rows must be excluded.
                            }
                            LogicalOperator::Scan(rel_table, rel_alias, _, _, None) => {
                                if self.is_rel_table(rel_table) {
                                    return Ok(LogicalOperator::CountRelTable {
                                        rel_table: rel_table.clone(),
                                        bound_table: String::new(),
                                        direction: crate::parser::ast::Direction::Right,
                                        alias: rel_alias.clone(),
                                    });
                                }
                            }
                            LogicalOperator::Join(left, right, _) => {
                                if let LogicalOperator::Join(inner_left, inner_right, _) =
                                    left.as_ref()
                                {
                                    if let (
                                        LogicalOperator::Scan(a_table, _, _, _, _),
                                        LogicalOperator::Scan(r_table, r_alias, _, _, _),
                                        LogicalOperator::Scan(_b_table, _, _, _, _),
                                    ) =
                                        (inner_left.as_ref(), inner_right.as_ref(), right.as_ref())
                                    {
                                        if self.is_rel_table(r_table) {
                                            return Ok(LogicalOperator::CountRelTable {
                                                rel_table: r_table.clone(),
                                                bound_table: a_table.clone(),
                                                direction: crate::parser::ast::Direction::Right,
                                                alias: r_alias.clone(),
                                            });
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }

                Ok(LogicalOperator::Aggregate {
                    child: Box::new(pushed_child),
                    group_by_cols,
                    dependent_group_by_cols: Vec::new(),
                    aggregates,
                })
            }
            _ => {
                if let Some(child) = op.clone().get_child().cloned() {
                    let new_child = self.rewrite(child)?;
                    let mut new_op = op.clone();
                    new_op.set_child(new_child);
                    Ok(new_op)
                } else {
                    Ok(op)
                }
            }
        }
    }
}

impl Rule for CountRelTableOptimizer {
    fn apply(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        self.rewrite(plan)
    }
}

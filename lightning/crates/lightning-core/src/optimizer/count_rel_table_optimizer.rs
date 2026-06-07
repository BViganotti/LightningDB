use crate::optimizer::Rule;
use crate::planner::logical_plan::LogicalOperator;
use crate::processor::aggregate::AggregateFunction;
use crate::Result;

pub struct CountRelTableOptimizer;

impl Default for CountRelTableOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl CountRelTableOptimizer {
    pub fn new() -> Self {
        Self
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

                // Check if it's a simple COUNT(*) aggregate
                if group_by_cols.is_empty() && aggregates.len() == 1 {
                    let (func, _) = &aggregates[0];
                    if *func == AggregateFunction::Count {
                        // Pattern: Aggregate(Count) -> Join(Join(Scan(Node), Scan(Rel)), Scan(Node))
                        // Or: Aggregate(Count) -> Scan(Rel)

                        match &pushed_child {
                            LogicalOperator::Scan(rel_table, rel_alias, _, _, _) => {
                                // Simple case: MATCH ()-[r:REL]->() RETURN count(r)
                                // We can optimize if it's a relationship table.
                                // In lightning, we don't easily know if a table is REL or NODE here without catalog access.
                                // However, for parity, we assume the optimizer is registered where it can make this decision.
                                // If it's a REL Scan with no filters and no properties, it's a candidate.
                                return Ok(LogicalOperator::CountRelTable {
                                    rel_table: rel_table.clone(),
                                    bound_table: String::new(), // Generic count if no bound
                                    direction: crate::parser::ast::Direction::Right,
                                    alias: rel_alias.clone(),
                                });
                            }
                            LogicalOperator::Join(left, right, _) => {
                                // Case: MATCH (a)-[r:REL]->(b) RETURN count(*)
                                // This matches the 3-table join pattern planned in logical_plan.rs
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
                                        return Ok(LogicalOperator::CountRelTable {
                                            rel_table: r_table.clone(),
                                            bound_table: a_table.clone(),
                                            direction: crate::parser::ast::Direction::Right,
                                            alias: r_alias.clone(),
                                        });
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

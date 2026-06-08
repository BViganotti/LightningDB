use crate::parser::ast::Literal;
use crate::planner::binder::{
    BoundCallClause as BoundCall, BoundClause, BoundExpression, BoundNodePattern, BoundOrderByItem,
    BoundProjectionItem, BoundPropertyAssignment, BoundQuery, BoundRelPattern, BoundStatement,
    BoundTransactionAction, BoundUnionQuery,
};

use crate::processor::aggregate::AggregateFunction;
use crate::LightningError;
use crate::Result;
use lightning_types::LogicalType;

#[derive(Debug, Clone)]
pub enum LogicalOperator {
    Scan(
        String,
        String,
        Option<(String, Option<usize>)>,
        Option<Vec<usize>>,
        Option<BoundExpression>, // pushed down filter
    ), // table_name, variable, (mask_id, mask_column_idx), projected_idxs, filter
    IndexScan(String, String, String, BoundExpression, Option<Vec<usize>>), // table_name, variable, pk_name, pk_value, projected_idxs
    SingleRow, // Dummy scan for queries without MATCH
    Filter(Box<LogicalOperator>, BoundExpression),
    Projection(Box<LogicalOperator>, Vec<BoundProjectionItem>),
    SemiMasker(Box<LogicalOperator>, String, String), // child, variable, mask_id
    Unwind(Box<LogicalOperator>, BoundExpression, String), // child, list_expression, alias
    Join(Box<LogicalOperator>, Box<LogicalOperator>, BoundExpression), // left, right, join_cond
    Aggregate {
        child: Box<LogicalOperator>,
        group_by_cols: Vec<usize>,
        dependent_group_by_cols: Vec<usize>,
        aggregates: Vec<(AggregateFunction, usize)>,
    }, // child, group_by_cols, dependent_group_by_cols, aggregates
    CreateNode(Option<Box<LogicalOperator>>, BoundNodePattern),
    CreateRel(Option<Box<LogicalOperator>>, BoundRelPattern),
    Delete(Box<LogicalOperator>, Vec<(String, String)>, bool), // child, (var, table_name), detach
    Set(Box<LogicalOperator>, Vec<BoundPropertyAssignment>),
    Sort(Box<LogicalOperator>, Vec<BoundOrderByItem>),
    Limit(Box<LogicalOperator>, u64),
    TopK(Box<LogicalOperator>, Vec<BoundOrderByItem>, u64),
    Skip(Box<LogicalOperator>, u64),
    Call(BoundCall),
    Subquery(Box<LogicalOperator>),
    RecursiveJoin {
        child: Box<LogicalOperator>,
        rel_table: String,
        rel_var: String,
        src_var: String,
        dst_node_table: String,
        dst_var: String,
        bounds: Option<(Option<u32>, Option<u32>)>,
        mask_id: Option<String>,
    },
    Intersect {
        probe_child: Box<LogicalOperator>,
        build_children: Vec<LogicalOperator>,
        key_vars: Vec<String>,
        intersect_var: String,
    },
    AllShortestPaths {
        child: Box<LogicalOperator>,
        rel_table_name: String,
        src_var_name: String,
        dst_var_name: String,
        path_var_name: String,
        max_depth: u32,
    },
    Flatten(Box<LogicalOperator>),
    UnwindDedup(Box<LogicalOperator>, BoundExpression),
    Merge {
        child: Box<LogicalOperator>,
        pattern: BoundNodePattern,
        on_create_assignments: Vec<BoundPropertyAssignment>,
        on_match_assignments: Vec<BoundPropertyAssignment>,
    },
    Union(Box<LogicalOperator>, Box<LogicalOperator>, bool), // left, right, is_all
    OptionalMatch(Box<LogicalOperator>, Box<LogicalOperator>), // child, match_branch
    With(
        Box<LogicalOperator>,
        Vec<BoundProjectionItem>,
        Option<BoundExpression>,
    ), // child, items, where
    CreateTableNode {
        name: String,
        columns: Vec<crate::catalog::PropertyDefinition>,
        primary_key: String,
        if_not_exists: bool,
    },
    CreateTableRel {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<crate::catalog::PropertyDefinition>,
        if_not_exists: bool,
    },
    DropTable(String, bool),
    CopyFrom {
        table_name: String,
        file_path: String,
        options: std::collections::HashMap<String, Literal>,
    },
    CopyTo {
        table_name: String,
        file_path: String,
        options: std::collections::HashMap<String, Literal>,
    },
    Transaction(BoundTransactionAction),
    Checkpoint,
    Vacuum,
    AlterTable { name: String, operation: crate::parser::ast::AlterOperation },
    CreateConstraint {
        name: String,
        table_name: String,
        property: String,
    },
    DropConstraint(String),
    CreateIndex {
        name: String,
        table_name: String,
        property: String,
    },
    CreateVectorIndex {
        table_name: String,
        field: String,
        index_type: String,
        metric: String,
        dimension: usize,
    },
    CreateFtsIndex {
        table_name: String,
        fields: Vec<String>,
    },
    DropIndex(String),
    CountRelTable {
        rel_table: String,
        bound_table: String, // Source node table for counting
        direction: crate::parser::ast::Direction,
        alias: String,
    },
    Accumulate(Box<LogicalOperator>),
    Distinct(Box<LogicalOperator>, Vec<usize>), // child, columns to distinct on
    SemiJoin(
        Box<LogicalOperator>,
        Box<LogicalOperator>,
        BoundExpression,
        bool,
    ), // child, subquery, join_cond, is_anti
    Profile(Box<LogicalOperator>),
    Explain(Box<LogicalOperator>),
    CreateSequence {
        name: String,
        start_with: u64,
        increment_by: i64,
    },
    CreateMacro {
        name: String,
        params: Vec<String>,
        body: crate::parser::ast::Expression,
    },
}

impl LogicalOperator {
    pub fn get_child(&self) -> Option<&LogicalOperator> {
        match self {
            LogicalOperator::Filter(c, _)
            | LogicalOperator::Projection(c, _)
            | LogicalOperator::SemiMasker(c, _, _)
            | LogicalOperator::Unwind(c, _, _)
            | LogicalOperator::Aggregate { child: c, .. }
            | LogicalOperator::Delete(c, ..)
            | LogicalOperator::Set(c, _)
            | LogicalOperator::Sort(c, _)
            | LogicalOperator::Limit(c, _)
            | LogicalOperator::TopK(c, _, _)
            | LogicalOperator::Skip(c, _)
            | LogicalOperator::Subquery(c)
            | LogicalOperator::Flatten(c)
            | LogicalOperator::UnwindDedup(c, _)
            | LogicalOperator::Profile(c)
            | LogicalOperator::Explain(c)
            | LogicalOperator::Accumulate(c)
            | LogicalOperator::Distinct(c, _)
            | LogicalOperator::SemiJoin(c, ..) => Some(c),
            LogicalOperator::CountRelTable { .. }
            | LogicalOperator::CreateSequence { .. }
            | LogicalOperator::CreateMacro { .. } => None,
            LogicalOperator::Join(l, _, _) | LogicalOperator::Union(l, _, _) => Some(l), // Return left as primary child
            LogicalOperator::RecursiveJoin { child, .. } => Some(child),
            LogicalOperator::Merge { child, .. } => Some(child),
            LogicalOperator::OptionalMatch(c, _) => Some(c),
            LogicalOperator::With(c, _, _) => Some(c),
            LogicalOperator::CreateNode(c_opt, _) | LogicalOperator::CreateRel(c_opt, _) => {
                c_opt.as_ref().map(|b| b.as_ref())
            }
            _ => None,
        }
    }

    pub fn set_child(&mut self, new_child: LogicalOperator) {
        match self {
            LogicalOperator::Filter(c, _)
            | LogicalOperator::Projection(c, _)
            | LogicalOperator::SemiMasker(c, _, _)
            | LogicalOperator::Unwind(c, _, _)
            | LogicalOperator::Aggregate { child: c, .. }
            | LogicalOperator::Delete(c, ..)
            | LogicalOperator::Set(c, _)
            | LogicalOperator::Sort(c, _)
            | LogicalOperator::Limit(c, _)
            | LogicalOperator::TopK(c, _, _)
            | LogicalOperator::Skip(c, _)
            | LogicalOperator::Subquery(c)
            | LogicalOperator::Flatten(c)
            | LogicalOperator::UnwindDedup(c, _)
            | LogicalOperator::Explain(c)
            | LogicalOperator::Accumulate(c)
            | LogicalOperator::Distinct(c, _)
            | LogicalOperator::SemiJoin(c, ..) => *c = Box::new(new_child),
            LogicalOperator::Join(l, _, _) | LogicalOperator::Union(l, _, _) => {
                *l = Box::new(new_child)
            }
            LogicalOperator::RecursiveJoin { child, .. } => *child = Box::new(new_child),
            LogicalOperator::Merge { child, .. } => *child = Box::new(new_child),
            LogicalOperator::OptionalMatch(c, _) => *c = Box::new(new_child),
            LogicalOperator::With(c, _, _) => *c = Box::new(new_child),
            LogicalOperator::CreateNode(c_opt, _) | LogicalOperator::CreateRel(c_opt, _) => {
                *c_opt = Some(Box::new(new_child))
            }
            LogicalOperator::CountRelTable { .. }
            | LogicalOperator::CreateSequence { .. }
            | LogicalOperator::CreateMacro { .. } => {}
            _ => {}
        }
    }
}

impl LogicalOperator {
    pub fn get_unwind_alias(&self) -> Option<String> {
        match self {
            LogicalOperator::Unwind(_, _, alias) => Some(alias.clone()),
            _ => None,
        }
    }
    pub fn get_variables(&self, vars: &mut std::collections::HashSet<String>) {
        match self {
            LogicalOperator::Scan(_, var, ..) | LogicalOperator::IndexScan(_, var, ..) => {
                vars.insert(var.clone());
            }
            LogicalOperator::Projection(_, items) => {
                for item in items {
                    vars.insert(item.alias.clone());
                }
            }
            LogicalOperator::Aggregate { .. } => {
                // This is slightly more complex as it depends on Projection child
            }
            _ => {
                if let Some(child) = self.get_child() {
                    child.get_variables(vars);
                }
            }
        }
    }
}

pub struct LogicalPlanner;

impl LogicalPlanner {
    pub fn plan_query(query: BoundQuery) -> Result<LogicalOperator> {
        let mut plan = if query.union_queries.is_empty() {
            LogicalOperator::SingleRow
        } else {
            Self::plan_union_query(query.union_queries[0].clone())?
        };

        if query.is_profile {
            plan = LogicalOperator::Profile(Box::new(plan));
        } else if query.is_explain {
            plan = LogicalOperator::Explain(Box::new(plan));
        }

        Ok(plan)
    }

    pub fn plan_union_query(uq: BoundUnionQuery) -> Result<LogicalOperator> {
        let mut left = Self::plan(uq.statement)?;
        if let Some((next_uq, is_all)) = uq.next_union {
            let right = Self::plan_union_query(*next_uq)?;
            left = LogicalOperator::Union(Box::new(left), Box::new(right), is_all);
        }
        Ok(left)
    }

    pub fn plan(bound_statement: BoundStatement) -> Result<LogicalOperator> {
        match bound_statement {
            BoundStatement::CreateTableNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            } => Ok(LogicalOperator::CreateTableNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            }),
            BoundStatement::CreateTableRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            } => Ok(LogicalOperator::CreateTableRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            }),
            BoundStatement::DropTable(name, if_exists) => {
                Ok(LogicalOperator::DropTable(name, if_exists))
            }
            BoundStatement::CopyFrom {
                table_name,
                file_path,
                options,
            } => Ok(LogicalOperator::CopyFrom {
                table_name,
                file_path,
                options,
            }),
            BoundStatement::CopyTo {
                table_name,
                file_path,
                options,
            } => Ok(LogicalOperator::CopyTo {
                table_name,
                file_path,
                options,
            }),
            BoundStatement::Transaction(action) => Ok(LogicalOperator::Transaction(action)),
            BoundStatement::Checkpoint => Ok(LogicalOperator::Checkpoint),
            BoundStatement::Vacuum => Ok(LogicalOperator::Vacuum),
            BoundStatement::AlterTable { name, operation } => {
                Ok(LogicalOperator::AlterTable { name, operation })
            }
            BoundStatement::CreateConstraint {
                name,
                table_name,
                property,
            } => Ok(LogicalOperator::CreateConstraint {
                name,
                table_name,
                property,
            }),
            BoundStatement::DropConstraint(name) => {
                Ok(LogicalOperator::DropConstraint(name))
            }
            BoundStatement::CreateIndex {
                name,
                table_name,
                property,
            } => Ok(LogicalOperator::CreateIndex {
                name,
                table_name,
                property,
            }),
            BoundStatement::DropIndex(name) => {
                Ok(LogicalOperator::DropIndex(name))
            }
            BoundStatement::CreateVectorIndex {
                table_name,
                field,
                index_type,
                metric,
                dimension,
            } => Ok(LogicalOperator::CreateVectorIndex {
                table_name,
                field,
                index_type,
                metric,
                dimension,
            }),
            BoundStatement::CreateFtsIndex {
                table_name,
                fields,
            } => Ok(LogicalOperator::CreateFtsIndex {
                table_name,
                fields,
            }),
            BoundStatement::StandaloneCall(name, args) => Ok(LogicalOperator::Call(BoundCall {
                procedure_name: name,
                parameters: args
                    .into_iter()
                    .map(|a| BoundExpression::Literal(a))
                    .collect(),
                yield_items: None,
            })),
            BoundStatement::Create(pat) => Ok(LogicalOperator::CreateNode(None, pat)),
            BoundStatement::CreateRel(pat) => Ok(LogicalOperator::CreateRel(None, pat)),
            BoundStatement::CreateSequence {
                name,
                start_with,
                increment_by,
            } => Ok(LogicalOperator::CreateSequence {
                name,
                start_with,
                increment_by,
            }),
            BoundStatement::CreateMacro { name, params, body } => {
                Ok(LogicalOperator::CreateMacro { name, params, body })
            }
            BoundStatement::Query(bound_match, bound_where, clauses) => {
                let mut plan = if let Some(m) = bound_match {
                    let mut match_elements_iter = m.elements.into_iter();
                    let first = match_elements_iter
                        .next()
                        .ok_or_else(|| crate::LightningError::Query("Empty MATCH clause".into()))?;

                    let mut plan = match first {
                        crate::planner::binder::BoundMatchElement::Node(table, var, properties) => {
                            let mut op =
                                LogicalOperator::Scan(table, var.clone(), None, None, None);
                            for (idx, expr) in properties {
                                let condition = BoundExpression::Comparison(
                                    Box::new(BoundExpression::PropertyLookup(
                                        var.clone(),
                                        idx,
                                        expr.get_type(),
                                    )),
                                    crate::parser::ast::ComparisonOperator::Equal,
                                    Box::new(expr),
                                );
                                op = LogicalOperator::Filter(Box::new(op), condition);
                            }
                            op
                        }
                        crate::planner::binder::BoundMatchElement::AllShortestPaths { .. } => {
                            return Err(crate::LightningError::Query(
                                "MATCH must start with a node".into(),
                            ));
                        }
                        crate::planner::binder::BoundMatchElement::Rel(_, _, _, _, _) => {
                            return Err(crate::LightningError::Query(
                                "MATCH must start with a node".into(),
                            ));
                        }
                    };

                    while let Some(element) = match_elements_iter.next() {
                        match element {
                            crate::planner::binder::BoundMatchElement::AllShortestPaths {
                                rel_table_name,
                                src_var,
                                dst_var: _,
                                path_var,
                                max_depth,
                            } => {
                                plan = LogicalOperator::AllShortestPaths {
                                    child: Box::new(plan),
                                    rel_table_name,
                                    src_var_name: src_var,
                                    dst_var_name: path_var.clone(),
                                    path_var_name: path_var,
                                    max_depth,
                                };
                            }
                            crate::planner::binder::BoundMatchElement::Node(
                                table,
                                var,
                                properties,
                            ) => {
                                let mut node_op =
                                    LogicalOperator::Scan(table, var.clone(), None, None, None);
                                for (idx, expr) in properties {
                                    let condition = BoundExpression::Comparison(
                                        Box::new(BoundExpression::PropertyLookup(
                                            var.clone(),
                                            idx,
                                            expr.get_type(),
                                        )),
                                        crate::parser::ast::ComparisonOperator::Equal,
                                        Box::new(expr),
                                    );
                                    node_op = LogicalOperator::Filter(Box::new(node_op), condition);
                                }
                                plan = LogicalOperator::Join(
                                    Box::new(plan),
                                    Box::new(node_op),
                                    BoundExpression::Literal(crate::parser::ast::Literal::Boolean(
                                        true,
                                    )),
                                );
                            }
                            crate::planner::binder::BoundMatchElement::Rel(
                                rel_table,
                                rel_var,
                                src_var,
                                dst_var,
                                bounds,
                            ) => {
                                if let Some(b) = bounds {
                                    // Recursive join requires knowing destination table
                                    // For simplicity in this reconstruction, assume next is node
                                    let next = match_elements_iter.next().ok_or_else(|| {
                                        crate::LightningError::Query(
                                            "Rel must be followed by node".into(),
                                        )
                                    })?;
                                    if let crate::planner::binder::BoundMatchElement::Node(
                                        dst_table,
                                        d_var,
                                        properties,
                                    ) = next
                                    {
                                        plan = LogicalOperator::RecursiveJoin {
                                            child: Box::new(plan),
                                            rel_table,
                                            rel_var: rel_var.clone(),
                                            src_var,
                                            dst_node_table: dst_table.clone(),
                                            dst_var: dst_var.clone(),
                                            bounds: Some(b),
                                            mask_id: None,
                                        };
                                        for (idx, expr) in properties {
                                            let condition = BoundExpression::Comparison(
                                                Box::new(BoundExpression::PropertyLookup(
                                                    dst_var.clone(),
                                                    idx,
                                                    expr.get_type(),
                                                )),
                                                crate::parser::ast::ComparisonOperator::Equal,
                                                Box::new(expr),
                                            );
                                            plan =
                                                LogicalOperator::Filter(Box::new(plan), condition);
                                        }
                                    } else {
                                        return Err(crate::LightningError::Query(
                                            "Rel must be followed by node".into(),
                                        ));
                                    }
                                } else {
                                    let rel_scan = LogicalOperator::Scan(
                                        rel_table,
                                        rel_var.clone(),
                                        None,
                                        None,
                                        None,
                                    );
                                    let join_cond = BoundExpression::Comparison(
                                        Box::new(BoundExpression::PropertyLookup(
                                            src_var,
                                            0,
                                            LogicalType::Uint64,
                                        )),
                                        crate::parser::ast::ComparisonOperator::Equal,
                                        Box::new(BoundExpression::PropertyLookup(
                                            rel_var.clone(),
                                            0,
                                            LogicalType::Uint64,
                                        )),
                                    );
                                    plan = LogicalOperator::Join(
                                        Box::new(plan),
                                        Box::new(rel_scan),
                                        join_cond,
                                    );

                                    let next = match_elements_iter.next().ok_or_else(|| {
                                        crate::LightningError::Query(
                                            "Rel must be followed by node".into(),
                                        )
                                    })?;
                                    if let crate::planner::binder::BoundMatchElement::Node(
                                        dst_table,
                                        d_var,
                                        properties,
                                    ) = next
                                    {
                                        let mut dst_op = LogicalOperator::Scan(
                                            dst_table,
                                            d_var.clone(),
                                            None,
                                            None,
                                            None,
                                        );
                                        for (idx, expr) in properties {
                                            let condition = BoundExpression::Comparison(
                                                Box::new(BoundExpression::PropertyLookup(
                                                    d_var.clone(),
                                                    idx,
                                                    expr.get_type(),
                                                )),
                                                crate::parser::ast::ComparisonOperator::Equal,
                                                Box::new(expr),
                                            );
                                            dst_op = LogicalOperator::Filter(
                                                Box::new(dst_op),
                                                condition,
                                            );
                                        }
                                        let join_cond_dst = BoundExpression::Comparison(
                                            Box::new(BoundExpression::PropertyLookup(
                                                rel_var,
                                                1,
                                                LogicalType::Uint64,
                                            )),
                                            crate::parser::ast::ComparisonOperator::Equal,
                                            Box::new(BoundExpression::PropertyLookup(
                                                d_var,
                                                0,
                                                LogicalType::Uint64,
                                            )),
                                        );
                                        plan = LogicalOperator::Join(
                                            Box::new(plan),
                                            Box::new(dst_op),
                                            join_cond_dst,
                                        );
                                    } else {
                                        return Err(crate::LightningError::Query(
                                            "Rel must be followed by node".into(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    plan
                } else {
                    LogicalOperator::SingleRow
                };

                if let Some(ref where_expr) = bound_where {
                    plan = LogicalOperator::Filter(Box::new(plan), where_expr.expression.clone());
                }

                for clause in clauses {
                    plan = match clause {
                        BoundClause::Return(ret) => {
                            let mut aggregates = Vec::new();
                            let mut group_by_exprs = Vec::new();
                            let mut aggregate_arg_exprs = Vec::new();
                            let mut aggregates_found = false;

                            for item in &ret.items {
                                if item.expression.is_aggregate() {
                                    aggregates_found = true;
                                    if let BoundExpression::Function(name, args, _) =
                                        &item.expression
                                    {
                                        let func = match name.to_uppercase().as_str() {
                                            "COUNT" => AggregateFunction::Count,
                                            "COUNT_DISTINCT" => AggregateFunction::CountDistinct,
                                            "SUM" => AggregateFunction::Sum,
                                            "MIN" => AggregateFunction::Min,
                                            "MAX" => AggregateFunction::Max,
                                            "AVG" => AggregateFunction::Avg,
                                            "COLLECT" => AggregateFunction::Collect,
                                            "GROUP_CONCAT" => AggregateFunction::GroupConcat,
                                            "MEDIAN" => AggregateFunction::Median,
                                            "COLLECT_DISTINCT" => {
                                                AggregateFunction::CollectDistinct
                                            }
                                            "STDDEV_POP" | "STDDEV" => AggregateFunction::StdDevPop,
                                            "STDDEV_SAMP" => AggregateFunction::StdDevSamp,
                                            "VAR_POP" | "VAR" => AggregateFunction::VarPop,
                                            "VAR_SAMP" => AggregateFunction::VarSamp,
                                            _ => {
                                                return Err(LightningError::Query(format!(
                                                    "Unknown aggregate function: {}",
                                                    name
                                                )))
                                            }
                                        };

                                        let input_idx = if args.is_empty() {
                                            // COUNT(*) — always add a dummy non-null literal.
                                            // Without this, input_idx=0 would point to the first
                                            // GROUP BY column, causing COUNT to count non-null
                                            // GROUP BY values instead of all rows.
                                            aggregate_arg_exprs.push(BoundProjectionItem {
                                                expression: BoundExpression::Literal(
                                                    crate::parser::ast::Literal::Number(1.0),
                                                ),
                                                alias: "_dummy".to_string(),
                                            });
                                            group_by_exprs.len() + aggregate_arg_exprs.len() - 1
                                        } else {
                                            let arg_expr = args[0].clone();
                                            let idx =
                                                group_by_exprs.len() + aggregate_arg_exprs.len();
                                            aggregate_arg_exprs.push(BoundProjectionItem {
                                                expression: arg_expr,
                                                alias: "".to_string(),
                                            });
                                            idx
                                        };
                                        aggregates.push((func, input_idx));
                                    }
                                } else {
                                    group_by_exprs.push(item.clone());
                                }
                            }

                            let mut current_plan = plan;
                            if !aggregates.is_empty() {
                                let mut pre_agg_items = group_by_exprs.clone();
                                pre_agg_items.extend(aggregate_arg_exprs);
                                current_plan = LogicalOperator::Projection(
                                    Box::new(current_plan),
                                    pre_agg_items,
                                );

                                let group_by_indices: Vec<usize> =
                                    (0..group_by_exprs.len()).collect();
                                current_plan = LogicalOperator::Aggregate {
                                    child: Box::new(current_plan),
                                    group_by_cols: group_by_indices.clone(),
                                    dependent_group_by_cols: Vec::new(),
                                    aggregates: aggregates,
                                };

                                // After aggregate, we need a final projection to match the RETURN items
                                match &ret.order_by {
                                    Some(order_by) => {
                                        current_plan = LogicalOperator::Sort(
                                            Box::new(current_plan),
                                            order_by.clone(),
                                        );
                                    }
                                    None => {}
                                }

                                let mut final_items = Vec::new();
                                let mut gb_idx = 0;
                                let mut agg_idx = group_by_indices.len();

                                for item in &ret.items {
                                    if item.expression.is_aggregate() {
                                        final_items.push(BoundProjectionItem {
                                            expression: BoundExpression::PropertyLookup(
                                                "".into(),
                                                agg_idx,
                                                item.expression.get_type(),
                                            ),
                                            alias: item.alias.clone(),
                                        });
                                        agg_idx += 1;
                                    } else {
                                        final_items.push(BoundProjectionItem {
                                            expression: BoundExpression::PropertyLookup(
                                                "".into(),
                                                gb_idx,
                                                item.expression.get_type(),
                                            ),
                                            alias: item.alias.clone(),
                                        });
                                        gb_idx += 1;
                                    }
                                }
                                current_plan = LogicalOperator::Projection(
                                    Box::new(current_plan),
                                    final_items,
                                );
                            } else {
                                // Handle DISTINCT by using a hash aggregate with all items as group_by
                                if ret.distinct {
                                    let distinct_items = ret.items.clone();
                                    current_plan = LogicalOperator::Projection(
                                        Box::new(current_plan),
                                        distinct_items.clone(),
                                    );
                                    // Use aggregate with all columns as group_by and no aggregates
                                    // This effectively deduplicates
                                    let group_by_indices: Vec<usize> =
                                        (0..distinct_items.len()).collect();
                                    current_plan = LogicalOperator::Aggregate {
                                        child: Box::new(current_plan),
                                        group_by_cols: group_by_indices,
                                        dependent_group_by_cols: Vec::new(),
                                        aggregates: Vec::new(),
                                    };
                                    // After aggregate, we need to project back to original items
                                    let final_items: Vec<BoundProjectionItem> = distinct_items
                                        .iter()
                                        .enumerate()
                                        .map(|(i, item)| BoundProjectionItem {
                                            expression: BoundExpression::PropertyLookup(
                                                "".into(),
                                                i,
                                                item.expression.get_type(),
                                            ),
                                            alias: item.alias.clone(),
                                        })
                                        .collect();
                                    current_plan = LogicalOperator::Projection(
                                        Box::new(current_plan),
                                        final_items,
                                    );
                                }
                                // Sort
                                if let Some(order_by) = &ret.order_by {
                                    current_plan = LogicalOperator::Sort(
                                        Box::new(current_plan),
                                        order_by.clone(),
                                    );
                                }
                                // If not distinct, apply projection (distinct already projected above)
                                if !ret.distinct {
                                    current_plan = LogicalOperator::Projection(
                                        Box::new(current_plan),
                                        ret.items.clone(),
                                    );
                                }
                            }

                            if let Some(skip) = ret.skip {
                                current_plan =
                                    LogicalOperator::Skip(Box::new(current_plan), skip as u64);
                            }
                            if let Some(limit) = ret.limit {
                                current_plan =
                                    LogicalOperator::Limit(Box::new(current_plan), limit as u64);
                            }
                            current_plan
                        }
                        BoundClause::Unwind(unwind) => LogicalOperator::Unwind(
                            Box::new(plan),
                            unwind.expression.clone(),
                            unwind.alias.clone(),
                        ),
                        BoundClause::Delete(del) => {
                            LogicalOperator::Delete(Box::new(plan), del.variables.clone(), del.detach)
                        }
                        BoundClause::Set(set) => {
                            LogicalOperator::Set(Box::new(plan), set.assignments.clone())
                        }
                        BoundClause::Create(pat) => {
                            LogicalOperator::CreateNode(Some(Box::new(plan)), pat.clone())
                        }
                        BoundClause::CreateRel(pat) => {
                            LogicalOperator::CreateRel(Some(Box::new(plan)), pat.clone())
                        }
                        BoundClause::Call(call) => LogicalOperator::Call(call.clone()),
                        BoundClause::Unwind(unwind) => LogicalOperator::Unwind(
                            Box::new(plan),
                            unwind.expression.clone(),
                            unwind.alias.clone(),
                        ),
                        BoundClause::Subquery(subquery) => {
                            let sub_plan = Self::plan_query(*subquery)?;
                            LogicalOperator::Join(
                                Box::new(plan),
                                Box::new(sub_plan),
                                BoundExpression::Literal(Literal::Boolean(true)),
                            )
                        }
                        BoundClause::Merge(merge) => LogicalOperator::Merge {
                            child: Box::new(plan),
                            pattern: merge.pattern.clone(),
                            on_create_assignments: merge.on_create_assignments.clone(),
                            on_match_assignments: merge.on_match_assignments.clone(),
                        },
                        BoundClause::OptionalMatch(opt_match) => {
                            let inner_plan = Self::plan(BoundStatement::Query(
                                Some(opt_match.clone()),
                                None,
                                vec![],
                            ))?;
                            LogicalOperator::OptionalMatch(Box::new(plan), Box::new(inner_plan))
                        }
                        BoundClause::Match(match_clause) => {
                            let inner_plan = Self::plan(BoundStatement::Query(
                                Some(match_clause.clone()),
                                None,
                                vec![],
                            ))?;
                            LogicalOperator::Join(
                                Box::new(plan),
                                Box::new(inner_plan),
                                BoundExpression::Literal(Literal::Boolean(true)),
                            )
                        }
                        _ => plan,
                    };
                }

                Ok(plan)
            }
            _ => Err(LightningError::Internal("Not a query".into())),
        }
    }
}

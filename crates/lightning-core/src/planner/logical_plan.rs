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
    IndexScan(String, String, String, BoundExpression, Option<Vec<usize>>), // table_name, variable, index_key, pk_value, projected_idxs
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

fn expr_summary(e: &BoundExpression) -> String {
    let s = format!("{e:?}");
    if s.chars().count() > 70 {
        format!("{}…", s.chars().take(70).collect::<String>())
    } else {
        s
    }
}

impl LogicalOperator {
    /// Render the plan as an indented operator tree (used by EXPLAIN).
    pub fn format_plan(&self) -> String {
        let mut out = String::new();
        self.fmt_plan(0, &mut out);
        out
    }

    fn fmt_plan(&self, indent: usize, out: &mut String) {
        out.push_str(&"  ".repeat(indent));
        out.push_str(&self.explain_label());
        out.push('\n');
        for c in self.explain_children() {
            c.fmt_plan(indent + 1, out);
        }
    }

    fn explain_label(&self) -> String {
        use LogicalOperator::*;
        match self {
            Scan(t, v, _, projected, _) => match projected {
                Some(idxs) => format!("Scan {t} AS {v} project={idxs:?}"),
                None => format!("Scan {t} AS {v}"),
            },
            IndexScan(t, v, k, ..) => format!("IndexScan {t} AS {v} ON {k}"),
            SingleRow => "SingleRow".to_string(),
            Filter(_, e) => format!("Filter {}", expr_summary(e)),
            Projection(_, items) => format!("Projection [{} items]", items.len()),
            SemiMasker(_, v, m) => format!("SemiMasker {v} mask={m}"),
            Unwind(_, _, a) => format!("Unwind {a}"),
            Join(_, _, c) => match c {
                BoundExpression::Literal(crate::parser::ast::Literal::Boolean(true)) =>
                    "CrossJoin (cartesian product)".to_string(),
                _ => format!("Join {}", expr_summary(c)),
            },
            Aggregate { group_by_cols, aggregates, .. } =>
                format!("Aggregate group_by={} aggs={}", group_by_cols.len(), aggregates.len()),
            CreateNode(..) => "CreateNode".to_string(),
            CreateRel(..) => "CreateRel".to_string(),
            Delete(_, _, d) => format!("Delete detach={d}"),
            Set(..) => "Set".to_string(),
            Sort(..) => "Sort".to_string(),
            Limit(_, n) => format!("Limit {n}"),
            TopK(_, _, n) => format!("TopK {n}"),
            Skip(_, n) => format!("Skip {n}"),
            Call(_) => "Call".to_string(),
            Subquery(_) => "Subquery".to_string(),
            RecursiveJoin { rel_table, .. } => format!("RecursiveJoin {rel_table}"),
            Intersect { .. } => "Intersect".to_string(),
            AllShortestPaths { .. } => "AllShortestPaths".to_string(),
            Flatten(_) => "Flatten".to_string(),
            UnwindDedup(..) => "UnwindDedup".to_string(),
            Merge { .. } => "Merge".to_string(),
            Union(_, _, all) => format!("Union all={all}"),
            OptionalMatch(..) => "OptionalMatch".to_string(),
            With(..) => "With".to_string(),
            Accumulate(_) => "Accumulate".to_string(),
            Distinct(..) => "Distinct".to_string(),
            SemiJoin(_, _, c, anti) => format!("SemiJoin anti={anti} {}", expr_summary(c)),
            Profile(_) => "Profile".to_string(),
            Explain(_) => "Explain".to_string(),
            CreateTableNode { name, .. } => format!("CreateTableNode {name}"),
            CreateTableRel { name, .. } => format!("CreateTableRel {name}"),
            DropTable(n, _) => format!("DropTable {n}"),
            CopyFrom { table_name, .. } => format!("CopyFrom {table_name}"),
            CopyTo { table_name, .. } => format!("CopyTo {table_name}"),
            Transaction(_) => "Transaction".to_string(),
            Checkpoint => "Checkpoint".to_string(),
            Vacuum => "Vacuum".to_string(),
            AlterTable { name, .. } => format!("AlterTable {name}"),
            CreateConstraint { name, .. } => format!("CreateConstraint {name}"),
            DropConstraint(n) => format!("DropConstraint {n}"),
            CreateIndex { name, .. } => format!("CreateIndex {name}"),
            CreateVectorIndex { table_name, .. } => format!("CreateVectorIndex {table_name}"),
            CreateFtsIndex { table_name, .. } => format!("CreateFtsIndex {table_name}"),
            DropIndex(n) => format!("DropIndex {n}"),
            CountRelTable { rel_table, .. } => format!("CountRelTable {rel_table}"),
            CreateSequence { name, .. } => format!("CreateSequence {name}"),
            CreateMacro { name, .. } => format!("CreateMacro {name}"),
        }
    }

    fn explain_children(&self) -> Vec<&LogicalOperator> {
        use LogicalOperator::*;
        match self {
            Filter(c, _) | Projection(c, _) | SemiMasker(c, _, _) | Unwind(c, _, _)
            | Flatten(c) | UnwindDedup(c, _) | Delete(c, _, _) | Set(c, _) | Sort(c, _)
            | Limit(c, _) | TopK(c, _, _) | Skip(c, _) | Subquery(c) | Accumulate(c)
            | Distinct(c, _) | Profile(c) | Explain(c)
            | Aggregate { child: c, .. } | RecursiveJoin { child: c, .. }
            | AllShortestPaths { child: c, .. } | Merge { child: c, .. } => vec![c],
            Join(l, r, _) | Union(l, r, _) | OptionalMatch(l, r) | SemiJoin(l, r, _, _) => {
                vec![l, r]
            }
            Intersect { probe_child, build_children, .. } => {
                let mut v: Vec<&LogicalOperator> = vec![probe_child];
                v.extend(build_children.iter());
                v
            }
            CreateNode(c, _) | CreateRel(c, _) => c.as_deref().into_iter().collect(),
            _ => vec![],
        }
    }

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
            | LogicalOperator::Profile(c)
            | LogicalOperator::SemiJoin(c, ..) => *c = Box::new(new_child),
            LogicalOperator::Join(l, _, _) | LogicalOperator::Union(l, _, _) => {
                *l = Box::new(new_child);
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
    /// Count all nodes in the plan tree (for optimizer fixed-point detection).
    pub fn node_count(&self) -> usize {
        let mut count = 1;
        match self {
            LogicalOperator::Filter(child, _)
            | LogicalOperator::Projection(child, _)
            | LogicalOperator::SemiMasker(child, _, _)
            | LogicalOperator::Unwind(child, _, _)
            | LogicalOperator::Sort(child, _)
            | LogicalOperator::Limit(child, _)
            | LogicalOperator::TopK(child, _, _)
            | LogicalOperator::Skip(child, _)
            | LogicalOperator::Subquery(child)
            | LogicalOperator::Flatten(child)
            | LogicalOperator::UnwindDedup(child, _)
            | LogicalOperator::Distinct(child, _)
            | LogicalOperator::Accumulate(child)
            | LogicalOperator::Profile(child)
            | LogicalOperator::Explain(child)
            | LogicalOperator::With(child, _, _)
            | LogicalOperator::Delete(child, _, _)
            | LogicalOperator::Set(child, _) => {
                count += child.node_count();
            }
            LogicalOperator::Join(left, right, _)
            | LogicalOperator::Union(left, right, _) => {
                count += left.node_count() + right.node_count();
            }
            LogicalOperator::OptionalMatch(child, inner) => {
                count += child.node_count() + inner.node_count();
            }
            LogicalOperator::Aggregate { child, .. } => {
                count += child.node_count();
            }
            LogicalOperator::CreateNode(Some(child), _) | LogicalOperator::CreateRel(Some(child), _) => {
                count += child.node_count();
            }
            LogicalOperator::CreateNode(None, _) | LogicalOperator::CreateRel(None, _) => {}
            LogicalOperator::RecursiveJoin { child, .. } => {
                count += child.node_count();
            }
            LogicalOperator::Intersect { probe_child, build_children, .. } => {
                count += probe_child.node_count();
                for bc in build_children {
                    count += bc.node_count();
                }
            }
            LogicalOperator::SemiJoin(left, right, _, _) => {
                count += left.node_count() + right.node_count();
            }
            LogicalOperator::Merge { child, .. } => {
                count += child.node_count();
            }
            _ => {}
        }
        count
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
            LogicalOperator::Join(left, right, _) | LogicalOperator::Union(left, right, _) => {
                left.get_variables(vars);
                right.get_variables(vars);
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
    /// Remap an ORDER BY expression from binder-relative indices/aliases to
    /// aggregate-output-relative indices.
    ///
    /// Group-by columns occupy output indices 0..group_by_count.
    /// Aggregate columns occupy output indices group_by_count..group_by_count+agg_count.
    ///
    /// The `ret_items` contains the RETURN clause's BoundProjectionItems, which
    /// provide the alias mapping (e.g., `sum(n.salary) AS total` → alias="total").
    fn remap_agg_order_by(
        expr: &mut BoundExpression,
        group_by_exprs: &[BoundProjectionItem],
        group_by_count: usize,
        ret_items: &[BoundProjectionItem],
    ) {
        match expr {
            BoundExpression::PropertyLookup(var, idx, _) => {
                for (gi, gb) in group_by_exprs.iter().enumerate() {
                    if let BoundExpression::PropertyLookup(gb_var, gb_idx, _) = &gb.expression {
                        if gb_var == var && gb_idx == idx {
                            *idx = gi;
                            return;
                        }
                    }
                }
                for (ri, item) in ret_items.iter().enumerate() {
                    if let BoundExpression::PropertyLookup(ri_var, ri_idx, _) = &item.expression {
                        if ri_var == var && ri_idx == idx {
                            let mut agg_offset = 0;
                            for prev_item in &ret_items[..ri] {
                                if prev_item.expression.is_aggregate() || matches!(&prev_item.expression, BoundExpression::PropertyLookup(_, _, _)) {
                                    let prev_var_idx = match &prev_item.expression {
                                        BoundExpression::PropertyLookup(pv, pi, _) => Some((pv.as_str(), *pi)),
                                        _ => None,
                                    };
                                    let is_gb = group_by_exprs.iter().any(|gb| {
                                        matches!(&gb.expression, BoundExpression::PropertyLookup(gv, gi, _)
                                            if prev_var_idx.is_some_and(|(pv, pi)| gv.as_str() == pv && *gi == pi))
                                    });
                                    if !is_gb {
                                        agg_offset += 1;
                                    }
                                }
                            }
                            *idx = group_by_count + agg_offset;
                            return;
                        }
                    }
                }
            }
            BoundExpression::Variable(name, _) => {
                // Variable expressions like `total` (from `sum(...) AS total`)
                // need to be resolved to the aggregate output column index.
                // Match the variable name against RETURN item aliases.
                for (ri, item) in ret_items.iter().enumerate() {
                    if item.alias == *name {
                        // Determine if this is a group-by or aggregate column
                        let is_gb = group_by_exprs.iter().any(|gb| {
                            gb.alias == item.alias
                        });
                        if is_gb {
                            // Find its position among group-by columns
                            for (gi, gb) in group_by_exprs.iter().enumerate() {
                                if gb.alias == item.alias {
                                    *expr = BoundExpression::PropertyLookup(
                                        String::new(), gi, item.expression.get_type(),
                                    );
                                    return;
                                }
                            }
                        } else {
                            // Aggregate column — count how many non-group-by
                            // RETURN items precede this one
                            let mut agg_idx = 0;
                            for prev_item in &ret_items[..ri] {
                                let prev_is_gb = group_by_exprs.iter().any(|gb| {
                                    gb.alias == prev_item.alias
                                });
                                if !prev_is_gb {
                                    agg_idx += 1;
                                }
                            }
                            *expr = BoundExpression::PropertyLookup(
                                String::new(), group_by_count + agg_idx, item.expression.get_type(),
                            );
                            return;
                        }
                    }
                }
            }
            BoundExpression::Function(name, args, _) => {
                for (ri, item) in ret_items.iter().enumerate() {
                    let item_is_match = match &item.expression {
                        BoundExpression::Function(iname, iargs, _) => {
                            iname.eq_ignore_ascii_case(name)
                                && args.len() == iargs.len()
                                && args.iter().zip(iargs.iter()).all(|(a, b)| {
                                    matches!((a, b),
                                        (BoundExpression::PropertyLookup(va, ia, _),
                                         BoundExpression::PropertyLookup(vb, ib, _))
                                        if va == vb && ia == ib
                                    )
                                })
                        }
                        _ => false,
                    };
                    if item_is_match || item.alias.eq_ignore_ascii_case(name) {
                        let is_gb = group_by_exprs.iter().any(|gb| {
                            gb.alias == item.alias
                        });
                        if is_gb {
                            for (gi, gb) in group_by_exprs.iter().enumerate() {
                                if gb.alias == item.alias {
                                    *expr = BoundExpression::PropertyLookup(
                                        String::new(), gi, item.expression.get_type(),
                                    );
                                    return;
                                }
                            }
                        } else {
                            let mut agg_idx = 0;
                            for prev_item in &ret_items[..ri] {
                                let prev_is_gb = group_by_exprs.iter().any(|gb| {
                                    gb.alias == prev_item.alias
                                });
                                if !prev_is_gb {
                                    agg_idx += 1;
                                }
                            }
                            *expr = BoundExpression::PropertyLookup(
                                String::new(), group_by_count + agg_idx, item.expression.get_type(),
                            );
                            return;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    pub fn plan_query(query: BoundQuery) -> Result<LogicalOperator> {
        let mut plan = if query.union_queries.is_empty() {
            LogicalOperator::SingleRow
        } else {
            let first = &query.union_queries[0];
            let mut p = Self::plan_union_query(first.clone())?;
            // A trailing SKIP/LIMIT on the last UNION arm applies to the whole
            // union (hoisted by the binder).
            if let Some(skip) = first.union_skip {
                p = LogicalOperator::Skip(Box::new(p), skip as u64);
            }
            if let Some(limit) = first.union_limit {
                p = LogicalOperator::Limit(Box::new(p), limit as u64);
            }
            p
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
                    .map(BoundExpression::Literal)
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
                    if !m.alternatives.is_empty() {
                        // Ambiguous unlabeled relationship: union the match
                        // plan for each candidate rel table, so the rest of the
                        // query runs over all candidates.
                        let mut it = m.alternatives.into_iter();
                        let first = it.next().expect("alternatives non-empty");
                        let mut p =
                            Self::plan(BoundStatement::Query(Some(first), None, vec![]))?;
                        for alt in it {
                            let r =
                                Self::plan(BoundStatement::Query(Some(alt), None, vec![]))?;
                            p = LogicalOperator::Union(Box::new(p), Box::new(r), true);
                        }
                        p
                    } else {
                    let mut match_elements_iter = m.elements.into_iter();
                    let first = match_elements_iter
                        .next()
                        .ok_or_else(|| crate::LightningError::Query("Empty MATCH clause".into()))?;

                    let mut rel_vars: Vec<(String, String)> = Vec::new();
                    // Variables already produced by earlier match elements. A
                    // node re-referenced by a later pattern must join on the
                    // existing binding, NOT be re-scanned (which would form a
                    // cartesian product).
                    let mut bound_vars: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    let mut plan = match first {
                        crate::planner::binder::BoundMatchElement::Node(table, var, properties) => {
                            bound_vars.insert(var.clone());
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
                                dst_var,
                                path_var,
                                max_depth,
                            } => {
                                bound_vars.insert(src_var.clone());
                                bound_vars.insert(dst_var.clone());
                                bound_vars.insert(path_var.clone());
                                plan = LogicalOperator::AllShortestPaths {
                                    child: Box::new(plan),
                                    rel_table_name,
                                    src_var_name: src_var,
                                    dst_var_name: dst_var,
                                    path_var_name: path_var,
                                    max_depth,
                                };
                            }
                            crate::planner::binder::BoundMatchElement::Node(
                                table,
                                var,
                                properties,
                            ) => {
                                if bound_vars.contains(&var) {
                                    // Already bound by an earlier pattern: this is
                                    // the same node re-referenced, so constrain the
                                    // existing binding instead of re-scanning.
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
                                        plan = LogicalOperator::Filter(Box::new(plan), condition);
                                    }
                                } else {
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
                                        node_op =
                                            LogicalOperator::Filter(Box::new(node_op), condition);
                                    }
                                    plan = LogicalOperator::Join(
                                        Box::new(plan),
                                        Box::new(node_op),
                                        BoundExpression::Literal(
                                            crate::parser::ast::Literal::Boolean(true),
                                        ),
                                    );
                                    bound_vars.insert(var.clone());
                                }
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
                                        _d_var,
                                        properties,
                                    ) = next
                                    {
                                        bound_vars.insert(src_var.clone());
                                        bound_vars.insert(dst_var.clone());
                                        bound_vars.insert(rel_var.clone());
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
                                    bound_vars.insert(src_var.clone());
                                    // Save rel table name before it's consumed by Scan
                                    let rel_table_for_uniqueness = rel_table.clone();
                                    let rel_scan = LogicalOperator::Scan(
                                        rel_table,
                                        rel_var.clone(),
                                        None,
                                        None,
                                        None,
                                    );
                                    // Save rel var name before join conditions consume it
                                    let cur_rel_var = rel_var;
                                    let rel_var_for_uniqueness = cur_rel_var.clone();
                                    bound_vars.insert(rel_var_for_uniqueness.clone());
                                    let join_cond = BoundExpression::Comparison(
                                        Box::new(BoundExpression::PropertyLookup(
                                            src_var,
                                            0,
                                            LogicalType::Uint64,
                                        )),
                                        crate::parser::ast::ComparisonOperator::Equal,
                                        Box::new(BoundExpression::PropertyLookup(
                                            cur_rel_var.clone(),
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
                                        bound_vars.insert(d_var.clone());
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
                                                cur_rel_var,
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
                                        // Relationship uniqueness: each relationship in a path
                                        // must be distinct from all previously traversed ones.
                                        // Compare by (_src, _dst) since rel tables lack an _id column.
                                        // Only compare rels from the same table (different tables
                                        // can never share an edge).
                                        for (prev_rel, prev_table) in &rel_vars {
                                            if prev_table != &rel_table_for_uniqueness {
                                                continue;
                                            }
                                            // NOT (prev._src == cur._src AND prev._dst == cur._dst)
                                            let uniqueness_cond = BoundExpression::Not(
                                                Box::new(BoundExpression::Logical(
                                                    Box::new(BoundExpression::Comparison(
                                                        Box::new(BoundExpression::PropertyLookup(
                                                            prev_rel.clone(),
                                                            0,
                                                            LogicalType::Uint64,
                                                        )),
                                                        crate::parser::ast::ComparisonOperator::Equal,
                                                        Box::new(BoundExpression::PropertyLookup(
                                                            rel_var_for_uniqueness.clone(),
                                                            0,
                                                            LogicalType::Uint64,
                                                        )),
                                                    )),
                                                    crate::parser::ast::LogicalOperator::And,
                                                    Box::new(BoundExpression::Comparison(
                                                        Box::new(BoundExpression::PropertyLookup(
                                                            prev_rel.clone(),
                                                            1,
                                                            LogicalType::Uint64,
                                                        )),
                                                        crate::parser::ast::ComparisonOperator::Equal,
                                                        Box::new(BoundExpression::PropertyLookup(
                                                            rel_var_for_uniqueness.clone(),
                                                            1,
                                                            LogicalType::Uint64,
                                                        )),
                                                    )),
                                                )),
                                            );
                                            plan = LogicalOperator::Filter(
                                                Box::new(plan),
                                                uniqueness_cond,
                                            );
                                        }
                                        rel_vars.push((rel_var_for_uniqueness, rel_table_for_uniqueness));
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
                    }
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
                            let mut _aggregates_found = false;

                            for item in &ret.items {
                                if item.expression.is_aggregate() {
                                    _aggregates_found = true;
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
                                                    "Unknown aggregate function: {name}"
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
                                                    crate::parser::ast::Literal::Integer(1),
                                                ),
                                                alias: "_dummy".to_string(),
                                            });
                                            group_by_exprs.len() + aggregate_arg_exprs.len() - 1
                                        } else {
                                            let arg_expr = args[0].clone();
                                            // Bare Variable references (e.g. `count(p)` where p is a
                                            // node/rel pattern variable) cannot be resolved to columns
                                            // in the input batch — the schema uses property names, not
                                            // variable names. Treat them like count(*) via a dummy
                                            // non-null literal.
                                            let is_bare_variable = matches!(
                                                &arg_expr,
                                                BoundExpression::Variable(_, _)
                                            );
                                            if is_bare_variable {
                                                aggregate_arg_exprs.push(BoundProjectionItem {
                                                    expression: BoundExpression::Literal(
                                                        crate::parser::ast::Literal::Integer(1),
                                                    ),
                                                    alias: "_dummy".to_string(),
                                                });
                                                group_by_exprs.len() + aggregate_arg_exprs.len() - 1
                                            } else {
                                                let idx = group_by_exprs.len()
                                                    + aggregate_arg_exprs.len();
                                                aggregate_arg_exprs.push(BoundProjectionItem {
                                                    expression: arg_expr,
                                                    alias: "".to_string(),
                                                });
                                                idx
                                            }
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
                                    aggregates,
                                };

                                // After aggregate, we need a final projection to match the RETURN items.
                                // The Sort's ORDER BY expressions use binder-relative PropertyLookup
                                // indices that reference the original scan schema, not the aggregate
                                // output schema. Remap them to aggregate-output-relative indices:
                                //   group_by_exprs → indices 0..group_by_exprs.len()-1
                                //   aggregate ags  → indices group_by_exprs.len()..
                                let group_by_count = group_by_exprs.len();
                                let remapped_order_by = ret.order_by.as_ref().map(|items| {
                                    items.iter().map(|item| {
                                        let mut new_item = item.clone();
                                        Self::remap_agg_order_by(
                                            &mut new_item.expression,
                                            &group_by_exprs,
                                            group_by_count,
                                            &ret.items,
                                        );
                                        new_item
                                    }).collect::<Vec<_>>()
                                });
                                if let Some(order_by) = &remapped_order_by {
                                    if !order_by.is_empty() {
                                        current_plan = LogicalOperator::Sort(
                                            Box::new(current_plan),
                                            order_by.clone(),
                                        );
                                    }
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
                                let skip_val = if skip < 0.0 { 0u64 } else { skip as u64 };
                                current_plan =
                                    LogicalOperator::Skip(Box::new(current_plan), skip_val);
                            }
                            if let Some(limit) = ret.limit {
                                let limit_val = if limit < 0.0 { 0u64 } else { limit as u64 };
                                current_plan =
                                    LogicalOperator::Limit(Box::new(current_plan), limit_val);
                            }
                            current_plan
                        }
                        BoundClause::Unwind(unwind) => LogicalOperator::Unwind(
                            Box::new(plan),
                            unwind.expression.clone(),
                            unwind.alias.clone(),
                        ),
                        BoundClause::Where(expr) => {
                            LogicalOperator::Filter(Box::new(plan), expr.clone())
                        }
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
                        BoundClause::With(ret, bound_where) => {
                            // WITH is like a RETURN + Projection that creates a new scope.
                            // It does NOT use the LogicalOperator::With variant directly;
                            // instead we create a Projection for the WITH items and a
                            // Filter for the WHERE clause, if present.
                            let mut current_plan = plan;
                            // Build aggregate-like processing (same as RETURN)
                            for item in &ret.items {
                                if item.expression.is_aggregate() {
                                    // For simplicity, WITH + aggregates not fully supported yet
                                }
                            }
                            // Apply WHERE
                            if let Some(w) = bound_where {
                                current_plan = LogicalOperator::Filter(
                                    Box::new(current_plan),
                                    w.expression.clone(),
                                );
                            }
                            // Apply ORDER BY
                            if let Some(order_by) = &ret.order_by {
                                current_plan = LogicalOperator::Sort(
                                    Box::new(current_plan),
                                    order_by.clone(),
                                );
                            }
                            // Apply projection
                            // WITH always projects (distinct or plain)
                            current_plan = LogicalOperator::Projection(
                                Box::new(current_plan),
                                ret.items.clone(),
                            );
                            // Apply skip/limit
                            if let Some(skip) = ret.skip {
                                let skip_val = if skip < 0.0 { 0u64 } else { skip as u64 };
                                current_plan = LogicalOperator::Skip(
                                    Box::new(current_plan), skip_val,
                                );
                            }
                            if let Some(limit) = ret.limit {
                                let limit_val = if limit < 0.0 { 0u64 } else { limit as u64 };
                                current_plan = LogicalOperator::Limit(
                                    Box::new(current_plan), limit_val,
                                );
                            }
                            current_plan
                        }
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
                        other => {
                            return Err(LightningError::Internal(format!(
                                "Unsupported clause type in query: {other:?}"
                            )));
                        }
                    };
                }

                Ok(plan)
            }
        }
    }
}

pub mod ast;

use self::ast::*;
use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "parser/cypher.pest"]
pub struct CypherParser;

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Pest error: {0}")]
    Pest(#[from] pest::error::Error<Rule>),
    #[error("Internal parser error: {0}")]
    Internal(String),
}

pub fn parse(query_str: &str) -> Result<Query, ParserError> {
    let preprocessed = preprocess_distinct_functions(query_str);

    let (clean, order_by, skip, limit) = strip_modifiers(&preprocessed);
    let mut pairs = CypherParser::parse(Rule::query, &clean)?;
    let mut q = parse_query(pairs.next().unwrap())?;

    if order_by.is_some() || skip.is_some() || limit.is_some() {
        inject_modifiers(&mut q, order_by, skip, limit)?;
    }
    Ok(q)
}

fn preprocess_distinct_functions(s: &str) -> String {
    // Convert count(DISTINCT x) to COUNT_DISTINCT(x)
    // Also handles other aggregate functions with DISTINCT
    let patterns = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"];
    let mut result = s.to_string();

    for func in patterns {
        let upper = func.to_uppercase();
        let search = format!("{}(DISTINCT ", upper);
        let replace = format!("{}_DISTINCT(", upper);

        // Case-insensitive replacement
        let mut pos = 0;
        while let Some(idx) = result[pos..].to_uppercase().find(&search) {
            let actual_idx = pos + idx;
            result = format!(
                "{}{}{}",
                &result[..actual_idx],
                replace,
                &result[actual_idx + search.len()..]
            );
            pos = actual_idx + replace.len();
        }
    }

    result
}

fn strip_modifiers(s: &str) -> (String, Option<String>, Option<f64>, Option<f64>) {
    let mut result = s.to_string();
    let mut ord = None;
    let mut skp = None;
    let mut lmt = None;

    // Extract ORDER BY
    let upper = result.to_uppercase();
    if let Some(p) = upper.find("RETURN ") {
        let after_return = p + 7;
        let after = &result[after_return..];
        let after_upper = after.to_uppercase();
        if let Some(pos) = after_upper.find("ORDER BY ") {
            let order_by_pos = after_return + pos;
            let order_expr_start = order_by_pos + 9; // after "ORDER BY "
            let order_rest = &result[order_expr_start..];
            let order_upper = order_rest.to_uppercase();
            // Find end of ORDER BY expression
            let end = order_upper
                .find(" LIMIT ")
                .or(order_upper.find(" SKIP "))
                .unwrap_or(order_rest.len());
            ord = Some(order_rest[..end].trim().to_string());
            // Remove only "ORDER BY <expr>" part, not the RETURN items before it
            result = format!(
                "{}{}",
                &result[..order_by_pos],
                &result[order_expr_start + end..]
            );
        }
    }

    // Extract SKIP
    let upper = result.to_uppercase();
    if let Some(p) = upper.find("RETURN ") {
        let after_return = p + 7;
        let after = &result[after_return..];
        let after_upper = after.to_uppercase();
        if let Some(pos) = after_upper.find(" SKIP ") {
            let skip_pos = after_return + pos;
            let skip_val_start = skip_pos + 6; // after "SKIP "
            let skip_rest = &result[skip_val_start..];
            let skip_upper = skip_rest.to_uppercase();
            // Find end of SKIP value
            let end = skip_upper.find(" LIMIT ").unwrap_or(skip_rest.len());
            if let Ok(v) = skip_rest[..end].trim().parse::<f64>() {
                skp = Some(v);
            }
            // Remove only "SKIP <val>" part
            result = format!("{}{}", &result[..skip_pos], &result[skip_val_start + end..]);
        }
    }

    // Extract LIMIT
    let upper = result.to_uppercase();
    if let Some(p) = upper.find("RETURN ") {
        let after_return = p + 7;
        let after = &result[after_return..];
        let after_upper = after.to_uppercase();
        if let Some(pos) = after_upper.find(" LIMIT ") {
            let limit_pos = after_return + pos;
            let limit_val_start = limit_pos + 7; // after "LIMIT "
            let limit_rest = &result[limit_val_start..];
            // Find end of LIMIT value
            let mut end = 0;
            for (i, c) in limit_rest.chars().enumerate() {
                if !c.is_numeric() && !c.is_whitespace() {
                    break;
                }
                end = i + 1;
            }
            if let Ok(v) = limit_rest[..end].trim().parse::<f64>() {
                lmt = Some(v);
            }
            // Remove only "LIMIT <val>" part
            result = format!(
                "{}{}",
                &result[..limit_pos],
                &result[limit_val_start + end..]
            );
        }
    }
    (result.trim().to_string(), ord, skp, lmt)
}

fn inject_modifiers(
    q: &mut Query,
    ord: Option<String>,
    skp: Option<f64>,
    lmt: Option<f64>,
) -> Result<(), ParserError> {
    if let Some(ref e) = ord {
        if let Ok(p) = CypherParser::parse(Rule::expression, e) {
            let expr = parse_expression(p.into_iter().next().unwrap())?;
            let desc = e.to_uppercase().contains("DESC");
            for u in &mut q.union_queries {
                if let Statement::Match(_, _, cs) = &mut u.statement {
                    for c in cs.iter_mut() {
                        if let Clause::Return(ref mut r) = c {
                            r.order_by = Some(vec![OrderByItem {
                                expression: expr.clone(),
                                descending: desc,
                            }]);
                        }
                    }
                }
            }
        }
    }
    if let Some(v) = skp {
        for u in &mut q.union_queries {
            if let Statement::Match(_, _, cs) = &mut u.statement {
                for c in cs.iter_mut() {
                    if let Clause::Return(ref mut r) = c {
                        r.skip = Some(v);
                    }
                }
            }
        }
    }
    if let Some(v) = lmt {
        for u in &mut q.union_queries {
            if let Statement::Match(_, _, cs) = &mut u.statement {
                for c in cs.iter_mut() {
                    if let Clause::Return(ref mut r) = c {
                        r.limit = Some(v);
                    }
                }
            }
        }
    }
    Ok(())
}

fn parse_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParserError> {
    let mut ugs = Vec::new();
    let mut ie = false;
    let mut ip = false;
    for i in pair.into_inner() {
        match i.as_rule() {
            Rule::EXPLAIN_OP => {
                let st = i.as_str().to_uppercase();
                if st == "PROFILE" {
                    ip = true
                } else {
                    ie = true;
                }
            }
            Rule::union_query => ugs.push(parse_union_query(i)?),
            _ => {}
        }
    }
    Ok(Query {
        union_queries: ugs,
        is_explain: ie,
        is_profile: ip,
    })
}

fn parse_union_query(p: pest::iterators::Pair<Rule>) -> Result<UnionQuery, ParserError> {
    let mut stmt = None;
    let mut nu = None;
    let mut ia = false;
    for i in p.into_inner() {
        match i.as_rule() {
            Rule::statement => stmt = Some(parse_statement(i)?),
            Rule::UNION_OP => ia = i.as_str().to_uppercase().contains("ALL"),
            Rule::union_query => nu = Some((Box::new(parse_union_query(i)?), ia)),
            _ => {}
        }
    }
    Ok(UnionQuery {
        statement: stmt.unwrap(),
        next_union: nu,
    })
}

fn parse_statement(p: pest::iterators::Pair<Rule>) -> Result<Statement, ParserError> {
    let inner: Vec<_> = p.clone().into_inner().collect();
    let mut match_clause_opt = None;
    let mut where_clause_opt = None;
    let mut clauses = Vec::new();

    for i in p.into_inner() {
        match i.as_rule() {
            Rule::transaction_statement => {
                return Ok(Statement::Transaction(
                    match i.into_inner().next().unwrap().as_rule() {
                        Rule::begin_tx => TransactionAction::Begin,
                        Rule::commit_tx => TransactionAction::Commit,
                        _ => TransactionAction::Rollback,
                    },
                ))
            }
            Rule::checkpoint_statement => return Ok(Statement::Checkpoint),
            Rule::create_node_table => {
                let mut it = i.into_inner();
                let mut if_not_exists = false;
                let name = loop {
                    let next = it.next().unwrap();
                    match next.as_rule() {
                        Rule::if_not_exists => {
                            if_not_exists = true;
                            continue;
                        }
                        Rule::table_name => break next.as_str().to_string(),
                        _ => continue,
                    }
                };
                let mut cols = Vec::new();
                let mut pk = String::new();
                for j in it {
                    match j.as_rule() {
                        Rule::column_def => {
                            let mut c = j.into_inner();
                            cols.push(ColumnDefinition {
                                name: c.next().unwrap().as_str().to_string(),
                                data_type: parse_data_type(c.next().unwrap())?,
                            });
                        }
                        Rule::primary_key_def => {
                            pk = j.into_inner().next().unwrap().as_str().to_string()
                        }
                        _ => {}
                    }
                }
                return Ok(Statement::CreateTableNode {
                    name,
                    columns: cols,
                    primary_key: pk,
                    if_not_exists,
                });
            }
            Rule::create_rel_table => {
                let mut name = String::new();
                let mut from_table = String::new();
                let mut to_table = String::new();
                let mut cols = Vec::new();
                let mut if_not_exists = false;
                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::table_name => {
                            if name.is_empty() {
                                name = j.as_str().to_string();
                            } else if from_table.is_empty() {
                                from_table = j.as_str().to_string();
                            } else {
                                to_table = j.as_str().to_string();
                            }
                        }
                        Rule::column_def => {
                            let mut c = j.into_inner();
                            cols.push(ColumnDefinition {
                                name: c.next().unwrap().as_str().to_string(),
                                data_type: parse_data_type(c.next().unwrap())?,
                            });
                        }
                        _ => {}
                    }
                }
                return Ok(Statement::CreateTableRel {
                    name,
                    from_table,
                    to_table,
                    columns: cols,
                    if_not_exists,
                });
            }
            Rule::drop_table => {
                let mut it = i.into_inner();
                let mut if_exists = false;
                let name = loop {
                    let next = it.next().unwrap();
                    match next.as_rule() {
                        Rule::if_exists => {
                            if_exists = true;
                            continue;
                        }
                        Rule::table_name => break next.as_str().to_string(),
                        _ => continue,
                    }
                };
                return Ok(Statement::DropTable(name, if_exists));
            }
            Rule::match_clause => {
                let pats = parse_match_clause(i)?;
                match_clause_opt = Some(MatchClause { patterns: pats });
            }
            Rule::optional_match_clause => {
                let pats = parse_match_clause(i)?;
                clauses.push(Clause::OptionalMatch(MatchClause { patterns: pats }));
            }
            Rule::unwind_clause => {
                let mut it = i.into_inner();
                let expr = parse_expression(it.next().unwrap())?;
                let alias = it.next().unwrap().as_str().to_string();
                clauses.push(Clause::Unwind(UnwindClause {
                    expression: expr,
                    alias,
                }));
            }
            Rule::where_clause => {
                let expr = parse_expression(i.into_inner().next().unwrap())?;
                where_clause_opt = Some(WhereClause { expression: expr });
            }
            Rule::return_clause => {
                let rc = parse_return_clause(i)?;
                clauses.push(Clause::Return(rc));
            }
            Rule::create_clause => {
                // Don't return immediately - add to clauses if there's a match clause
                let pattern = i
                    .into_inner()
                    .find(|j| j.as_rule() == Rule::pattern)
                    .map(|j| parse_pattern(j))
                    .transpose()?;

                if let Some(p) = pattern {
                    if match_clause_opt.is_some()
                        || !clauses.is_empty()
                        || where_clause_opt.is_some()
                    {
                        // There's context from previous clauses, add Create as a clause
                        clauses.push(Clause::Create(p));
                    } else {
                        // Standalone CREATE, return as statement
                        return Ok(Statement::Create(p));
                    }
                }
            }
            Rule::call_clause => {
                let mut it = i.into_inner();
                return Ok(Statement::StandaloneCall(
                    it.next().unwrap().as_str().to_string(),
                    Vec::new(),
                ));
            }
            Rule::set_clause => {
                let mut assignments = Vec::new();
                for j in i.into_inner() {
                    // j is a set_item; look inside for property_assignment or map_assignment
                    match j.as_rule() {
                        Rule::set_item => {
                            for child in j.into_inner() {
                                match child.as_rule() {
                                    Rule::property_assignment => {
                                        let mut parts = child.into_inner();
                                        let prop_lookup = parts.next().unwrap();
                                        let value = parts.next().unwrap();

                                        let mut prop_parts = prop_lookup.into_inner();
                                        let variable = prop_parts.next().unwrap().as_str().to_string();
                                        let property_key = prop_parts.next().unwrap().as_str().to_string();

                                        assignments.push(PropertyAssignment {
                                            variable,
                                            property_key,
                                            expression: parse_expression(value)?,
                                        });
                                    }
                                    Rule::map_assignment => {
                                        let mut parts = child.into_inner();
                                        let variable = parts.next().unwrap().as_str().to_string();
                                        let _op = parts.next().unwrap().as_str().to_string();
                                        for item in parts {
                                            if item.as_rule() == Rule::property_item {
                                                let mut item_parts = item.into_inner();
                                                let key = item_parts.next().unwrap().as_str().to_string();
                                                let val_expr = item_parts.next().unwrap();
                                                assignments.push(PropertyAssignment {
                                                    variable: variable.clone(),
                                                    property_key: key,
                                                    expression: parse_expression(val_expr)?,
                                                });
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if !assignments.is_empty() {
                    clauses.push(Clause::Set(SetClause { assignments }));
                }
            }
            Rule::remove_clause => {
                let mut properties = Vec::new();
                for j in i.into_inner() {
                    if j.as_rule() == Rule::property_lookup {
                        let mut parts = j.into_inner();
                        let variable = parts.next().unwrap().as_str().to_string();
                        let property_key = parts.next().unwrap().as_str().to_string();
                        properties.push((variable, property_key));
                    }
                }
                clauses.push(Clause::Remove(RemoveClause { properties }));
            }
            Rule::delete_clause => {
                let mut to_delete = Vec::new();
                let mut is_detach = false;
                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::variable => to_delete.push(j.as_str().to_string()),
                        _ => {
                            if j.as_str().to_uppercase() == "DETACH" {
                                is_detach = true;
                            }
                        }
                    }
                }
                clauses.push(Clause::Delete(DeleteClause {
                    variables: to_delete,
                    detach: is_detach,
                }));
            }
            Rule::merge_clause => {
                let mut pattern = None;
                let mut on_match = Vec::new();
                let mut on_create = Vec::new();

                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::pattern => pattern = Some(parse_pattern(j)?),
                        Rule::on_match_clause => {
                            for prop_assign in j.into_inner() {
                                if prop_assign.as_rule() == Rule::property_assignment {
                                    let mut parts = prop_assign.into_inner();
                                    let prop_lookup = parts.next().unwrap();
                                    let value = parts.next().unwrap();
                                    let mut prop_parts = prop_lookup.into_inner();
                                    let variable = prop_parts.next().unwrap().as_str().to_string();
                                    let property_key =
                                        prop_parts.next().unwrap().as_str().to_string();
                                    on_match.push(PropertyAssignment {
                                        variable,
                                        property_key,
                                        expression: parse_expression(value)?,
                                    });
                                }
                            }
                        }
                        Rule::on_create_clause => {
                            for prop_assign in j.into_inner() {
                                if prop_assign.as_rule() == Rule::property_assignment {
                                    let mut parts = prop_assign.into_inner();
                                    let prop_lookup = parts.next().unwrap();
                                    let value = parts.next().unwrap();
                                    let mut prop_parts = prop_lookup.into_inner();
                                    let variable = prop_parts.next().unwrap().as_str().to_string();
                                    let property_key =
                                        prop_parts.next().unwrap().as_str().to_string();
                                    on_create.push(PropertyAssignment {
                                        variable,
                                        property_key,
                                        expression: parse_expression(value)?,
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(p) = pattern {
                    if match_clause_opt.is_some()
                        || !clauses.is_empty()
                        || where_clause_opt.is_some()
                    {
                        clauses.push(Clause::Merge(MergeClause {
                            pattern: p,
                            on_match_assignments: on_match,
                            on_create_assignments: on_create,
                        }));
                    } else {
                        return Ok(Statement::Merge(MergeClause {
                            pattern: p,
                            on_match_assignments: on_match,
                            on_create_assignments: on_create,
                        }));
                    }
                }
            }
            _ => {}
        }
    }

    if match_clause_opt.is_some() {
        return Ok(Statement::Match(
            match_clause_opt,
            where_clause_opt,
            clauses,
        ));
    }

    if !clauses.is_empty() {
        return Ok(Statement::Match(None, None, clauses));
    }

    Err(ParserError::Internal("empty statement".into()))
}

fn parse_return_clause(p: pest::iterators::Pair<Rule>) -> Result<ReturnClause, ParserError> {
    // Check if RETURN contains DISTINCT by looking at string content
    let return_str = p.as_str().to_uppercase();
    let distinct = return_str.contains("DISTINCT");

    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for i in p.into_inner() {
        match i.as_rule() {
            Rule::projection_items => {
                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::star => items.push(ProjectionItem::Star),
                        Rule::projection_item => {
                            let mut it = j.into_inner();
                            let expr = parse_expression(it.next().unwrap())?;
                            let alias = it.next().map(|a| a.as_str().to_string());
                            items.push(ProjectionItem::Expression(expr, alias));
                        }
                        _ => {}
                    }
                }
            }
            Rule::order_by_clause => {
                let mut obi = Vec::new();
                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::sort_item => {
                            let mut sit = j.into_inner();
                            let expr = parse_expression(sit.next().unwrap())?;
                            let desc = sit
                                .next()
                                .map(|d| d.as_str().to_uppercase().contains("DESC"))
                                .unwrap_or(false);
                            obi.push(OrderByItem {
                                expression: expr,
                                descending: desc,
                            });
                        }
                        _ => {}
                    }
                }
                order_by = Some(obi);
            }
            Rule::skip_clause => {
                let val = i
                    .into_inner()
                    .next()
                    .unwrap()
                    .as_str()
                    .parse::<f64>()
                    .unwrap_or(0.0);
                skip = Some(val);
            }
            Rule::limit_clause => {
                let val = i
                    .into_inner()
                    .next()
                    .unwrap()
                    .as_str()
                    .parse::<f64>()
                    .unwrap_or(0.0);
                limit = Some(val);
            }
            _ => {}
        }
    }

    Ok(ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    })
}

fn parse_match_clause(p: pest::iterators::Pair<Rule>) -> Result<Vec<Pattern>, ParserError> {
    let mut pats = Vec::new();
    for i in p.into_inner() {
        if i.as_rule() == Rule::pattern {
            pats.push(parse_pattern(i)?);
        }
    }
    Ok(pats)
}

fn parse_pattern(p: pest::iterators::Pair<Rule>) -> Result<Pattern, ParserError> {
    let mut is_shortest_path = false;
    let mut shortest_path_start = None;
    let mut shortest_path_chain = None;
    let mut shortest_path_end = None;
    let mut np = None;
    let mut rcs = Vec::new();

    for i in p.into_inner() {
        match i.as_rule() {
            Rule::shortest_path_pattern => {
                is_shortest_path = true;
                for j in i.into_inner() {
                    match j.as_rule() {
                        Rule::node_pattern => {
                            if shortest_path_start.is_none() {
                                shortest_path_start = Some(parse_node_pattern(j)?);
                            } else if shortest_path_end.is_none() {
                                shortest_path_end = Some(parse_node_pattern(j)?);
                            }
                        }
                        Rule::relationship_chain => {
                            shortest_path_chain = Some(parse_relationship_chain(j)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::node_pattern => np = Some(parse_node_pattern(i)?),
            Rule::relationship_chain => rcs.push(parse_relationship_chain(i)?),
            _ => {}
        }
    }

    Ok(Pattern {
        node_pattern: np.unwrap(),
        relationship_chains: rcs,
        is_shortest_path,
        shortest_path_start,
        shortest_path_chain,
        shortest_path_end,
    })
}

fn parse_node_pattern(p: pest::iterators::Pair<Rule>) -> Result<NodePattern, ParserError> {
    let mut v = None;
    let mut ls = Vec::new();
    let mut ps = Vec::new();
    for i in p.into_inner() {
        match i.as_rule() {
            Rule::variable => v = Some(i.as_str().to_string()),
            Rule::labels => {
                for j in i.into_inner() {
                    ls.push(j.as_str().to_string());
                }
            }
            Rule::properties => {
                for j in i.into_inner() {
                    ps.push(parse_property_item(j)?);
                }
            }
            _ => {}
        }
    }
    Ok(NodePattern {
        variable: v,
        labels: ls,
        properties: ps,
    })
}

fn parse_relationship_chain(
    p: pest::iterators::Pair<Rule>,
) -> Result<RelationshipChain, ParserError> {
    let mut rp = None;
    let mut np = None;
    for i in p.into_inner() {
        match i.as_rule() {
            Rule::relationship_pattern => rp = Some(parse_relationship_pattern(i)?),
            Rule::node_pattern => np = Some(parse_node_pattern(i)?),
            _ => {}
        }
    }
    Ok(RelationshipChain {
        relationship_pattern: rp.unwrap(),
        node_pattern: np.unwrap(),
    })
}

fn parse_relationship_pattern(
    p: pest::iterators::Pair<Rule>,
) -> Result<RelationshipPattern, ParserError> {
    let mut v = None;
    let mut ls = Vec::new();
    let mut d = Direction::Both;
    let mut ps = Vec::new();
    let mut b = None;
    for i in p.into_inner() {
        match i.as_rule() {
            Rule::left_arrow => d = Direction::Left,
            Rule::right_arrow => d = Direction::Right,
            Rule::variable => v = Some(i.as_str().to_string()),
            Rule::labels => {
                for j in i.into_inner() {
                    ls.push(j.as_str().to_string());
                }
            }
            Rule::properties => {
                for j in i.into_inner() {
                    ps.push(parse_property_item(j)?);
                }
            }
            Rule::var_len_bounds => b = parse_var_len(i).ok(),
            _ => {}
        }
    }
    Ok(RelationshipPattern {
        variable: v,
        labels: ls,
        direction: d,
        properties: ps,
        var_len_bounds: b,
    })
}

fn parse_var_len(
    p: pest::iterators::Pair<Rule>,
) -> Result<(Option<u32>, Option<u32>), ParserError> {
    let mut l = None;
    let mut u = None;
    for i in p.into_inner() {
        if i.as_rule() == Rule::number_literal {
            l = Some(i.as_str().parse().unwrap_or(1));
        }
    }
    Ok((l, u))
}

fn parse_property_item(p: pest::iterators::Pair<Rule>) -> Result<PropertyItem, ParserError> {
    let mut k = String::new();
    let mut v = None;
    for i in p.into_inner() {
        match i.as_rule() {
            Rule::property_key => k = i.as_str().to_string(),
            Rule::expression => v = Some(parse_expression(i)?),
            _ => {}
        }
    }
    Ok(PropertyItem {
        key: k,
        value: v.unwrap(),
    })
}

fn parse_expression(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    parse_logical_or(p.into_inner().next().unwrap())
}

fn parse_logical_or(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let ps = p.into_inner().collect::<Vec<_>>();
    if ps.len() == 1 {
        return parse_xor(ps[0].clone());
    }
    let mut e = parse_xor(ps[0].clone())?;
    for i in (1..ps.len()).step_by(2) {
        e = Expression::Logical(
            Box::new(e),
            LogicalOperator::Or,
            Box::new(parse_xor(ps[i + 1].clone())?),
        );
    }
    Ok(e)
}

fn parse_xor(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let ps = p.into_inner().collect::<Vec<_>>();
    if ps.len() == 1 {
        return parse_logical_and(ps[0].clone());
    }
    let mut e = parse_logical_and(ps[0].clone())?;
    for i in (1..ps.len()).step_by(2) {
        e = Expression::Logical(
            Box::new(e),
            LogicalOperator::Xor,
            Box::new(parse_logical_and(ps[i + 1].clone())?),
        );
    }
    Ok(e)
}

fn parse_logical_and(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let ps = p.into_inner().collect::<Vec<_>>();
    if ps.len() == 1 {
        return parse_not(ps[0].clone());
    }
    let mut e = parse_not(ps[0].clone())?;
    for i in (1..ps.len()).step_by(2) {
        e = Expression::Logical(
            Box::new(e),
            LogicalOperator::And,
            Box::new(parse_not(ps[i + 1].clone())?),
        );
    }
    Ok(e)
}

fn parse_not(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let mut not_count = 0;
    let mut comparison_pair = None;

    for i in p.into_inner() {
        match i.as_rule() {
            Rule::not_op => not_count += 1,
            Rule::comparison_expr => comparison_pair = Some(i),
            _ => {}
        }
    }

    let mut expr = parse_comparison(comparison_pair.unwrap())?;

    // Apply NOT operators (each NOT inverts the expression)
    for _ in 0..not_count {
        expr = Expression::Not(Box::new(expr));
    }

    Ok(expr)
}

fn parse_comparison(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let ps = p.into_inner().collect::<Vec<_>>();
    if ps.len() == 1 {
        return parse_arithmetic(ps[0].clone());
    }

    if ps[1].as_rule() == Rule::comparison_operator {
        return Ok(Expression::Comparison(
            Box::new(parse_arithmetic(ps[0].clone())?),
            parse_comparison_operator(ps[1].clone()),
            Box::new(parse_arithmetic(ps[2].clone())?),
        ));
    } else if ps[1].as_rule() == Rule::string_predicate {
        let op_pair = ps[1].clone().into_inner().next().unwrap();
        let right_expr = op_pair.clone().into_inner().next().unwrap();

        let func_name = match op_pair.as_rule() {
            Rule::contains_op => "CONTAINS",
            Rule::starts_with_op => "STARTS_WITH",
            Rule::ends_with_op => "ENDS_WITH",
            _ => {
                return Err(ParserError::Internal(format!(
                    "Unknown string predicate: {:?}",
                    op_pair.as_rule()
                )))
            }
        };

        return Ok(Expression::Function(
            func_name.to_string(),
            vec![
                parse_arithmetic(ps[0].clone())?,
                parse_arithmetic(right_expr)?,
            ],
            false,
        ));
    } else if ps[1].as_rule() == Rule::is_null_check {
        let is_not = ps[1].clone().into_inner().any(|p| p.as_rule() == Rule::not_op);
        let func_name = if is_not { "IS_NOT_NULL" } else { "IS_NULL" };
        return Ok(Expression::Function(
            func_name.to_string(),
            vec![parse_arithmetic(ps[0].clone())?],
            false,
        ));
    } else if ps[1].as_rule() == Rule::in_check {
        let is_not = ps[1].clone().into_inner().any(|p| {
            let s = p.as_str().to_uppercase();
            s == "NOT"
        });
        let lhs = parse_arithmetic(ps[0].clone())?;
        // Collect all list items from the in_check
        let mut items = Vec::new();
        for child in ps[1].clone().into_inner() {
            if child.as_rule() == Rule::expression {
                items.push(parse_expression(child)?);
            }
        }
        if items.is_empty() {
            return Ok(Expression::Literal(Literal::Boolean(is_not)));
        }
        // Build: (lhs = item1) OR (lhs = item2) OR ...
        let mut or_expr = Expression::Comparison(
            Box::new(lhs.clone()),
            ComparisonOperator::Equal,
            Box::new(items[0].clone()),
        );
        for item in &items[1..] {
            or_expr = Expression::Logical(
                Box::new(or_expr),
                LogicalOperator::Or,
                Box::new(Expression::Comparison(
                    Box::new(lhs.clone()),
                    ComparisonOperator::Equal,
                    Box::new(item.clone()),
                )),
            );
        }
        if is_not {
            return Ok(Expression::Not(Box::new(or_expr)));
        }
        return Ok(or_expr);
    }

    Err(ParserError::Internal(format!(
        "Unexpected comparison rule: {:?}",
        ps[1].as_rule()
    )))
}

fn parse_arithmetic(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let ps = p.into_inner().collect::<Vec<_>>();
    if ps.len() == 1 {
        return parse_atom(ps[0].clone());
    }
    let mut e = parse_atom(ps[0].clone())?;
    for i in (1..ps.len()).step_by(2) {
        e = Expression::Arithmetic(
            Box::new(e),
            parse_arithmetic_operator(ps[i].clone()),
            Box::new(parse_atom(ps[i + 1].clone())?),
        );
    }
    Ok(e)
}

fn parse_atom(p: pest::iterators::Pair<Rule>) -> Result<Expression, ParserError> {
    let i = p.into_inner().next().unwrap();
    match i.as_rule() {
        Rule::literal => Ok(Expression::Literal(parse_literal(i)?)),
        Rule::variable => Ok(Expression::Variable(i.as_str().to_string())),
        Rule::parameter => {
            let s = i.as_str();
            Ok(Expression::Parameter(s[1..].to_string()))
        }
        Rule::function_call => {
            let mut it = i.into_inner();
            let n = it.next().unwrap().as_str().to_string();
            let distinct = n.to_uppercase().contains("_DISTINCT");
            let clean_name = if distinct {
                n.to_uppercase().replace("_DISTINCT", "")
            } else {
                n.clone()
            };

            let mut as_ = Vec::new();

            let collected: Vec<_> = it.collect();

            if collected.is_empty() {
                // No arguments (e.g., COUNT(*))
            } else if collected.len() == 1 && collected[0].as_rule() == Rule::star {
                // Star argument - handled separately
            } else {
                for item in collected {
                    match item.as_rule() {
                        Rule::expression => {
                            as_.push(parse_expression(item)?);
                        }
                        Rule::star => {}
                        other => {
                            tracing::debug!("unexpected arg rule: {:?}", other);
                        }
                    }
                }
            }

            Ok(Expression::Function(clean_name, as_, distinct))
        }
        Rule::property_lookup => {
            let mut it = i.into_inner();
            Ok(Expression::PropertyLookup(
                it.next().unwrap().as_str().to_string(),
                it.next().unwrap().as_str().to_string(),
            ))
        }
        Rule::list_literal => {
            let mut is = Vec::new();
            for i in i.into_inner() {
                is.push(parse_expression(i)?);
            }
            Ok(Expression::List(is))
        }
        Rule::parenthesized_expression => parse_expression(i.into_inner().next().unwrap()),
        Rule::case_expression => Ok(Expression::Case {
            expression: None,
            when_then: Vec::new(),
            else_expression: None,
        }),
        Rule::exists_subquery => Ok(Expression::Exists(Vec::new())),
        Rule::cast_expression => {
            let mut inner = i.into_inner();
            let expr = parse_expression(inner.next().unwrap())?;
            let type_literal = inner.last().unwrap().as_str().to_uppercase();
            Ok(Expression::Function(
                "CAST".to_string(),
                vec![expr, Expression::Literal(Literal::String(type_literal))],
                false,
            ))
        }
        Rule::extract_expression => {
            let mut inner = i.into_inner();
            let field_token = inner.next().unwrap();
            let field = field_token.as_str().to_uppercase();
            let _from = inner.next(); // skip FROM
            let source = parse_expression(inner.next().unwrap())?;
            Ok(Expression::Function(
                "DATE_PART".to_string(),
                vec![
                    Expression::Literal(Literal::String(field)),
                    source,
                ],
                false,
            ))
        }
        Rule::list_subscript => {
            let tokens: Vec<_> = i.into_inner().collect();
            if tokens.is_empty() {
                return Err(ParserError::Internal("empty list_subscript".into()));
            }
            let variable = if tokens[0].as_rule() == Rule::variable {
                tokens[0].as_str().to_string()
            } else {
                return Err(ParserError::Internal("expected variable in subscript".into()));
            };
            let index_expr = if tokens.len() > 1 {
                parse_expression(tokens[1].clone())?
            } else {
                return Err(ParserError::Internal("expected index in subscript".into()));
            };
            let has_range = tokens.iter().any(|p| p.as_rule() == Rule::range_operator);
            if has_range {
                let end_expr = if tokens.len() > 3 {
                    Some(parse_expression(tokens[3].clone())?)
                } else {
                    None
                };
                let mut args = vec![Expression::Variable(variable.clone()), index_expr];
                if let Some(end) = end_expr {
                    args.push(end);
                }
                Ok(Expression::Function("LIST_SLICE".to_string(), args, false))
            } else {
                Ok(Expression::Function(
                    "LIST_EXTRACT".to_string(),
                    vec![Expression::Variable(variable), index_expr],
                    false,
                ))
            }
        }
        _ => Err(ParserError::Internal(format!("atom:{:?}", i.as_rule()))),
    }
}

fn parse_literal(p: pest::iterators::Pair<Rule>) -> Result<Literal, ParserError> {
    let i = p.into_inner().next().unwrap();
    match i.as_rule() {
        Rule::string_literal => {
            let s = i.as_str();
            Ok(Literal::String(s[1..s.len() - 1].to_string()))
        }
        Rule::number_literal => Ok(Literal::Number(i.as_str().parse().unwrap_or(0.0))),
        Rule::boolean_literal => Ok(Literal::Boolean(i.as_str().to_uppercase() == "TRUE")),
        Rule::null_literal => Ok(Literal::Null),
        _ => Err(ParserError::Internal("bad lit".into())),
    }
}

fn parse_comparison_operator(p: pest::iterators::Pair<Rule>) -> ComparisonOperator {
    match p.as_str() {
        "=" => ComparisonOperator::Equal,
        "!=" | "<>" => ComparisonOperator::NotEqual,
        "<" => ComparisonOperator::LessThan,
        "<=" => ComparisonOperator::LessThanOrEqual,
        ">" => ComparisonOperator::GreaterThan,
        ">=" => ComparisonOperator::GreaterThanOrEqual,
        _ => unreachable!(),
    }
}

fn parse_arithmetic_operator(p: pest::iterators::Pair<Rule>) -> ArithmeticOperator {
    match p.as_str() {
        "+" => ArithmeticOperator::Add,
        "-" => ArithmeticOperator::Subtract,
        "*" => ArithmeticOperator::Multiply,
        "/" => ArithmeticOperator::Divide,
        "%" => ArithmeticOperator::Modulo,
        _ => unreachable!(),
    }
}

fn parse_data_type(p: pest::iterators::Pair<Rule>) -> Result<DataType, ParserError> {
    match p.as_rule() {
        Rule::data_type => {
            // The data_type rule wraps the actual type - extract the string and match it
            match p.as_str().to_uppercase().as_str() {
                "INT64" => Ok(DataType::Int64),
                "INT32" => Ok(DataType::Int32),
                "DOUBLE" => Ok(DataType::Double),
                "FLOAT" => Ok(DataType::Float),
                "STRING" => Ok(DataType::String),
                "BOOL" => Ok(DataType::Bool),
                "DATE" => Ok(DataType::Date),
                "TIMESTAMP" => Ok(DataType::Timestamp),
                s if s.contains("LIST") => {
                    // Handle LIST(INT32) format - extract inner type
                    if let Some(start) = s.find("(") {
                        if let Some(end) = s.find(")") {
                            let inner_str = &s[start + 1..end];
                            match inner_str.to_uppercase().as_str() {
                                "INT64" => Ok(DataType::List(Box::new(DataType::Int64))),
                                "INT32" => Ok(DataType::List(Box::new(DataType::Int32))),
                                "DOUBLE" => Ok(DataType::List(Box::new(DataType::Double))),
                                "FLOAT" => Ok(DataType::List(Box::new(DataType::Float))),
                                "STRING" => Ok(DataType::List(Box::new(DataType::String))),
                                "BOOL" => Ok(DataType::List(Box::new(DataType::Bool))),
                                _ => Ok(DataType::List(Box::new(DataType::String))),
                            }
                        } else {
                            Ok(DataType::List(Box::new(DataType::String)))
                        }
                    } else {
                        Ok(DataType::List(Box::new(DataType::String)))
                    }
                }
                s if s.contains("STRUCT") => {
                    // Handle STRUCT(a STRING, b BOOL) format
                    let inner = s.trim_start_matches("STRUCT(").trim_end_matches(")");
                    let mut fields = Vec::new();
                    let parts: Vec<&str> = inner.split(", ").collect();
                    for part in parts {
                        if let Some(space_idx) = part.find(' ') {
                            let name = part[..space_idx].to_string();
                            let type_str = part[space_idx + 1..].to_uppercase();
                            let data_type = match type_str.as_str() {
                                "INT64" => DataType::Int64,
                                "INT32" => DataType::Int32,
                                "DOUBLE" => DataType::Double,
                                "FLOAT" => DataType::Float,
                                "STRING" => DataType::String,
                                "BOOL" => DataType::Bool,
                                "DATE" => DataType::Date,
                                "TIMESTAMP" => DataType::Timestamp,
                                _ => DataType::String,
                            };
                            fields.push(ColumnDefinition { name, data_type });
                        }
                    }
                    Ok(DataType::Struct(fields))
                }
                _ => Ok(DataType::String),
            }
        }
        Rule::list_type => {
            let inner: Vec<_> = p.into_inner().collect();
            for item in &inner {
                if item.as_rule() == Rule::data_type {
                    return Ok(DataType::List(Box::new(parse_data_type(item.clone())?)));
                }
            }
            Ok(DataType::List(Box::new(DataType::String)))
        }
        Rule::struct_type => {
            let inner: Vec<_> = p.into_inner().collect();
            let mut fields = Vec::new();
            let mut name = None;
            for inner_item in inner {
                match inner_item.as_rule() {
                    Rule::variable => {
                        name = Some(inner_item.as_str().to_string());
                    }
                    Rule::data_type => {
                        if let Some(n) = name.take() {
                            fields.push(ColumnDefinition {
                                name: n,
                                data_type: parse_data_type(inner_item)?,
                            });
                        }
                    }
                    _ => {}
                }
            }
            Ok(DataType::Struct(fields))
        }
        _ => match p.as_str().to_uppercase().as_str() {
            "INT64" => Ok(DataType::Int64),
            "INT32" => Ok(DataType::Int32),
            "DOUBLE" => Ok(DataType::Double),
            "FLOAT" => Ok(DataType::Float),
            "STRING" => Ok(DataType::String),
            "BOOL" => Ok(DataType::Bool),
            "DATE" => Ok(DataType::Date),
            "TIMESTAMP" => Ok(DataType::Timestamp),
            _ => Ok(DataType::String),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_or_expression() {
        let query = "MATCH (t:Test) WHERE (t.val = 1 OR t.val = 3) RETURN count(*)";
        let result = parse(query);
        assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    }

    #[test]
    fn test_parse_in_expr() {
        let query = "MATCH (t:Test) WHERE t.val IN [1, 3] RETURN count(*)";
        let result = parse(query);
        assert!(result.is_ok(), "Failed to parse IN: {:?}", result.err());
    }

    #[test]
    fn test_parse_not_in_expr() {
        let query = "MATCH (t:Test) WHERE t.val NOT IN [1, 3] RETURN count(*)";
        let result = parse(query);
        assert!(result.is_ok(), "Failed to parse NOT IN: {:?}", result.err());
    }
}

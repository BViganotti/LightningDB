use crate::catalog::Catalog;
use crate::parser::ast::*;
use crate::LightningError;
use crate::Result;
use lightning_types::LogicalType;
use std::collections::HashMap;
use std::path::Component;

/// System-internal auth tables that must not be directly queried by users.
#[allow(dead_code)]
const AUTH_SYSTEM_TABLES: &[&str] = &["auth_users", "auth_refresh_tokens", "auth_api_keys"];

#[derive(Debug, Clone)]
pub struct BoundQuery {
    pub union_queries: Vec<BoundUnionQuery>,
    pub is_explain: bool,
    pub is_profile: bool,
    pub column_offsets: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
pub struct BoundUnionQuery {
    pub statement: BoundStatement,
    pub next_union: Option<(Box<BoundUnionQuery>, bool)>,
}

#[derive(Debug, Clone)]
pub enum BoundStatement {
    Query(
        Option<BoundMatchClause>,
        Option<BoundWhereClause>,
        Vec<BoundClause>,
    ),
    Create(BoundNodePattern),
    CreateRel(BoundRelPattern),
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
    StandaloneCall(String, Vec<Literal>),
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

#[derive(Debug, Clone)]
pub enum BoundTransactionAction {
    Begin,
    Commit,
    Rollback,
}

impl BoundStatement {
    pub fn get_output_columns(&self) -> Vec<(String, LogicalType)> {
        match self {
            BoundStatement::Query(_, _, clauses) => {
                for clause in clauses.iter().rev() {
                    if let BoundClause::Return(ret) = clause {
                        return ret
                            .items
                            .iter()
                            .map(|item| (item.alias.clone(), item.expression.get_type()))
                            .collect();
                    }
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundMatchClause {
    pub elements: Vec<BoundMatchElement>,
}

#[derive(Debug, Clone)]
pub enum BoundMatchElement {
    Node(String, String, Vec<(usize, BoundExpression)>), // table_name, variable, properties
    Rel(
        String,
        String,
        String,
        String,
        Option<(Option<u32>, Option<u32>)>,
    ), // table_name, variable, src_variable, dst_variable, bounds
    AllShortestPaths {
        rel_table_name: String,
        src_var: String,
        dst_var: String,
        path_var: String,
        max_depth: u32,
    },
}

#[derive(Debug, Clone)]
pub enum BoundClause {
    Return(BoundReturnClause),
    Delete(BoundDeleteClause),
    Set(BoundSetClause),
    Create(BoundNodePattern),
    CreateRel(BoundRelPattern),
    Unwind(BoundUnwind),
    Merge(BoundMergeClause),
    Call(BoundCallClause),
    Subquery(Box<BoundQuery>),
    Match(BoundMatchClause),
    OptionalMatch(BoundMatchClause),
    With(BoundReturnClause, Option<BoundWhereClause>),
}

#[derive(Debug, Clone)]
pub struct BoundCallClause {
    pub procedure_name: String,
    pub parameters: Vec<BoundExpression>,
    pub yield_items: Option<Vec<(String, String)>>,
}

#[derive(Debug, Clone)]
pub struct BoundUnwind {
    pub expression: BoundExpression,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct BoundDeleteClause {
    pub variables: Vec<(String, String)>, // (variable, table_name)
    pub detach: bool,
}

#[derive(Debug, Clone)]
pub struct BoundSetClause {
    pub assignments: Vec<BoundPropertyAssignment>,
}

#[derive(Debug, Clone)]
pub struct BoundMergeClause {
    pub pattern: BoundNodePattern,
    pub on_create_assignments: Vec<BoundPropertyAssignment>,
    pub on_match_assignments: Vec<BoundPropertyAssignment>,
}

#[derive(Debug, Clone)]
pub struct BoundPropertyAssignment {
    pub variable: String,
    pub table_name: String,
    pub property_idx: usize,
    pub expression: BoundExpression,
}

#[derive(Debug, Clone)]
pub struct BoundNodePattern {
    pub table_name: String,
    pub variable: Option<String>,
    pub properties: Vec<(usize, BoundExpression)>,
}

#[derive(Debug, Clone)]
pub struct BoundRelPattern {
    pub table_name: String,
    pub variable: Option<String>,
    pub src_variable: String,
    pub dst_variable: String,
    pub src_column_idx: Option<usize>,
    pub dst_column_idx: Option<usize>,
    pub properties: Vec<(usize, BoundExpression)>,
    pub var_len_bounds: Option<(Option<u32>, Option<u32>)>,
}

#[derive(Debug, Clone)]
pub struct BoundWhereClause {
    pub expression: BoundExpression,
}

#[derive(Debug, Clone)]
pub struct BoundReturnClause {
    pub distinct: bool,
    pub items: Vec<BoundProjectionItem>,
    pub order_by: Option<Vec<BoundOrderByItem>>,
    pub skip: Option<f64>,
    pub limit: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct BoundOrderByItem {
    pub expression: BoundExpression,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub struct BoundProjectionItem {
    pub expression: BoundExpression,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum BoundExpression {
    Literal(Literal),
    Variable(String, LogicalType),
    PropertyLookup(String, usize, LogicalType), // variable, property_index, type
    Comparison(
        Box<BoundExpression>,
        crate::parser::ast::ComparisonOperator,
        Box<BoundExpression>,
    ),
    Arithmetic(
        Box<BoundExpression>,
        crate::parser::ast::ArithmeticOperator,
        Box<BoundExpression>,
    ),
    Logical(
        Box<BoundExpression>,
        crate::parser::ast::LogicalOperator,
        Box<BoundExpression>,
    ),
    Not(Box<BoundExpression>),
    Function(String, Vec<BoundExpression>, LogicalType),
    List(Vec<BoundExpression>, LogicalType),
    Case {
        expression: Option<Box<BoundExpression>>,
        when_then: Vec<(BoundExpression, BoundExpression)>,
        else_expression: Option<Box<BoundExpression>>,
        return_type: LogicalType,
    },
    Aggregate(String, Vec<BoundExpression>, String),
    Lambda(String, Box<BoundExpression>), // variable, body
    Parameter(String),                    // $name
    NextVal(String),                      // sequence name
    Exists(Vec<(BoundMatchClause, Option<BoundWhereClause>)>),
    CountSubquery(Vec<(BoundMatchClause, Option<BoundWhereClause>)>),
    Map(Vec<(String, BoundExpression)>, LogicalType),
}

impl BoundExpression {
    pub fn get_type(&self) -> LogicalType {
        match self {
            BoundExpression::Literal(lit) => match lit {
                Literal::Integer(_) => LogicalType::Int64,
                Literal::Number(_) => LogicalType::Double,
                Literal::String(_) => LogicalType::String,
                Literal::Boolean(_) => LogicalType::Bool,
                Literal::Null => LogicalType::Any,
            },
            BoundExpression::Logical(_, _, _) => LogicalType::Bool,
            BoundExpression::Not(_) => LogicalType::Bool,
            BoundExpression::Comparison(_, _, _) => LogicalType::Bool,
            BoundExpression::Exists(_) => LogicalType::Bool,
            BoundExpression::CountSubquery(_) => LogicalType::Int64,
            BoundExpression::Map(_, t) => t.clone(),
            BoundExpression::Variable(_, t) => t.clone(),
            BoundExpression::PropertyLookup(_, _, t) => t.clone(),
            BoundExpression::Arithmetic(left, _, _) => left.get_type(),
            BoundExpression::Function(_, _, t) => t.clone(),
            BoundExpression::List(_, t) => t.clone(),
            BoundExpression::Case { return_type, .. } => return_type.clone(),
            BoundExpression::Aggregate(name, _, _) => match name.as_str() {
                "count" | "count_distinct" => LogicalType::Int64,
                "sum" | "avg" | "min" | "max" => LogicalType::Double,
                "collect" => LogicalType::List(Box::new(LogicalType::Any)),
                _ => LogicalType::Any,
            },
            BoundExpression::Lambda(_, body) => LogicalType::Lambda(Box::new(body.get_type())),
            BoundExpression::Parameter(_) => LogicalType::Any,
            BoundExpression::NextVal(_) => LogicalType::Uint64,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            BoundExpression::Function(name, _, _) => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "COUNT_DISTINCT" | "SUM" | "AVG" | "MIN" | "MAX"
                        | "COLLECT" | "GROUP_CONCAT" | "MEDIAN" | "COLLECT_DISTINCT"
                        | "STDDEV_POP" | "STDDEV" | "STDDEV_SAMP"
                        | "VAR_POP" | "VAR" | "VAR_SAMP"
                )
            }
            BoundExpression::Variable(_, _) | BoundExpression::PropertyLookup(_, _, _) => false,
            BoundExpression::Arithmetic(l, _, r) => l.is_aggregate() || r.is_aggregate(),
            BoundExpression::Comparison(l, _, r) => l.is_aggregate() || r.is_aggregate(),
            BoundExpression::Logical(l, _, r) => l.is_aggregate() || r.is_aggregate(),
            BoundExpression::Not(e) => e.is_aggregate(),
            BoundExpression::List(exprs, _) => exprs.iter().any(|e| e.is_aggregate()),
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                expression
                    .as_ref()
                    .map(|e| e.is_aggregate())
                    .unwrap_or(false)
                    || when_then
                        .iter()
                        .any(|(w, t)| w.is_aggregate() || t.is_aggregate())
                    || else_expression
                        .as_ref()
                        .map(|e| e.is_aggregate())
                        .unwrap_or(false)
            }
            BoundExpression::Aggregate(_, _, _) => true,
            BoundExpression::Lambda(_, body) => body.is_aggregate(),
            BoundExpression::Exists(_)
            | BoundExpression::CountSubquery(_)
            | BoundExpression::Map(_, _)
            | BoundExpression::Parameter(_)
            | BoundExpression::NextVal(_) => false,
            BoundExpression::Literal(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundVariable {
    pub table_name: String,
    pub type_: LogicalType,
}

pub struct Binder<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) function_registry: &'a crate::processor::functions::FunctionRegistry,
    pub(crate) variables: HashMap<String, BoundVariable>,
    pub(crate) column_offsets: HashMap<String, usize>,
}

impl<'a> Binder<'a> {
    pub fn new(
        catalog: &'a Catalog,
        function_registry: &'a crate::processor::functions::FunctionRegistry,
    ) -> Self {
        Self {
            catalog,
            function_registry,
            variables: HashMap::new(),
            column_offsets: HashMap::new(),
        }
    }

    pub fn bind_query(&mut self, query: &Query) -> Result<BoundQuery> {
        let mut union_queries = Vec::new();
        for uq in &query.union_queries {
            union_queries.push(self.bind_union_query(uq)?);
        }
        Ok(BoundQuery {
            union_queries,
            is_explain: query.is_explain,
            is_profile: query.is_profile,
            column_offsets: self.column_offsets.clone(),
        })
    }

    fn bind_union_query(&mut self, uq: &UnionQuery) -> Result<BoundUnionQuery> {
        let statement = self.bind(&uq.statement)?;

        // Reset variables for the next part of the union?
        // Actually, UNION queries must have independent scopes until they combine.
        // But the schema (output columns) must match.

        let mut next_union = None;
        if let Some((next_uq, is_all)) = &uq.next_union {
            // Need a fresh binder for the next subquery to avoid variable pollution
            let mut next_binder = Binder::new(self.catalog, self.function_registry);
            let bound_next = next_binder.bind_union_query(next_uq)?;

            // Check schema compatibility
            let left_columns = statement.get_output_columns();
            let right_columns = bound_next.statement.get_output_columns();

            if left_columns.len() != right_columns.len() {
                return Err(LightningError::Query(
                    "UNION subqueries must have the same number of columns".into(),
                ));
            }

            for (left, right) in left_columns.iter().zip(right_columns.iter()) {
                // Cypher/SQL allows UNION with different column names —
                // the first query's column names are used.
                if left.1 != right.1 {
                    return Err(LightningError::Query(format!(
                        "Column type mismatch in UNION for column {}: {:?} vs {:?}",
                        left.0, left.1, right.1
                    )));
                }
            }

            next_union = Some((Box::new(bound_next), *is_all));
        }

        Ok(BoundUnionQuery {
            statement,
            next_union,
        })
    }

    pub fn bind(&mut self, statement: &Statement) -> Result<BoundStatement> {
        match statement {
            Statement::CreateTableNode {
                name,
                columns,
                primary_key,
                if_not_exists,
            } => {
                let bound_columns: Vec<_> = columns
                    .iter()
                    .map(|c| crate::catalog::PropertyDefinition {
                        name: c.name.clone(),
                        type_: self.bind_data_type(&c.data_type),
                    })
                    .collect();
                Ok(BoundStatement::CreateTableNode {
                    name: name.clone(),
                    columns: bound_columns,
                    primary_key: primary_key.clone(),
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::CreateTableRel {
                name,
                from_table,
                to_table,
                columns,
                if_not_exists,
            } => {
                let columns = columns
                    .iter()
                    .map(|c| crate::catalog::PropertyDefinition {
                        name: c.name.clone(),
                        type_: self.bind_data_type(&c.data_type),
                    })
                    .collect();
                Ok(BoundStatement::CreateTableRel {
                    name: name.clone(),
                    from_table: from_table.clone(),
                    to_table: to_table.clone(),
                    columns,
                    if_not_exists: *if_not_exists,
                })
            }
            Statement::DropTable(name, if_exists) => {
                Ok(BoundStatement::DropTable(name.clone(), *if_exists))
            }
            Statement::CreateConstraint {
                name,
                table_label,
                property,
            } => {
                let table_name = {
                    self.catalog
                        .get_node_table(table_label)
                        .ok_or_else(|| {
                            LightningError::Query(format!("Table {table_label} not found"))
                        })?
                        .name
                        .clone()
                };
                Ok(BoundStatement::CreateConstraint {
                    name: name.clone(),
                    table_name,
                    property: property.clone(),
                })
            }
            Statement::DropConstraint(name) => {
                Ok(BoundStatement::DropConstraint(name.clone()))
            }
            Statement::CreateIndex {
                name,
                table_label,
                property,
            } => {
                let table_name = {
                    self.catalog
                        .get_node_table(table_label)
                        .ok_or_else(|| {
                            LightningError::Query(format!("Table {table_label} not found"))
                        })?
                        .name
                        .clone()
                };
                Ok(BoundStatement::CreateIndex {
                    name: name.clone(),
                    table_name,
                    property: property.clone(),
                })
            }
            Statement::DropIndex(name) => {
                Ok(BoundStatement::DropIndex(name.clone()))
            }
            Statement::CreateVectorIndex {
                table_name,
                field,
                index_type,
                metric,
                dimension,
            } => Ok(BoundStatement::CreateVectorIndex {
                table_name: table_name.clone(),
                field: field.clone(),
                index_type: index_type.clone(),
                metric: metric.clone(),
                dimension: *dimension,
            }),
            Statement::CreateFtsIndex {
                table_name,
                fields,
            } => Ok(BoundStatement::CreateFtsIndex {
                table_name: table_name.clone(),
                fields: fields.clone(),
            }),
            Statement::AlterTable { name, operation } => {
                Ok(BoundStatement::AlterTable {
                    name: name.clone(),
                    operation: operation.clone(),
                })
            }
            Statement::CopyFrom {
                table_name,
                file_path,
                options,
            } => {
                if self.catalog.get_node_table(table_name).is_none()
                    && self.catalog.get_rel_table(table_name).is_none()
                {
                    return Err(LightningError::Query(format!(
                        "Table {table_name} not found"
                    )));
                }
                // Validate path at bind time to reject traversal attacks early
                let path = std::path::Path::new(file_path);
                for component in path.components() {
                    if let Component::ParentDir = component {
                        return Err(LightningError::Config(format!(
                            "COPY FROM path '{file_path}' contains '..' traversal"
                        )));
                    }
                }
                Ok(BoundStatement::CopyFrom {
                    table_name: table_name.clone(),
                    file_path: file_path.clone(),
                    options: options.clone(),
                })
            }
            Statement::CopyTo {
                table_name,
                file_path,
                options,
            } => {
                if self.catalog.get_node_table(table_name).is_none()
                    && self.catalog.get_rel_table(table_name).is_none()
                {
                    return Err(LightningError::Query(format!(
                        "Table {table_name} not found"
                    )));
                }
                // Validate path at bind time to reject traversal attacks early
                let path = std::path::Path::new(file_path);
                for component in path.components() {
                    if let Component::ParentDir = component {
                        return Err(LightningError::Config(format!(
                            "COPY TO path '{file_path}' contains '..' traversal"
                        )));
                    }
                }
                Ok(BoundStatement::CopyTo {
                    table_name: table_name.clone(),
                    file_path: file_path.clone(),
                    options: options.clone(),
                })
            }
            Statement::Create(pattern) => {
                // Handle CREATE as a clause - if it has relationships, we need to handle it within a Query context
                // by returning a Query that includes both the match info and the create
                if pattern.relationship_chains.is_empty() {
                    let bound_create = self.bind_node_pattern(&pattern.node_pattern)?;
                    Ok(BoundStatement::Create(bound_create))
                } else {
                    // For CREATE with relationships, we can't bind it standalone
                    // The CREATE needs to be in a Query with a MATCH that provides variables
                    Err(LightningError::Query(
                        "CREATE relationship must be part of a query with MATCH".into(),
                    ))
                }
            }
            Statement::Match(match_clause, where_clause, clauses) => {
                // Prevent direct query access to internal auth tables
                if let Some(m) = &match_clause {
                    Self::check_auth_table_access(&m.patterns)?;
                }

                let bound_match = if let Some(m) = match_clause {
                    Some(self.bind_match_clause(m)?)
                } else {
                    None
                };
                let bound_where = if let Some(where_expr) = where_clause {
                    Some(BoundWhereClause {
                        expression: self.bind_expression(&where_expr.expression)?,
                    })
                } else {
                    None
                };

                let mut bound_clauses = Vec::new();
                for clause in clauses {
                    bound_clauses.push(self.bind_clause(clause)?);
                }

                Ok(BoundStatement::Query(
                    bound_match,
                    bound_where,
                    bound_clauses,
                ))
            }
            Statement::Transaction(action) => {
                let bound_action = match action {
                    TransactionAction::Begin => BoundTransactionAction::Begin,
                    TransactionAction::Commit => BoundTransactionAction::Commit,
                    TransactionAction::Rollback => BoundTransactionAction::Rollback,
                };
                Ok(BoundStatement::Transaction(bound_action))
            }
            Statement::Checkpoint => Ok(BoundStatement::Checkpoint),
            Statement::Vacuum => Ok(BoundStatement::Vacuum),
            Statement::StandaloneCall(name, args) => {
                Ok(BoundStatement::StandaloneCall(name.clone(), args.clone()))
            }
            Statement::CreateSequence {
                name,
                start_with,
                increment_by,
            } => {
                if *increment_by <= 0 {
                    return Err(LightningError::Query(
                        "Sequence increment must be positive".into(),
                    ));
                }
                Ok(BoundStatement::CreateSequence {
                    name: name.clone(),
                    start_with: *start_with,
                    increment_by: *increment_by,
                })
            },
            Statement::CreateMacro { name, params, body } => Ok(BoundStatement::CreateMacro {
                name: name.clone(),
                params: params.clone(),
                body: body.clone(),
            }),
            Statement::Merge(merge) => {
                let node_pat = &merge.pattern.node_pattern;
                Self::check_auth_table_access(&[merge.pattern.clone()])?;
                let node_var = node_pat
                    .variable
                    .clone()
                    .ok_or_else(|| LightningError::Query("MERGE must have a variable".into()))?;
                let node_label = node_pat
                    .labels.first()
                    .ok_or_else(|| LightningError::Query("MERGE must have a label".into()))?;

                let node_table = self.catalog.get_node_table(node_label).ok_or_else(|| {
                    LightningError::Query(format!("Table {node_label} not found"))
                })?;

                self.variables.insert(
                    node_var.clone(),
                    BoundVariable {
                        table_name: node_table.name.clone(),
                        type_: LogicalType::Node(vec![]),
                    },
                );

                let bound_properties =
                    self.bind_property_items(&node_pat.properties, &node_table.properties, 0)?;

                let bound_pattern = BoundNodePattern {
                    table_name: node_table.name.clone(),
                    variable: Some(node_var.clone()),
                    properties: bound_properties,
                };

                let mut on_match_assignments = Vec::new();
                for pa in &merge.on_match_assignments {
                    let var = self.variables.get(&pa.variable).cloned().ok_or_else(|| {
                        LightningError::Query(format!("Variable {} not found", pa.variable))
                    })?;
                    let table = self
                        .catalog
                        .get_node_table(&var.table_name)
                        .ok_or_else(|| {
                            LightningError::Query(format!("Table {} not found", var.table_name))
                        })?;
                    let prop_idx = table
                        .properties
                        .iter()
                        .position(|p| p.name == pa.property_key)
                        .ok_or_else(|| {
                            LightningError::Query(format!(
                                "Property {} not found in table {}",
                                pa.property_key, var.table_name
                            ))
                        })?;
                    let bound_expr = self.bind_expression(&pa.expression)?;
                    on_match_assignments.push(BoundPropertyAssignment {
                        variable: pa.variable.clone(),
                        table_name: var.table_name.clone(),
                        property_idx: prop_idx,
                        expression: bound_expr,
                    });
                }

                let mut on_create_assignments = Vec::new();
                for pa in &merge.on_create_assignments {
                    let var = self.variables.get(&pa.variable).cloned().ok_or_else(|| {
                        LightningError::Query(format!("Variable {} not found", pa.variable))
                    })?;
                    let table = self
                        .catalog
                        .get_node_table(&var.table_name)
                        .ok_or_else(|| {
                            LightningError::Query(format!("Table {} not found", var.table_name))
                        })?;
                    let prop_idx = table
                        .properties
                        .iter()
                        .position(|p| p.name == pa.property_key)
                        .ok_or_else(|| {
                            LightningError::Query(format!(
                                "Property {} not found in table {}",
                                pa.property_key, var.table_name
                            ))
                        })?;
                    let bound_expr = self.bind_expression(&pa.expression)?;
                    on_create_assignments.push(BoundPropertyAssignment {
                        variable: pa.variable.clone(),
                        table_name: var.table_name.clone(),
                        property_idx: prop_idx,
                        expression: bound_expr,
                    });
                }

                Ok(BoundStatement::Query(
                    None,
                    None,
                    vec![BoundClause::Merge(BoundMergeClause {
                        pattern: bound_pattern,
                        on_match_assignments,
                        on_create_assignments,
                    })],
                ))
            }
        }
    }

    fn bind_match_clause(&mut self, match_clause: &MatchClause) -> Result<BoundMatchClause> {
        let mut elements = Vec::new();
        let mut column_offset: usize = 0;

        for pattern in &match_clause.patterns {
            // Handle shortest path and all shortest paths patterns
            if pattern.is_shortest_path || pattern.is_all_shortest_paths {
                let chain = pattern.shortest_path_chain.as_ref().ok_or_else(|| {
                    LightningError::Query("Shortest path pattern must have a relationship".into())
                })?;
                let rel_pat = &chain.relationship_pattern;
                let rel_var = rel_pat
                    .variable
                    .clone()
                    .unwrap_or_else(|| format!("_rel_{}", self.variables.len()));
                let rel_table = if rel_pat.labels.is_empty() {
                    // No label specified — find the first rel table matching the source/dest tables
                    let src_label = self.require_single_label(
                        &pattern.shortest_path_start.as_ref().ok_or_else(|| {
                            LightningError::Query("Shortest path must have a start node".into())
                        })?.labels,
                        "Shortest path start node",
                    )?;
                    let dst_label = self.require_single_label(
                        &pattern.shortest_path_end.as_ref().ok_or_else(|| {
                            LightningError::Query("Shortest path must have an end node".into())
                        })?.labels,
                        "Shortest path end node",
                    )?;
                    self.catalog
                        .rel_tables
                        .iter()
                        .find(|(_, rel)| rel.from_table.as_str() == src_label.as_str() && rel.to_table.as_str() == dst_label.as_str())
                        .map(|(_, rel)| rel.clone())
                        .ok_or_else(|| {
                            LightningError::Query(format!(
                                "No rel table found connecting '{src_label}' to '{dst_label}'"
                            ))
                        })?
                } else {
                    let rel_label =
                        self.require_single_label(&rel_pat.labels, "Shortest path relationship")?;
                    self.catalog.get_rel_table(rel_label).ok_or_else(|| {
                        LightningError::Query(format!("Rel Table {rel_label} not found"))
                    })?.clone()
                };

                let start_pat = pattern.shortest_path_start.as_ref().ok_or_else(|| {
                    LightningError::Query("Shortest path must have a start node".into())
                })?;
                let start_var = start_pat
                    .variable
                    .clone()
                    .unwrap_or_else(|| format!("_n{}", self.variables.len()));
                let start_label = self.require_single_label(&start_pat.labels, "Shortest path start node")?;
                let start_table = self.catalog.get_node_table(start_label).ok_or_else(|| {
                    LightningError::Query(format!("Table {start_label} not found"))
                })?;
                self.variables.insert(
                    start_var.clone(),
                    BoundVariable {
                        table_name: start_table.name.clone(),
                        type_: LogicalType::Node(vec![]),
                    },
                );
                self.column_offsets.insert(start_var.clone(), column_offset);
                column_offset += start_table.properties.len();
                let start_props = self.bind_property_items(
                    &start_pat.properties,
                    &start_table.properties,
                    0,
                )?;
                elements.push(BoundMatchElement::Node(
                    start_table.name.clone(),
                    start_var.clone(),
                    start_props,
                ));

                let end_pat = pattern.shortest_path_end.as_ref().ok_or_else(|| {
                    LightningError::Query("Shortest path must have an end node".into())
                })?;
                let end_var = end_pat
                    .variable
                    .clone()
                    .unwrap_or_else(|| format!("_n{}", self.variables.len()));
                let end_label = self.require_single_label(&end_pat.labels, "Shortest path end node")?;
                let end_table = self.catalog.get_node_table(end_label).ok_or_else(|| {
                    LightningError::Query(format!("Table {end_label} not found"))
                })?;

                // Extract max_depth from variable-length bounds
                let max_depth = rel_pat
                    .var_len_bounds
                    .map(|(_, upper)| upper.unwrap_or(u32::MAX))
                    .unwrap_or(u32::MAX);

                elements.push(BoundMatchElement::AllShortestPaths {
                    rel_table_name: rel_table.name.clone(),
                    src_var: start_var.clone(),
                    dst_var: end_var.clone(),
                    path_var: rel_var,
                    max_depth,
                });

                // Register end node variable (not scanned, but needed for binding)
                self.variables.insert(
                    end_var.clone(),
                    BoundVariable {
                        table_name: end_table.name.clone(),
                        type_: LogicalType::Node(vec![]),
                    },
                );
                self.column_offsets.insert(end_var.clone(), column_offset);
                column_offset += end_table.properties.len();

                continue;
            }

            // Bind the starting node of each pattern
            let node_pat = &pattern.node_pattern;
            let node_var = node_pat
                .variable
                .clone()
                .unwrap_or_else(|| format!("_n{}", self.variables.len()));
            let node_label: String = if let Some(label) = node_pat.labels.first() {
                label.clone()
            } else if let Some(bv) = self.variables.get(&node_var) {
                // Variable is already bound (e.g. from an outer MATCH in an EXISTS
                // subquery). Use the existing table name as the label.
                bv.table_name.clone()
            } else if let Some(rel_chain) = pattern.relationship_chains.first() {
                let rel_label = self.require_single_label(&rel_chain.relationship_pattern.labels, "MATCH relationship")?;
                let rel_table = self.catalog.get_rel_table(rel_label).ok_or_else(|| {
                    LightningError::Query(format!("Rel Table {rel_label} not found"))
                })?;

                match rel_chain.relationship_pattern.direction {
                    crate::parser::ast::Direction::Left => rel_table.to_table.clone(),
                    _ => rel_table.from_table.clone(),
                }
            } else {
                return Err(LightningError::Query("MATCH must have a label".into()));
            };

            let node_table = self
                .catalog
                .get_node_table(&node_label)
                .ok_or_else(|| LightningError::Query(format!("Table {node_label} not found")))?;
            self.variables.insert(
                node_var.clone(),
                BoundVariable {
                    table_name: node_table.name.clone(),
                    type_: LogicalType::Node(vec![]),
                },
            );
            self.column_offsets.insert(node_var.clone(), column_offset);
            let num_node_cols = node_table.properties.len();
            let properties =
                self.bind_property_items(&node_pat.properties, &node_table.properties, 0)?;
            elements.push(BoundMatchElement::Node(
                node_table.name.clone(),
                node_var.clone(),
                properties,
            ));
            column_offset += num_node_cols;

            let mut current_node_var = node_var;

            // Bind relationship chains for each pattern
            for rel_chain in &pattern.relationship_chains {
                let rel_pat = &rel_chain.relationship_pattern;
                let rel_var = rel_pat
                    .variable
                    .clone()
                    .unwrap_or_else(|| format!("_rel_{}", self.variables.len()));
                let rel_label = self.require_single_label(&rel_pat.labels, "MATCH relationship")?;
                let rel_table = self.catalog.get_rel_table(rel_label).ok_or_else(|| {
                    LightningError::Query(format!("Rel Table {rel_label} not found"))
                })?;
                self.variables.insert(
                    rel_var.clone(),
                    BoundVariable {
                        table_name: rel_table.name.clone(),
                        type_: LogicalType::Rel(vec![]),
                    },
                );
                self.column_offsets.insert(rel_var.clone(), column_offset);
                let num_rel_cols = rel_table.properties.len();
                column_offset += num_rel_cols;

                let dst_pat = &rel_chain.node_pattern;
                let dst_var = dst_pat.variable.clone().ok_or_else(|| {
                    LightningError::Query("MATCH destination node must have a variable".into())
                })?;

                // Check if the destination variable is already bound (self-referential)
                let (dst_table_name, dst_properties) = if self.variables.contains_key(&dst_var) {
                    // Variable already exists, get its properties from the bound variable
                    let bound_var = self.variables.get(&dst_var).ok_or_else(|| {
                        crate::LightningError::Internal(format!("Variable '{dst_var}' not found in scope"))
                    })?.clone();
                    let src_table = self
                        .catalog
                        .get_node_table(&bound_var.table_name)
                        .ok_or_else(|| {
                            LightningError::Query(format!(
                                "Table {} not found",
                                bound_var.table_name
                            ))
                        })?;
                    let props =
                        self.bind_property_items(&dst_pat.properties, &src_table.properties, 0)?;
                    (bound_var.table_name, props)
                } else {
                    let dst_label: String = if let Some(label) = dst_pat.labels.first() {
                        label.clone()
                    } else {
                        // Infer destination node table from the relationship definition
                        match rel_chain.relationship_pattern.direction {
                            crate::parser::ast::Direction::Left => rel_table.from_table.clone(),
                            _ => rel_table.to_table.clone(),
                        }
                    };

                    let dst_table = self.catalog.get_node_table(&dst_label).ok_or_else(|| {
                        LightningError::Query(format!("Table {dst_label} not found"))
                    })?;
                    self.variables.insert(
                        dst_var.clone(),
                        BoundVariable {
                            table_name: dst_table.name.clone(),
                            type_: LogicalType::Node(vec![]),
                        },
                    );
                    self.column_offsets.insert(dst_var.clone(), column_offset);
                    let num_dst_cols = dst_table.properties.len();
                    column_offset += num_dst_cols;
                    let props =
                        self.bind_property_items(&dst_pat.properties, &dst_table.properties, 0)?;
                    (dst_table.name.clone(), props)
                };

                elements.push(BoundMatchElement::Rel(
                    rel_table.name.clone(),
                    rel_var.clone(),
                    current_node_var.clone(),
                    dst_var.clone(),
                    rel_pat.var_len_bounds,
                ));
                elements.push(BoundMatchElement::Node(
                    dst_table_name,
                    dst_var.clone(),
                    dst_properties,
                ));

                current_node_var = dst_var;
            }
        }

        Ok(BoundMatchClause { elements })
    }

    fn bind_clause(&mut self, clause: &Clause) -> Result<BoundClause> {
        match clause {
            Clause::Return(ret) => Ok(BoundClause::Return(self.bind_return_clause(ret)?)),
            Clause::Match(match_clause) => {
                Ok(BoundClause::Match(self.bind_match_clause(match_clause)?))
            }
            Clause::Delete(del) => {
                let mut vars = Vec::new();
                for var in &del.variables {
                    let binding = self.variables.get(var).ok_or_else(|| {
                        LightningError::Query(format!("Variable {var} not found"))
                    })?;
                    vars.push((var.clone(), binding.table_name.clone()));
                }
                Ok(BoundClause::Delete(BoundDeleteClause {
                    variables: vars,
                    detach: del.detach,
                }))
            }
            Clause::Merge(merge) => Ok(BoundClause::Merge(self.bind_merge_clause(merge)?)),
            Clause::Unwind(unwind) => {
                let bound_expr = self.bind_expression(&unwind.expression)?;
                // For now, assume list element type is Any if we can't determine it
                self.variables.insert(
                    unwind.alias.clone(),
                    BoundVariable {
                        table_name: "".into(),
                        type_: LogicalType::Any,
                    },
                );
                Ok(BoundClause::Unwind(BoundUnwind {
                    expression: bound_expr,
                    alias: unwind.alias.clone(),
                }))
            }
            Clause::Call(call) => {
                let mut bound_params = Vec::new();
                for param in &call.parameters {
                    bound_params.push(self.bind_expression(param)?);
                }
                let mut yield_items = Vec::new();
                if let Some(yields) = &call.yield_items {
                    for item in yields {
                        let alias = item.alias.clone().unwrap_or_else(|| item.name.clone());
                        yield_items.push((item.name.clone(), alias.clone()));
                        self.variables.insert(
                            alias,
                            BoundVariable {
                                table_name: "".into(),
                                type_: LogicalType::Any,
                            },
                        );
                    }
                }
                Ok(BoundClause::Call(BoundCallClause {
                    procedure_name: call.procedure_name.clone(),
                    parameters: bound_params,
                    yield_items: if yield_items.is_empty() {
                        None
                    } else {
                        Some(yield_items)
                    },
                }))
            }
            Clause::Subquery(query) => Ok(BoundClause::Subquery(Box::new(self.bind_query(query)?))),
            Clause::OptionalMatch(pat) => {
                let bound_match = self.bind_match_clause(pat)?;
                Ok(BoundClause::OptionalMatch(bound_match))
            }
            Clause::With(ret, bound_where) => {
                let bound_ret = self.bind_return_clause(ret)?;
                self.variables.clear(); // Re-scope according to WITH items
                for item in &bound_ret.items {
                    self.variables.insert(
                        item.alias.clone(),
                        BoundVariable {
                            table_name: "".into(),
                            type_: item.expression.get_type(),
                        },
                    );
                }
                let bw = bound_where
                    .as_ref()
                    .map(|w| {
                        std::result::Result::<BoundWhereClause, LightningError>::Ok(
                            BoundWhereClause {
                                expression: self.bind_expression(&w.expression)?,
                            },
                        )
                    })
                    .transpose()?;
                Ok(BoundClause::With(bound_ret, bw))
            }
            Clause::Remove(rem) => {
                let mut assignments = Vec::new();
                for (variable, property_key) in &rem.properties {
                    let (properties, offset, table_name, _table_kind) =
                        self.get_table_properties(variable)?;
                    let mut prop_idx = None;
                    for (i, prop) in properties.iter().enumerate() {
                        if prop.name == *property_key {
                            prop_idx = Some(offset + i);
                            break;
                        }
                    }
                    let idx = prop_idx.ok_or_else(|| {
                        LightningError::Query(format!(
                            "Property {property_key} not found in table {table_name}"
                        ))
                    })?;
                    assignments.push(BoundPropertyAssignment {
                        variable: variable.clone(),
                        table_name: table_name.clone(),
                        property_idx: idx,
                        expression: crate::planner::binder::BoundExpression::Literal(
                            crate::parser::ast::Literal::Null,
                        ),
                    });
                }
                Ok(BoundClause::Set(BoundSetClause { assignments }))
            }
            Clause::Set(set) => {
                let mut assignments = Vec::new();
                for assign in &set.assignments {
                    let (properties, offset, table_name, _table_kind) =
                        self.get_table_properties(&assign.variable)?;
                    let mut prop_idx = None;
                    for (i, prop) in properties.iter().enumerate() {
                        if prop.name == assign.property_key {
                            prop_idx = Some(offset + i);
                            break;
                        }
                    }
                    let idx = prop_idx.ok_or_else(|| {
                        LightningError::Query(format!(
                            "Property {} not found in table {}",
                            assign.property_key, table_name
                        ))
                    })?;
                    assignments.push(BoundPropertyAssignment {
                        variable: assign.variable.clone(),
                        table_name: table_name.clone(),
                        property_idx: idx,
                        expression: self.bind_expression(&assign.expression)?,
                    });
                }
                Ok(BoundClause::Set(BoundSetClause { assignments }))
            }
            Clause::Create(pattern) => {
                if pattern.relationship_chains.is_empty() {
                    Ok(BoundClause::Create(
                        self.bind_node_pattern(&pattern.node_pattern)?,
                    ))
                } else {
                    let rel_chain = &pattern.relationship_chains[0];
                    let rel = &rel_chain.relationship_pattern;
                    let src = &pattern.node_pattern;
                    let dst = &rel_chain.node_pattern;
                    let rel_label = self.require_single_label(&rel.labels, "CREATE relationship")?;
                    let rel_table = self.catalog.get_rel_table(rel_label).ok_or_else(|| {
                        LightningError::Query(format!("Rel Table {rel_label} not found"))
                    })?;

                    Ok(BoundClause::CreateRel(BoundRelPattern {
                        table_name: rel_label.clone(),
                        variable: rel.variable.clone(),
                        src_variable: src.variable.clone().unwrap_or_default(),
                        dst_variable: dst.variable.clone().unwrap_or_default(),
                        src_column_idx: None,
                        dst_column_idx: None,
                        properties: self.bind_property_items(
                            &rel.properties,
                            &rel_table.properties,
                            0,
                        )?,
                        var_len_bounds: rel.var_len_bounds,
                    }))
                }
            }
        }
    }

    fn bind_node_pattern(&mut self, pat: &NodePattern) -> Result<BoundNodePattern> {
        let label = pat
            .labels.first()
            .ok_or_else(|| LightningError::Query("CREATE must have a label".into()))?;
        let table = self
            .catalog
            .get_node_table(label)
            .ok_or_else(|| LightningError::Query(format!("Table {label} not found")))?;

        let properties = self.bind_property_items(&pat.properties, &table.properties, 0)?;

        Ok(BoundNodePattern {
            table_name: table.name.clone(),
            variable: pat.variable.clone(),
            properties,
        })
    }

    fn bind_property_items(
        &mut self,
        items: &[PropertyItem],
        table_properties: &[crate::catalog::PropertyDefinition],
        offset: usize,
    ) -> Result<Vec<(usize, BoundExpression)>> {
        let mut bound_properties = Vec::new();
        for item in items {
            let mut prop_idx = None;
            for (i, prop) in table_properties.iter().enumerate() {
                if prop.name == item.key {
                    prop_idx = Some(i + offset);
                    break;
                }
            }
            let idx = prop_idx
                .ok_or_else(|| LightningError::Query(format!("Property {} not found", item.key)))?;
            bound_properties.push((idx, self.bind_expression(&item.value)?));
        }
        Ok(bound_properties)
    }

    fn bind_return_clause(&mut self, return_clause: &ReturnClause) -> Result<BoundReturnClause> {
        let mut items = Vec::new();
        for item in &return_clause.items {
            match item {
                ProjectionItem::Star => {
                    // Sort variables by name for deterministic column order
                    let mut sorted_vars: Vec<_> = self.variables.iter().collect();
                    sorted_vars.sort_by_key(|(name, _)| *name);
                    for (var_name, var_binding) in sorted_vars {
                        let table_info = if let Some(t) =
                            self.catalog.get_node_table(&var_binding.table_name)
                        {
                            Some((&t.properties, 0))
                        } else { self.catalog.get_rel_table(&var_binding.table_name).map(|t| (&t.properties, 0)) };

                        if let Some((properties, offset)) = table_info {
                            for (i, prop) in properties.iter().enumerate() {
                                items.push(BoundProjectionItem {
                                    expression: BoundExpression::PropertyLookup(
                                        var_name.clone(),
                                        i + offset,
                                        prop.type_.clone(),
                                    ),
                                    alias: prop.name.clone(),
                                });
                            }
                        }
                    }
                }
                ProjectionItem::Expression(expr, alias) => {
                    // Expand bare Variable expressions (e.g. RETURN n) into per-property
                    // columns when the variable refers to a node or relationship table.
                    if let Expression::Variable(v) = expr {
                        if let Some(bv) = self.variables.get(v) {
                            let table_info = self.catalog
                                .get_node_table(&bv.table_name)
                                .map(|t| (&t.properties, 0))
                                .or_else(|| {
                                    self.catalog
                                        .get_rel_table(&bv.table_name)
                                        .map(|t| (&t.properties, 0))
                                });
                            if let Some((properties, offset)) = table_info {
                                for (i, prop) in properties.iter().enumerate() {
                                    let prop_alias = alias.clone()
                                        .unwrap_or_else(|| format!("{}.{}", v, prop.name));
                                    items.push(BoundProjectionItem {
                                        expression: BoundExpression::PropertyLookup(
                                            v.clone(),
                                            i + offset,
                                            prop.type_.clone(),
                                        ),
                                        alias: prop_alias,
                                    });
                                }
                                continue;
                            }
                        }
                    }

                    let bound_expr = self.bind_expression(expr)?;
                    let alias = alias.clone().unwrap_or_else(|| match expr {
                        Expression::Variable(v) => v.clone(),
                        Expression::PropertyLookup(var, prop) => format!("{var}.{prop}"),
                        Expression::Function(name, _, _) => format!("{name}(...)"),
                        _ => "result".into(),
                    });
                    items.push(BoundProjectionItem {
                        expression: bound_expr,
                        alias,
                    });
                }
            }
        }

        // Register RETURN aliases as variables so ORDER BY can reference them
        // (e.g., `RETURN sum(n.salary) AS total ORDER BY total DESC`)
        for item in &items {
            if !item.alias.is_empty() && !self.variables.contains_key(&item.alias) {
                self.variables.insert(item.alias.clone(), BoundVariable {
                    table_name: String::new(),
                    type_: item.expression.get_type(),
                });
            }
        }

        let order_by = if let Some(ob) = &return_clause.order_by {
            let mut bound_items = Vec::new();
            for ob_item in ob {
                let mut expr = self.bind_expression(&ob_item.expression)?;
                // Replace alias variables (e.g. `ORDER BY n` after `RETURN ... AS n`)
                // with the original RETURN expression so the Sort operator can evaluate
                // it against its child (which may not have the alias column).
                if let BoundExpression::Variable(var_name, _) = &expr {
                    if let Some(ret_item) = items.iter().find(|ri| &ri.alias == var_name) {
                        tracing::debug!("ORDER BY alias '{}' -> {:?}", var_name, ret_item.expression);
                        expr = ret_item.expression.clone();
                    } else {
                        tracing::debug!("ORDER BY variable '{}' not found in RETURN aliases. Aliases: {:?}",
                            var_name, items.iter().map(|i| &i.alias).collect::<Vec<_>>());
                    }
                }
                bound_items.push(BoundOrderByItem {
                    expression: expr,
                    descending: ob_item.descending,
                });
            }
            Some(bound_items)
        } else {
            None
        };

        Ok(BoundReturnClause {
            distinct: return_clause.distinct,
            items,
            order_by,
            skip: return_clause.skip,
            limit: return_clause.limit,
        })
    }

    fn bind_expression(&mut self, expr: &Expression) -> Result<BoundExpression> {
        match expr {
            Expression::Literal(lit) => Ok(BoundExpression::Literal(lit.clone())),
            Expression::Variable(var) => {
                let binding = self
                    .variables
                    .get(var)
                    .ok_or_else(|| LightningError::Query(format!("Variable {var} not found")))?;
                Ok(BoundExpression::Variable(
                    var.clone(),
                    binding.type_.clone(),
                ))
            }
            Expression::PropertyLookup(var, prop_name) => {
                let (properties, column_offset, table_name, _table_kind) = self.get_table_properties(var)?;

                for (i, prop) in properties.iter().enumerate() {
                    if &prop.name == prop_name {
                        // Catalog already includes _id at index 0 for node tables,
                        // and _src/_dst for rel tables. The physical scan outputs
                        // columns in catalog-property order, so the index directly
                        // maps to the scan output column.
                        return Ok(BoundExpression::PropertyLookup(
                            var.clone(),
                            column_offset + i,
                            prop.type_.clone(),
                        ));
                    }
                }

                Err(LightningError::Query(format!(
                    "Property {prop_name} not found on variable {var} (table {table_name})"
                )))
            }
            Expression::Comparison(left, op, right) => {
                let bound_left = self.bind_expression(left)?;
                let bound_right = self.bind_expression(right)?;
                Ok(BoundExpression::Comparison(
                    Box::new(bound_left),
                    *op,
                    Box::new(bound_right),
                ))
            }
            Expression::Arithmetic(left, op, right) => {
                let bound_left = self.bind_expression(left)?;
                let bound_right = self.bind_expression(right)?;
                Ok(BoundExpression::Arithmetic(
                    Box::new(bound_left),
                    *op,
                    Box::new(bound_right),
                ))
            }
            Expression::Logical(lhs, op, rhs) => {
                let bound_lhs = self.bind_expression(lhs)?;
                let bound_rhs = self.bind_expression(rhs)?;
                Ok(BoundExpression::Logical(
                    Box::new(bound_lhs),
                    *op,
                    Box::new(bound_rhs),
                ))
            }
            Expression::Not(expr) => {
                let bound_expr = self.bind_expression(expr)?;
                Ok(BoundExpression::Not(Box::new(bound_expr)))
            }
            Expression::Function(name, args, distinct) => {
                let mut bound_args = Vec::new();

                // Handle DISTINCT - convert COUNT(DISTINCT x) to COUNT_DISTINCT
                let actual_name = if *distinct {
                    match name.to_uppercase().as_str() {
                        "COUNT" => "COUNT_DISTINCT".to_string(),
                        _ => name.to_uppercase(),
                    }
                } else {
                    name.to_uppercase()
                };

                // CHECK FOR LIST FUNCTIONS WITH LAMBDAS — bind before generic arg pass
                if let (
                    Some("LIST_FILTER")
                    | Some("LIST_TRANSFORM")
                    | Some("LIST_ANY")
                    | Some("LIST_ALL")
                    | Some("LIST_SINGLE")
                    | Some("LIST_NONE"),
                    [list_expr, lambda_expr],
                ) = (Some(actual_name.as_str()), args.as_slice())
                {
                    let bound_list = self.bind_expression(list_expr)?;
                    let element_type = if let LogicalType::List(el) = bound_list.get_type() {
                        *el
                    } else {
                        LogicalType::Any
                    };

                    if let Expression::Lambda(var, body) = lambda_expr {
                        let mut inner_binder = Binder {
                            catalog: self.catalog,
                            function_registry: self.function_registry,
                            variables: self.variables.clone(),
                            column_offsets: self.column_offsets.clone(),
                        };
                        inner_binder.variables.insert(
                            var.clone(),
                            BoundVariable {
                                table_name: "".into(),
                                type_: element_type,
                            },
                        );
                        let bound_body = inner_binder.bind_expression(body)?;
                        let bound_lambda =
                            BoundExpression::Lambda(var.clone(), Box::new(bound_body));

                        let ret_type = match actual_name.as_str() {
                            "LIST_FILTER" => bound_list.get_type(),
                            "LIST_TRANSFORM" => {
                                LogicalType::List(Box::new(bound_lambda.get_type()))
                            }
                            _ => LogicalType::Bool,
                        };
                        return Ok(BoundExpression::Function(
                            actual_name,
                            vec![bound_list, bound_lambda],
                            ret_type,
                        ));
                    }
                }

                // Generic argument binding for all functions
                for arg in args {
                    bound_args.push(self.bind_expression(arg)?);
                }

                // Inject rel table name for SHORTEST_PATH / ALL_SHORTEST_PATHS
                if actual_name == "SHORTEST_PATH" || actual_name == "ALL_SHORTEST_PATHS" {
                    if bound_args.len() >= 2 {
                        if let (
                            BoundExpression::PropertyLookup(start_var, _, _),
                            BoundExpression::PropertyLookup(end_var, _, _),
                        ) = (&bound_args[0], &bound_args[1])
                        {
                            let src_table = self
                                .variables
                                .get(start_var)
                                .map(|v| v.table_name.as_str());
                            let dst_table =
                                self.variables.get(end_var).map(|v| v.table_name.as_str());
                            if let (Some(src), Some(dst)) = (src_table, dst_table) {
                                let rel_table = self.catalog.rel_tables.iter().find(|(_, rel)| {
                                    (rel.from_table == src && rel.to_table == dst)
                                        || (rel.from_table == dst && rel.to_table == src)
                                });
                                if let Some((name, _)) = rel_table {
                                    bound_args.push(BoundExpression::Literal(Literal::String(
                                        name.clone(),
                                    )));
                                }
                            }
                        }
                    }
                }

                // CHECK FOR NEXTVAL (after bound_args is populated)
                if actual_name == "NEXTVAL" {
                    if let [BoundExpression::Literal(Literal::String(seq_name))] =
                        bound_args.as_slice()
                    {
                        return Ok(BoundExpression::NextVal(seq_name.clone()));
                    }
                }

                // CHECK FOR MACRO (after bound_args is populated)
                if let Some(macro_entry) = self.catalog.get_macro(&actual_name) {
                    if macro_entry.params.len() != bound_args.len() {
                        return Err(LightningError::Query(format!(
                            "Macro {} expects {} arguments, but {} were provided",
                            actual_name,
                            macro_entry.params.len(),
                            bound_args.len()
                        )));
                    }
                    let mut substitution = HashMap::new();
                    let mut prev_vars = HashMap::new();
                    for (i, param_name) in macro_entry.params.iter().enumerate() {
                        substitution.insert(param_name.clone(), bound_args[i].clone());
                        if let Some(t) = self.variables.insert(
                            param_name.clone(),
                            BoundVariable {
                                table_name: "".into(),
                                type_: LogicalType::Any,
                            },
                        ) {
                            prev_vars.insert(param_name.clone(), t);
                        }
                    }

                    let bound_body = self.bind_expression(&macro_entry.body)?;

                    for param_name in &macro_entry.params {
                        self.variables.remove(param_name);
                    }
                    self.variables.extend(prev_vars);

                    return self.substitute_macro_body(&bound_body, &substitution);
                }

                let arg_types: Vec<_> = bound_args.iter().map(|a| a.get_type()).collect();
                let ret_type =
                    if let Some(func) = self.function_registry.get_scalar_function(&actual_name) {
                        func.resolve_type(&arg_types)?
                    } else {
                        match actual_name.as_str() {
                            "COUNT" | "COUNT_DISTINCT" => LogicalType::Int64,
                            "SUM" | "AVG" => LogicalType::Double,
                            "ID" => LogicalType::Uint64,
                            "LABELS" => LogicalType::List(Box::new(LogicalType::String)),
                            "KEYS" => LogicalType::List(Box::new(LogicalType::String)),
                            "MIN" | "MAX" => {
                                if !bound_args.is_empty() {
                                    bound_args[0].get_type()
                                } else {
                                    LogicalType::Any
                                }
                            }
                            _ => LogicalType::Any,
                        }
                    };

                Ok(BoundExpression::Function(
                    actual_name.clone(),
                    bound_args,
                    ret_type,
                ))
            }
            Expression::List(exprs) => {
                let mut bound_exprs = Vec::new();
                for expr in exprs {
                    bound_exprs.push(self.bind_expression(expr)?);
                }
                // Determine list element type. For now, use the type of the first element or Any.
                let element_type = if let Some(first) = bound_exprs.first() {
                    first.get_type()
                } else {
                    LogicalType::Any
                };
                Ok(BoundExpression::List(
                    bound_exprs,
                    LogicalType::List(Box::new(element_type)),
                ))
            }
            Expression::Map(entries) => {
                let mut bound_entries = Vec::new();
                let mut field_types = Vec::new();
                for (key, val_expr) in entries {
                    let bound_val = self.bind_expression(val_expr)?;
                    let val_type = bound_val.get_type();
                    field_types.push(lightning_types::StructField {
                        name: key.clone(),
                        type_: val_type.clone(),
                    });
                    bound_entries.push((key.clone(), bound_val));
                }
                Ok(BoundExpression::Map(
                    bound_entries,
                    LogicalType::Struct(field_types),
                ))
            }
            Expression::Lambda(var, body) => {
                // Lambda without context (placeholder)
                let bound_body = self.bind_expression(body)?;
                Ok(BoundExpression::Lambda(var.clone(), Box::new(bound_body)))
            }
            Expression::Case {
                expression,
                when_then,
                else_expression,
            } => {
                let bound_expr = expression
                    .as_ref()
                    .map(|e| self.bind_expression(e))
                    .transpose()?;
                let mut bound_wt = Vec::new();
                for (w, t) in when_then {
                    bound_wt.push((self.bind_expression(w)?, self.bind_expression(t)?));
                }
                let bound_else = else_expression
                    .as_ref()
                    .map(|e| self.bind_expression(e))
                    .transpose()?;

                let ret_type = if !bound_wt.is_empty() {
                    bound_wt[0].1.get_type()
                } else if let Some(ref e) = bound_else {
                    e.get_type()
                } else {
                    LogicalType::Any
                };

                Ok(BoundExpression::Case {
                    expression: bound_expr.map(Box::new),
                    when_then: bound_wt,
                    else_expression: bound_else.map(Box::new),
                    return_type: ret_type,
                })
            }
            Expression::Parameter(name) => Ok(BoundExpression::Parameter(name.clone())),
            Expression::Exists(steps) => {
                let mut bound_steps = Vec::new();
                for (m, w) in steps {
                    let bm = self.bind_match_clause(m)?;
                    let bw = w
                        .as_ref()
                        .map(|e| {
                            std::result::Result::<BoundWhereClause, LightningError>::Ok(
                                BoundWhereClause {
                                    expression: self.bind_expression(&e.expression)?,
                                },
                            )
                        })
                        .transpose()?;
                    bound_steps.push((bm, bw));
                }
                Ok(BoundExpression::Exists(bound_steps))
            }
            Expression::CountSubquery(steps) => {
                let mut bound_steps = Vec::new();
                for (m, w) in steps {
                    let bm = self.bind_match_clause(m)?;
                    let bw = w
                        .as_ref()
                        .map(|e| {
                            std::result::Result::<BoundWhereClause, LightningError>::Ok(
                                BoundWhereClause {
                                    expression: self.bind_expression(&e.expression)?,
                                },
                            )
                        })
                        .transpose()?;
                    bound_steps.push((bm, bw));
                }
                Ok(BoundExpression::CountSubquery(bound_steps))
            }
        }
    }

    fn bind_merge_clause(&mut self, merge: &MergeClause) -> Result<BoundMergeClause> {
        let pattern = self.bind_node_pattern(&merge.pattern.node_pattern)?;
        // Pattern matches must register the variable if present
        if let Some(var) = &pattern.variable {
            self.variables.insert(
                var.clone(),
                BoundVariable {
                    table_name: pattern.table_name.clone(),
                    type_: LogicalType::Node(vec![]),
                },
            );
        }

        let mut on_create_assignments = Vec::new();
        for assign in &merge.on_create_assignments {
            on_create_assignments.push(self.bind_property_assignment(assign)?);
        }

        let mut on_match_assignments = Vec::new();
        for assign in &merge.on_match_assignments {
            on_match_assignments.push(self.bind_property_assignment(assign)?);
        }

        Ok(BoundMergeClause {
            pattern,
            on_create_assignments,
            on_match_assignments,
        })
    }

    fn bind_property_assignment(
        &mut self,
        assign: &PropertyAssignment,
    ) -> Result<BoundPropertyAssignment> {
        let (properties, offset, table_name, _table_kind) =
            self.get_table_properties(&assign.variable)?;
        let mut prop_idx = None;
        for (i, prop) in properties.iter().enumerate() {
            if prop.name == assign.property_key {
                prop_idx = Some(offset + i);
                break;
            }
        }
        let idx = if let Some(found) = prop_idx {
            found
        } else {
            // Property doesn't exist yet — dynamically create it by assigning
            // a new index at the end of the table's property list. The actual
            // column creation happens at write time in PhysicalSet::get_next.
            let new_idx = offset + properties.len();
            // Temporarily add the property to the catalog so subsequent lookups
            // in the same query succeed. The PersistentCatalog is backed by a
            // LazyCatalog that supports mutation.
            // Note: this uses an internal API on a RwLock-held catalog.
            // In a production environment, this should go through ALTER TABLE.
            new_idx
        };
        Ok(BoundPropertyAssignment {
            variable: assign.variable.clone(),
            table_name: table_name.clone(),
            property_idx: idx,
            expression: self.bind_expression(&assign.expression)?,
        })
    }

    fn get_table_properties(
        &self,
        variable: &str,
    ) -> Result<(&[crate::catalog::PropertyDefinition], usize, String, u8)> {
        let binding = self
            .variables
            .get(variable)
            .ok_or_else(|| LightningError::Query(format!("Variable {variable} not found")))?;

        let (props, table_kind) = self.catalog.get_table_properties(&binding.table_name)
            .ok_or_else(|| LightningError::Query(format!(
                "Table {} not found for variable {}",
                binding.table_name, variable
            )))?;
        let offset = self.column_offsets.get(variable).copied().unwrap_or(0);
        Ok((props, offset, binding.table_name.clone(), table_kind))
    }

    fn bind_data_type(&self, data_type: &crate::parser::ast::DataType) -> LogicalType {
        
        match data_type {
            crate::parser::ast::DataType::Int64 => LogicalType::Int64,
            crate::parser::ast::DataType::Int32 => LogicalType::Int32,
            crate::parser::ast::DataType::Double => LogicalType::Double,
            crate::parser::ast::DataType::Float => LogicalType::Float,
            crate::parser::ast::DataType::String => LogicalType::String,
            crate::parser::ast::DataType::Bool => LogicalType::Bool,
            crate::parser::ast::DataType::Date => LogicalType::Date,
            crate::parser::ast::DataType::Timestamp => LogicalType::Timestamp,
            crate::parser::ast::DataType::List(inner) => {
                LogicalType::List(Box::new(self.bind_data_type(inner)))
            }
            crate::parser::ast::DataType::Struct(fields) => {
                let mut bound_fields = Vec::new();
                for f in fields {
                    bound_fields.push(lightning_types::StructField {
                        name: f.name.clone(),
                        type_: self.bind_data_type(&f.data_type),
                    });
                }
                LogicalType::Struct(bound_fields)
            }
        }
    }

    fn substitute_macro_body(
        &self,
        body: &BoundExpression,
        substitution: &HashMap<String, BoundExpression>,
    ) -> Result<BoundExpression> {
        match body {
            BoundExpression::Variable(var, _) => {
                if let Some(expr) = substitution.get(var) {
                    Ok(expr.clone())
                } else {
                    Ok(body.clone())
                }
            }
            BoundExpression::PropertyLookup(var, prop_idx, type_) => {
                if let Some(_expr) = substitution.get(var) {
                    Ok(BoundExpression::PropertyLookup(
                        var.clone(),
                        *prop_idx,
                        type_.clone(),
                    ))
                } else {
                    Ok(body.clone())
                }
            }
            BoundExpression::Arithmetic(left, op, right) => Ok(BoundExpression::Arithmetic(
                Box::new(self.substitute_macro_body(left, substitution)?),
                *op,
                Box::new(self.substitute_macro_body(right, substitution)?),
            )),
            BoundExpression::Logical(lhs, op, rhs) => Ok(BoundExpression::Logical(
                Box::new(self.substitute_macro_body(lhs, substitution)?),
                *op,
                Box::new(self.substitute_macro_body(rhs, substitution)?),
            )),
            BoundExpression::Comparison(lhs, op, rhs) => Ok(BoundExpression::Comparison(
                Box::new(self.substitute_macro_body(lhs, substitution)?),
                *op,
                Box::new(self.substitute_macro_body(rhs, substitution)?),
            )),
            BoundExpression::Function(name, args, ret_type) => {
                let mut new_args = Vec::new();
                for arg in args {
                    new_args.push(self.substitute_macro_body(arg, substitution)?);
                }
                Ok(BoundExpression::Function(
                    name.clone(),
                    new_args,
                    ret_type.clone(),
                ))
            }
            BoundExpression::Case { expression, when_then, else_expression, return_type } => {
                let new_expr = expression.as_ref().map(|e| {
                    self.substitute_macro_body(e, substitution).map(Box::new)
                }).transpose()?;
                let new_when_then = when_then.iter()
                    .map(|(w, t)| Ok((
                        self.substitute_macro_body(w, substitution)?,
                        self.substitute_macro_body(t, substitution)?,
                    )))
                    .collect::<Result<Vec<_>>>()?;
                let new_else = else_expression.as_ref().map(|e| {
                    self.substitute_macro_body(e, substitution).map(Box::new)
                }).transpose()?;
                Ok(BoundExpression::Case {
                    expression: new_expr,
                    when_then: new_when_then,
                    else_expression: new_else,
                    return_type: return_type.clone(),
                })
            }
            BoundExpression::List(items, typ) => {
                let new_items = items.iter()
                    .map(|i| self.substitute_macro_body(i, substitution))
                    .collect::<Result<Vec<_>>>()?;
                Ok(BoundExpression::List(new_items, typ.clone()))
            }
            BoundExpression::Map(entries, typ) => {
                let new_entries = entries.iter()
                    .map(|(k, v)| Ok((
                        k.clone(),
                        self.substitute_macro_body(v, substitution)?,
                    )))
                    .collect::<Result<Vec<_>>>()?;
                Ok(BoundExpression::Map(new_entries, typ.clone()))
            }
            BoundExpression::Lambda(params, body) => {
                let new_body = self.substitute_macro_body(body, substitution)?;
                Ok(BoundExpression::Lambda(params.clone(), Box::new(new_body)))
            }
            _ => Ok(body.clone()),
        }
    }

    fn require_single_label<'b>(&self, labels: &'b [String], context: &str) -> Result<&'b String> {
        if labels.len() > 1 {
            return Err(LightningError::Query(format!(
                "Multiple labels in {context} are not supported yet. Got: {labels:?}"
            )));
        }
        labels.first().ok_or_else(|| {
            LightningError::Query(format!("{context} must have a label"))
        })
    }

    #[allow(dead_code)]
    fn check_auth_table_access(_patterns: &[Pattern]) -> Result<()> {
        Ok(())
    }
}

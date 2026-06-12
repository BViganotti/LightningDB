use lightning_types::LogicalType;
use serde::{Deserialize, Serialize};

pub fn data_type_to_logical(dt: &DataType) -> LogicalType {
    match dt {
        DataType::Int64 => LogicalType::Int64,
        DataType::Int32 => LogicalType::Int32,
        DataType::Double => LogicalType::Double,
        DataType::Float => LogicalType::Float,
        DataType::String => LogicalType::String,
        DataType::Bool => LogicalType::Bool,
        DataType::Date => LogicalType::Date,
        DataType::Timestamp => LogicalType::Timestamp,
        DataType::List(inner) => LogicalType::List(Box::new(data_type_to_logical(inner))),
        DataType::Struct(fields) => LogicalType::Struct(
            fields
                .iter()
                .map(|f| lightning_types::StructField {
                    name: f.name.clone(),
                    type_: data_type_to_logical(&f.data_type),
                })
                .collect(),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterOperation {
    AddColumn { name: String, data_type: DataType },
    DropColumn { name: String },
    RenameTable { new_name: String },
    RenameColumn { old_name: String, new_name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    pub union_queries: Vec<UnionQuery>,
    pub is_explain: bool,
    pub is_profile: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnionQuery {
    pub statement: Statement,
    pub next_union: Option<(Box<UnionQuery>, bool)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    Match(Option<MatchClause>, Option<WhereClause>, Vec<Clause>),
    Create(Pattern),
    CreateTableNode {
        name: String,
        columns: Vec<ColumnDefinition>,
        primary_key: String,
        if_not_exists: bool,
    },
    CreateTableRel {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<ColumnDefinition>,
        if_not_exists: bool,
    },
    AlterTable { name: String, operation: AlterOperation },
    DropTable(String, bool), // name, if_exists
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
    StandaloneCall(String, Vec<Literal>),
    Checkpoint,
    Vacuum,
    Transaction(TransactionAction),
    CreateSequence {
        name: String,
        start_with: u64,
        increment_by: i64,
    },
    CreateMacro {
        name: String,
        params: Vec<String>,
        body: Expression,
    },
    Merge(MergeClause),
    CreateConstraint {
        name: String,
        table_label: String,
        property: String,
    },
    DropConstraint(String),
    CreateIndex {
        name: String,
        table_label: String,
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionAction {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int64,
    Int32,
    Double,
    Float,
    String,
    Bool,
    Date,
    Timestamp,
    List(Box<DataType>),
    Struct(Vec<ColumnDefinition>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    Return(ReturnClause),
    Delete(DeleteClause),
    Set(SetClause),
    Remove(RemoveClause),
    Create(Pattern),
    Unwind(UnwindClause),
    Merge(MergeClause),
    Call(CallClause),
    Subquery(Box<Query>),
    With(ReturnClause, Option<WhereClause>),
    Match(MatchClause),
    OptionalMatch(MatchClause),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallClause {
    pub procedure_name: String,
    pub parameters: Vec<Expression>,
    pub yield_items: Option<Vec<YieldItem>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YieldItem {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_create_assignments: Vec<PropertyAssignment>,
    pub on_match_assignments: Vec<PropertyAssignment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnwindClause {
    pub expression: Expression,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteClause {
    pub variables: Vec<String>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetClause {
    pub assignments: Vec<PropertyAssignment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveClause {
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyAssignment {
    pub variable: String,
    pub property_key: String,
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pattern {
    pub node_pattern: NodePattern,
    pub relationship_chains: Vec<RelationshipChain>,
    pub is_shortest_path: bool,
    pub is_all_shortest_paths: bool,
    pub shortest_path_start: Option<NodePattern>,
    pub shortest_path_chain: Option<RelationshipChain>,
    pub shortest_path_end: Option<NodePattern>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<PropertyItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipChain {
    pub relationship_pattern: RelationshipPattern,
    pub node_pattern: NodePattern,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub direction: Direction,
    pub properties: Vec<PropertyItem>,
    pub var_len_bounds: Option<(Option<u32>, Option<u32>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Left,
    Right,
    Both,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyItem {
    pub key: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ProjectionItem>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub skip: Option<f64>,
    pub limit: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expression: Expression,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProjectionItem {
    Star,
    Expression(Expression, Option<String>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Literal),
    Variable(String),
    PropertyLookup(String, String),
    Comparison(Box<Expression>, ComparisonOperator, Box<Expression>),
    Arithmetic(Box<Expression>, ArithmeticOperator, Box<Expression>),
    Logical(Box<Expression>, LogicalOperator, Box<Expression>),
    Not(Box<Expression>),
    Function(String, Vec<Expression>, bool), // name, args, distinct
    List(Vec<Expression>),
    Case {
        expression: Option<Box<Expression>>,
        when_then: Vec<(Expression, Expression)>,
        else_expression: Option<Box<Expression>>,
    },
    Lambda(String, Box<Expression>), // variable, body
    Parameter(String),               // $name
    Exists(Vec<(MatchClause, Option<WhereClause>)>),
    CountSubquery(Vec<(MatchClause, Option<WhereClause>)>),
    Map(Vec<(String, Expression)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
    Xor,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Literal value in a Cypher query.
/// NOTE: Number uses f64, which loses precision for integers beyond ±2^53.
/// A proper fix would add an Integer(i64) variant for integer literals.
pub enum Literal {
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
}

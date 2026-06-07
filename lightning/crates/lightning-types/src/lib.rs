use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalTypeID {
    Any,
    Bool,
    Int128,
    Int64,
    Int32,
    Int16,
    Int8,
    Uint128,
    Uint64,
    Uint32,
    Uint16,
    Uint8,
    Float,
    Double,
    String,
    Blob,
    Timestamp,
    Date,
    Interval,
    InternalID,
    Serial,
    List,
    Struct,
    Map,
    Union,
    Node,
    Rel,
    RecursiveRel,
    Lambda,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalID {
    pub offset: u64,
    pub table_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalType {
    Any,
    Bool,
    Int128,
    Int64,
    Int32,
    Int16,
    Int8,
    Uint128,
    Uint64,
    Uint32,
    Uint16,
    Uint8,
    Float,
    Double,
    String,
    Blob,
    Timestamp,
    Date,
    Interval,
    InternalID,
    Serial,
    List(Box<LogicalType>),
    Struct(Vec<StructField>),
    Map(Box<LogicalType>, Box<LogicalType>),
    Union(Vec<StructField>),
    Node(Vec<StructField>),
    Rel(Vec<StructField>),
    Lambda(Box<LogicalType>), // return type
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub type_: LogicalType,
}

impl LogicalType {
    pub fn id(&self) -> LogicalTypeID {
        match self {
            LogicalType::Any => LogicalTypeID::Any,
            LogicalType::Bool => LogicalTypeID::Bool,
            LogicalType::Int128 => LogicalTypeID::Int128,
            LogicalType::Int64 => LogicalTypeID::Int64,
            LogicalType::Int32 => LogicalTypeID::Int32,
            LogicalType::Int16 => LogicalTypeID::Int16,
            LogicalType::Int8 => LogicalTypeID::Int8,
            LogicalType::Uint128 => LogicalTypeID::Uint128,
            LogicalType::Uint64 => LogicalTypeID::Uint64,
            LogicalType::Uint32 => LogicalTypeID::Uint32,
            LogicalType::Uint16 => LogicalTypeID::Uint16,
            LogicalType::Uint8 => LogicalTypeID::Uint8,
            LogicalType::Float => LogicalTypeID::Float,
            LogicalType::Double => LogicalTypeID::Double,
            LogicalType::String => LogicalTypeID::String,
            LogicalType::Blob => LogicalTypeID::Blob,
            LogicalType::Timestamp => LogicalTypeID::Timestamp,
            LogicalType::Date => LogicalTypeID::Date,
            LogicalType::Interval => LogicalTypeID::Interval,
            LogicalType::InternalID => LogicalTypeID::InternalID,
            LogicalType::Serial => LogicalTypeID::Serial,
            LogicalType::List(_) => LogicalTypeID::List,
            LogicalType::Struct(_) => LogicalTypeID::Struct,
            LogicalType::Map(_, _) => LogicalTypeID::Map,
            LogicalType::Union(_) => LogicalTypeID::Union,
            LogicalType::Node(_) => LogicalTypeID::Node,
            LogicalType::Rel(_) => LogicalTypeID::Rel,
            LogicalType::Lambda(_) => LogicalTypeID::Lambda,
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Any => write!(f, "ANY"),
            LogicalType::Bool => write!(f, "BOOL"),
            LogicalType::Int128 => write!(f, "INT128"),
            LogicalType::Int64 => write!(f, "INT64"),
            LogicalType::Int32 => write!(f, "INT32"),
            LogicalType::Int16 => write!(f, "INT16"),
            LogicalType::Int8 => write!(f, "INT8"),
            LogicalType::Uint128 => write!(f, "UINT128"),
            LogicalType::Uint64 => write!(f, "UINT64"),
            LogicalType::Uint32 => write!(f, "UINT32"),
            LogicalType::Uint16 => write!(f, "UINT16"),
            LogicalType::Uint8 => write!(f, "UINT8"),
            LogicalType::Float => write!(f, "FLOAT"),
            LogicalType::Double => write!(f, "DOUBLE"),
            LogicalType::String => write!(f, "STRING"),
            LogicalType::Blob => write!(f, "BLOB"),
            LogicalType::Timestamp => write!(f, "TIMESTAMP"),
            LogicalType::Date => write!(f, "DATE"),
            LogicalType::Interval => write!(f, "INTERVAL"),
            LogicalType::InternalID => write!(f, "INTERNAL_ID"),
            LogicalType::Serial => write!(f, "SERIAL"),
            LogicalType::List(child) => write!(f, "{}[]", child),
            LogicalType::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}: {}", field.name, field.type_)?;
                }
                write!(f, ")")
            }
            LogicalType::Map(key, value) => write!(f, "MAP({}, {})", key, value),
            LogicalType::Union(fields) => {
                write!(f, "UNION(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}: {}", field.name, field.type_)?;
                }
                write!(f, ")")
            }
            LogicalType::Node(fields) => {
                write!(f, "NODE(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}: {}", field.name, field.type_)?;
                }
                write!(f, ")")
            }
            LogicalType::Rel(fields) => {
                write!(f, "REL(")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}: {}", field.name, field.type_)?;
                }
                write!(f, ")")
            }
            LogicalType::Lambda(ret) => write!(f, "LAMBDA -> {}", ret),
        }
    }
}

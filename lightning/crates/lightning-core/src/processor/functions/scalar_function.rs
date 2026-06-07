use crate::Result;
use arrow::array::ArrayRef;
use lightning_types::LogicalType;
use std::sync::Arc;

pub type ScalarFunctionExec = Arc<dyn Fn(&[ArrayRef], usize) -> Result<ArrayRef> + Send + Sync>;

#[derive(Clone)]
pub struct ScalarFunction {
    pub name: String,
    pub exec: ScalarFunctionExec,
}

impl ScalarFunction {
    pub fn new(name: String, exec: ScalarFunctionExec) -> Self {
        Self { name, exec }
    }

    pub fn execute(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        (self.exec)(args, num_rows)
    }

    pub fn resolve_type(&self, arg_types: &[LogicalType]) -> Result<LogicalType> {
        match self.name.as_str() {
            "UPPER" | "LOWER" | "SUBSTRING" | "REPLACE" | "TO_STRING" | "CONCAT" | "INITCAP"
            | "TRIM" | "LTRIM" | "RTRIM" | "LEFT" | "RIGHT" | "REPEAT" => Ok(LogicalType::String),
            "ABS" | "CEIL" | "FLOOR" | "ROUND" | "SQRT" | "LOG" | "LN" | "EXP" | "SIN" | "COS"
            | "TAN" | "POW" => Ok(LogicalType::Double),
            "TO_INT" | "LENGTH" | "SIZE" | "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE"
            | "SECOND" | "MOD" => Ok(LogicalType::Int64),
            "DATE" | "CURRENT_DATE" => Ok(LogicalType::Date),
            "TIMESTAMP" | "CURRENT_TIMESTAMP" => Ok(LogicalType::Timestamp),
            "COALESCE" | "ID" => {
                if let Some(first) = arg_types.first() {
                    Ok(first.clone())
                } else {
                    Ok(LogicalType::Any)
                }
            }
            "LIST_CONTAINS" | "CONTAINS" | "STARTS_WITH" | "ENDS_WITH" => Ok(LogicalType::Bool),
            "RANGE" | "LIST_APPEND" | "LIST_PREPEND" | "LIST_CONCAT" | "SPLIT"
            | "LIST_DISTINCT" | "LIST_SORT" | "LIST_REVERSE" | "MAP_KEYS" | "MAP_VALUES"
            | "LIST_SLICE" => {
                if let Some(LogicalType::List(inner)) = arg_types.first() {
                    Ok(LogicalType::List(inner.clone()))
                } else {
                    Ok(LogicalType::List(Box::new(LogicalType::Any)))
                }
            }
            "DATE_ADD" | "DATE_SUB" => Ok(LogicalType::Date),
            "LIST_EXTRACT" | "MAP_EXTRACT" => Ok(LogicalType::Any),
            _ => Ok(LogicalType::Any),
        }
    }
}

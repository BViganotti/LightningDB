use crate::planner::binder::BoundExpression;
use crate::planner::logical_plan::LogicalOperator;
use crate::processor::Value;
use crate::{LightningError, Result};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder,
    Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampMicrosecondBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use lightning_types::LogicalType;
use std::sync::Arc;

pub fn logical_type_to_arrow_type(t: &LogicalType) -> DataType {
    match t {
        LogicalType::Int64 => DataType::Int64,
        LogicalType::Int32 => DataType::Int32,
        LogicalType::Uint64 | LogicalType::Node(_) => DataType::UInt64,
        LogicalType::Double => DataType::Float64,
        LogicalType::Bool => DataType::Boolean,
        LogicalType::String => DataType::Utf8,
        LogicalType::Date => DataType::Date32,
        LogicalType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        LogicalType::List(child) => DataType::List(Arc::new(Field::new(
            "item",
            logical_type_to_arrow_type(child),
            true,
        ))),
        LogicalType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        &field.name,
                        logical_type_to_arrow_type(&field.type_),
                        true,
                    ))
                })
                .collect(),
        ),
        _ => DataType::Null,
    }
}

pub fn append_null_to_builder(builder: &mut dyn ArrayBuilder, t: &DataType) -> Result<()> {
    match t {
        DataType::Int64 => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_null(),
        DataType::Int32 => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_null(),
        DataType::UInt64 => builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .unwrap()
            .append_null(),
        DataType::Float64 => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_null(),
        DataType::Boolean => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_null(),
        DataType::Utf8 => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
        DataType::Date32 => builder
            .as_any_mut()
            .downcast_mut::<Date32Builder>()
            .unwrap()
            .append_null(),
        DataType::Timestamp(_, _) => builder
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_null(),
        DataType::List(ref inner) => {
            // For List types, handle null append based on inner type
            match inner.data_type() {
                DataType::Float32 => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::ListBuilder<arrow::array::Float32Builder>>()
                        .unwrap()
                        .append_null();
                }
                DataType::Null => {
                    // List of nulls - this is an empty list, just return Ok
                    // since there's nothing meaningful to append for Null type
                    return Ok(());
                }
                _ => {
                    return Err(LightningError::Internal(format!(
                        "Unsupported list inner type for append_null_to_builder: {:?}",
                        inner.data_type()
                    )));
                }
            }
        }
        DataType::Float32 => builder
            .as_any_mut()
            .downcast_mut::<arrow::array::Float32Builder>()
            .unwrap()
            .append_null(),
        _ => {
            return Err(LightningError::Internal(format!(
                "Unsupported type for append_null_to_builder: {:?}",
                t
            )))
        }
    }
    Ok(())
}

pub fn append_value_to_builder(
    builder: &mut dyn ArrayBuilder,
    val: &Value,
    t: &LogicalType,
) -> Result<()> {
    match val {
        Value::Null => append_null_to_builder(builder, &logical_type_to_arrow_type(t)),
        _ => match t {
            LogicalType::Int64 => {
                let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                b.append_value(val.as_number() as i64);
                Ok(())
            }
            LogicalType::Int32 => {
                let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
                b.append_value(val.as_number() as i32);
                Ok(())
            }
            LogicalType::Uint64 | LogicalType::Node(_) => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<UInt64Builder>()
                    .unwrap();
                match val {
                    Value::Node(id) => b.append_value(*id),
                    Value::Number(n) => b.append_value(*n as u64),
                    _ => b.append_null(),
                }
                Ok(())
            }
            LogicalType::Double => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<Float64Builder>()
                    .unwrap();
                b.append_value(val.as_number());
                Ok(())
            }
            LogicalType::String => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap();
                if let Value::String(s) = val {
                    b.append_value(s);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            LogicalType::Bool => {
                let b = builder
                    .as_any_mut()
                    .downcast_mut::<BooleanBuilder>()
                    .unwrap();
                if let Value::Boolean(bv) = val {
                    b.append_value(*bv);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            _ => Err(LightningError::Internal(format!(
                "Unsupported type for append_value_to_builder: {:?}",
                t
            ))),
        },
    }
}

pub fn append_raw_to_builder(
    builder: &mut dyn ArrayBuilder,
    data: &[u8],
    logical_type: &LogicalType,
) -> Result<()> {
    match logical_type {
        LogicalType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            b.append_value(i64::from_le_bytes(data[0..8].try_into().unwrap()));
        }
        LogicalType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            b.append_value(i32::from_le_bytes(data[0..4].try_into().unwrap()));
        }
        LogicalType::Uint64 | LogicalType::Node(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            b.append_value(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        }
        LogicalType::Double => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            b.append_value(f64::from_le_bytes(data[0..8].try_into().unwrap()));
        }
        LogicalType::Bool => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            b.append_value(data[0] != 0);
        }
        LogicalType::String => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            let len = if data[0] == 255 { 63 } else { data[0] as usize };
            let actual_len = std::cmp::min(len, 63);
            b.append_value(std::str::from_utf8(&data[1..1 + actual_len]).unwrap_or(""));
        }
        LogicalType::List(_) => {
            // Lists (like embeddings) are variable-length and not suitable for raw append
            // Just append an empty/null list for now
            return Ok(());
        }
        _ => {
            return Err(LightningError::Internal(
                "Type not supported for raw append".into(),
            ))
        }
    }
    Ok(())
}

pub fn from_arrow(array: &ArrayRef, i: usize) -> Value {
    if array.is_null(i) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::Number(a.value(i))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(i).to_string())
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Boolean(a.value(i))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Node(a.value(i))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(a.value(i) as f64)
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(a.value(i) as f64)
        }
        _ => Value::Null,
    }
}

pub fn append_to_builder(
    builder: &mut dyn ArrayBuilder,
    array: &ArrayRef,
    idx: usize,
) -> Result<()> {
    if array.is_null(idx) {
        append_null_to_builder(builder, array.data_type())?;
        return Ok(());
    }
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        DataType::Timestamp(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap()
                .append_value(a.value(idx));
        }
        _ => {
            return Err(LightningError::Internal(format!(
                "Unsupported type for append_to_builder: {:?}",
                array.data_type()
            )))
        }
    }
    Ok(())
}

pub fn values_to_array(values: &[Value], data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Number(n) => builder.append_value(*n as i64),
                    Value::Node(id) => builder.append_value(*id as i64),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Number(n) => builder.append_value(*n as i32),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Node(id) => builder.append_value(*id),
                    Value::Number(n) => builder.append_value(*n as u64),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Number(n) => builder.append_value(*n),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Boolean(b) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(
                values.len(),
                values
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.len(),
                        _ => 0,
                    })
                    .sum(),
            );
            for v in values {
                match v {
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Date(d) => builder.append_value(*d),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(_, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Value::Timestamp(ts) => builder.append_value(*ts),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => Arc::new(arrow::array::NullArray::new(values.len())),
    }
}

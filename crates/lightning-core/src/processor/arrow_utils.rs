use crate::processor::Value;
use crate::{LightningError, Result};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder,
    Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampMicrosecondBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use lightning_types::LogicalType;
use std::sync::Arc;

macro_rules! downcast_ref_array {
    ($array:expr, $ty:ty) => {
        $array
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| LightningError::Internal("type mismatch: expected Arrow type".into()))?
    };
}

macro_rules! downcast_builder {
    ($builder:expr, $ty:ty) => {
        $builder
            .as_any_mut()
            .downcast_mut::<$ty>()
            .ok_or_else(|| LightningError::Internal("type mismatch: expected Arrow type".into()))?
    };
}

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
    macro_rules! downcast_append_null {
        ($builder:expr, $ty:ty) => {{
            $builder
                .as_any_mut()
                .downcast_mut::<$ty>()
                .ok_or_else(|| {
                    LightningError::Internal("type mismatch: expected Arrow type".into())
                })?
                .append_null();
        }};
    }
    match t {
        DataType::Int64 => downcast_append_null!(builder, Int64Builder),
        DataType::Int32 => downcast_append_null!(builder, Int32Builder),
        DataType::UInt64 => downcast_append_null!(builder, UInt64Builder),
        DataType::Float64 => downcast_append_null!(builder, Float64Builder),
        DataType::Boolean => downcast_append_null!(builder, BooleanBuilder),
        DataType::Utf8 => downcast_append_null!(builder, StringBuilder),
        DataType::Date32 => downcast_append_null!(builder, Date32Builder),
        DataType::Timestamp(_, _) => downcast_append_null!(builder, TimestampMicrosecondBuilder),
        DataType::List(ref inner) => {
            // For List types, handle null append based on inner type
            match inner.data_type() {
                DataType::Float32 => {
                    builder
                        .as_any_mut()
                        .downcast_mut::<arrow::array::ListBuilder<arrow::array::Float32Builder>>()
                        .ok_or_else(|| {
                            LightningError::Internal("type mismatch: expected Arrow type".into())
                        })?
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
        DataType::Float32 => downcast_append_null!(builder, arrow::array::Float32Builder),
        _ => {
            return Err(LightningError::Internal(format!(
                "Unsupported type for append_null_to_builder: {t:?}"
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
                let b = downcast_builder!(builder, Int64Builder);
                b.append_value(val.as_number() as i64);
                Ok(())
            }
            LogicalType::Int32 => {
                let b = downcast_builder!(builder, Int32Builder);
                b.append_value(val.as_number() as i32);
                Ok(())
            }
            LogicalType::Uint64 | LogicalType::Node(_) => {
                let b = downcast_builder!(builder, UInt64Builder);
                match val {
                    Value::Node(id) => b.append_value(*id),
                    Value::Number(n) => b.append_value(*n as u64),
                    _ => b.append_null(),
                }
                Ok(())
            }
            LogicalType::Double => {
                let b = downcast_builder!(builder, Float64Builder);
                b.append_value(val.as_number());
                Ok(())
            }
            LogicalType::String => {
                let b = downcast_builder!(builder, StringBuilder);
                if let Value::String(s) = val {
                    b.append_value(s);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            LogicalType::Bool => {
                let b = downcast_builder!(builder, BooleanBuilder);
                if let Value::Boolean(bv) = val {
                    b.append_value(*bv);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            _ => Err(LightningError::Internal(format!(
                "Unsupported type for append_value_to_builder: {t:?}"
            ))),
        },
    }
}

pub fn append_raw_to_builder(
    builder: &mut dyn ArrayBuilder,
    data: &[u8],
    logical_type: &LogicalType,
) -> Result<()> {
    let required = match logical_type {
        LogicalType::Int64 | LogicalType::Uint64 | LogicalType::Node(_) | LogicalType::Double | LogicalType::Timestamp => 8usize,
        LogicalType::Int32 | LogicalType::Date => 4,
        LogicalType::Bool => 1,
        LogicalType::String => 1usize.saturating_add(std::cmp::min(data.first().copied().unwrap_or(0) as usize, 63)),
        LogicalType::List(_) => 0,
        _ => return Err(LightningError::Internal("Type not supported for raw append".into())),
    };
    if data.len() < required {
        return Err(LightningError::Internal(format!(
            "append_raw_to_builder: expected at least {required} bytes for {logical_type:?}, got {}",
            data.len()
        )));
    }
    match logical_type {
        LogicalType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Int64 Arrow type".into()))?;
            b.append_value(i64::from_le_bytes(data[..8].try_into().map_err(|_| LightningError::Internal("failed to read Int64 from raw data".into()))?));
        }
        LogicalType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Int32 Arrow type".into()))?;
            b.append_value(i32::from_le_bytes(data[..4].try_into().map_err(|_| LightningError::Internal("failed to read Int32 from raw data".into()))?));
        }
        LogicalType::Uint64 | LogicalType::Node(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected UInt64 Arrow type".into()))?;
            b.append_value(u64::from_le_bytes(data[..8].try_into().map_err(|_| LightningError::Internal("failed to read UInt64 from raw data".into()))?));
        }
        LogicalType::Double => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Float64 Arrow type".into()))?;
            b.append_value(f64::from_le_bytes(data[..8].try_into().map_err(|_| LightningError::Internal("failed to read Double from raw data".into()))?));
        }
        LogicalType::Bool => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Boolean Arrow type".into()))?;
            b.append_value(data[0] != 0);
        }
        LogicalType::String => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected String Arrow type".into()))?;
            let len = if data[0] == 255 { 63 } else { data[0] as usize };
            let actual_len = std::cmp::min(len, 63);
            b.append_value(
                std::str::from_utf8(&data[1..1 + actual_len])
                    .map_err(|e| LightningError::Internal(format!("Invalid UTF-8 in String data: {e}")))?,
            );
        }
        LogicalType::Timestamp => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Timestamp Arrow type".into()))?;
            b.append_value(i64::from_le_bytes(data[..8].try_into().map_err(|_| LightningError::Internal("failed to read Timestamp from raw data".into()))?));
        }
        LogicalType::Date => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .ok_or_else(|| LightningError::Internal("type mismatch: expected Date32 Arrow type".into()))?;
            b.append_value(i32::from_le_bytes(data[..4].try_into().map_err(|_| LightningError::Internal("failed to read Date from raw data".into()))?));
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

pub fn from_arrow(array: &ArrayRef, i: usize) -> Result<Value> {
    if array.is_null(i) {
        return Ok(Value::Null);
    }
    match array.data_type() {
        DataType::Float64 => {
            let a = downcast_ref_array!(array, Float64Array);
            Ok(Value::Number(a.value(i)))
        }
        DataType::Utf8 => {
            let a = downcast_ref_array!(array, StringArray);
            Ok(Value::String(a.value(i).to_string()))
        }
        DataType::Boolean => {
            let a = downcast_ref_array!(array, BooleanArray);
            Ok(Value::Boolean(a.value(i)))
        }
        DataType::UInt64 => {
            let a = downcast_ref_array!(array, UInt64Array);
            Ok(Value::Node(a.value(i)))
        }
        DataType::Int64 => {
            let a = downcast_ref_array!(array, Int64Array);
            Ok(Value::Number(a.value(i) as f64))
        }
        DataType::Int32 => {
            let a = downcast_ref_array!(array, Int32Array);
            Ok(Value::Number(a.value(i) as f64))
        }
        _ => Ok(Value::Null),
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
            let a = downcast_ref_array!(array, Int64Array);
            let b = downcast_builder!(builder, Int64Builder);
            b.append_value(a.value(idx));
        }
        DataType::Int32 => {
            let a = downcast_ref_array!(array, Int32Array);
            let b = downcast_builder!(builder, Int32Builder);
            b.append_value(a.value(idx));
        }
        DataType::UInt64 => {
            let a = downcast_ref_array!(array, UInt64Array);
            let b = downcast_builder!(builder, UInt64Builder);
            b.append_value(a.value(idx));
        }
        DataType::Float64 => {
            let a = downcast_ref_array!(array, Float64Array);
            let b = downcast_builder!(builder, Float64Builder);
            b.append_value(a.value(idx));
        }
        DataType::Boolean => {
            let a = downcast_ref_array!(array, BooleanArray);
            let b = downcast_builder!(builder, BooleanBuilder);
            b.append_value(a.value(idx));
        }
        DataType::Utf8 => {
            let a = downcast_ref_array!(array, StringArray);
            let b = downcast_builder!(builder, StringBuilder);
            b.append_value(a.value(idx));
        }
        DataType::Date32 => {
            let a = downcast_ref_array!(array, Date32Array);
            let b = downcast_builder!(builder, Date32Builder);
            b.append_value(a.value(idx));
        }
        DataType::Timestamp(_, _) => {
            let a = downcast_ref_array!(array, arrow::array::TimestampMicrosecondArray);
            let b = downcast_builder!(builder, TimestampMicrosecondBuilder);
            b.append_value(a.value(idx));
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

pub fn str_col(batch: &arrow::record_batch::RecordBatch, col: usize) -> std::result::Result<&StringArray, LightningError> {
    batch.column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| LightningError::Internal("Column is not a string array".into()))
}

pub fn i64_col(batch: &arrow::record_batch::RecordBatch, col: usize) -> std::result::Result<&Int64Array, LightningError> {
    batch.column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| LightningError::Internal("Column is not an int64 array".into()))
}

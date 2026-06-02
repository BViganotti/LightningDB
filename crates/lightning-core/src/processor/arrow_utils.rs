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

fn downcast_builder_mut<'a, T: ArrayBuilder + 'static>(
    builder: &'a mut dyn ArrayBuilder,
    expected: &DataType,
) -> Result<&'a mut T> {
    builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
        LightningError::Internal(format!(
            "Builder type mismatch: expected {:?}, got {}",
            expected,
            std::any::type_name::<T>(),
        ))
    })
}

fn downcast_array_ref<'a, T: Array + 'static>(
    array: &'a dyn Array,
    expected: &DataType,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        LightningError::Internal(format!(
            "Array type mismatch: expected {:?}, got {}",
            expected,
            std::any::type_name::<T>(),
        ))
    })
}

pub fn append_null_to_builder(builder: &mut dyn ArrayBuilder, t: &DataType) -> Result<()> {
    match t {
        DataType::Int64 => downcast_builder_mut::<Int64Builder>(builder, t)?.append_null(),
        DataType::Int32 => downcast_builder_mut::<Int32Builder>(builder, t)?.append_null(),
        DataType::UInt64 => downcast_builder_mut::<UInt64Builder>(builder, t)?.append_null(),
        DataType::Float64 => downcast_builder_mut::<Float64Builder>(builder, t)?.append_null(),
        DataType::Boolean => downcast_builder_mut::<BooleanBuilder>(builder, t)?.append_null(),
        DataType::Utf8 => downcast_builder_mut::<StringBuilder>(builder, t)?.append_null(),
        DataType::Date32 => downcast_builder_mut::<Date32Builder>(builder, t)?.append_null(),
        DataType::Timestamp(_, _) => downcast_builder_mut::<TimestampMicrosecondBuilder>(builder, t)?.append_null(),
        DataType::List(ref inner) => {
            match inner.data_type() {
                DataType::Float32 => {
                    downcast_builder_mut::<arrow::array::ListBuilder<arrow::array::Float32Builder>>(builder, t)?
                        .append_null();
                }
                DataType::Null => {
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
        DataType::Float32 => downcast_builder_mut::<arrow::array::Float32Builder>(builder, t)?.append_null(),
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
    let arrow_type = logical_type_to_arrow_type(t);
    match val {
        Value::Null => append_null_to_builder(builder, &arrow_type),
        _ => match t {
            LogicalType::Int64 => {
                let b = downcast_builder_mut::<Int64Builder>(builder, &arrow_type)?;
                b.append_value(val.as_number() as i64);
                Ok(())
            }
            LogicalType::Int32 => {
                let b = downcast_builder_mut::<Int32Builder>(builder, &arrow_type)?;
                b.append_value(val.as_number() as i32);
                Ok(())
            }
            LogicalType::Uint64 | LogicalType::Node(_) => {
                let b = downcast_builder_mut::<UInt64Builder>(builder, &arrow_type)?;
                match val {
                    Value::Node(id) => b.append_value(*id),
                    Value::Number(n) => b.append_value(*n as u64),
                    _ => b.append_null(),
                }
                Ok(())
            }
            LogicalType::Double => {
                let b = downcast_builder_mut::<Float64Builder>(builder, &arrow_type)?;
                b.append_value(val.as_number());
                Ok(())
            }
            LogicalType::String => {
                let b = downcast_builder_mut::<StringBuilder>(builder, &arrow_type)?;
                if let Value::String(s) = val {
                    b.append_value(s);
                } else {
                    b.append_null();
                }
                Ok(())
            }
            LogicalType::Bool => {
                let b = downcast_builder_mut::<BooleanBuilder>(builder, &arrow_type)?;
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

fn read_u64_le(data: &[u8], type_name: &str) -> Result<u64> {
    let bytes = data
        .get(0..8)
        .ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer too short for {type_name}: expected 8 bytes, got {}",
                data.len()
            ))
        })?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i64_le(data: &[u8], type_name: &str) -> Result<i64> {
    let bytes = data
        .get(0..8)
        .ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer too short for {type_name}: expected 8 bytes, got {}",
                data.len()
            ))
        })?;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i32_le(data: &[u8], type_name: &str) -> Result<i32> {
    let bytes = data
        .get(0..4)
        .ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer too short for {type_name}: expected 4 bytes, got {}",
                data.len()
            ))
        })?;
    Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_f64_le(data: &[u8], type_name: &str) -> Result<f64> {
    let bytes = data
        .get(0..8)
        .ok_or_else(|| {
            LightningError::Internal(format!(
                "Buffer too short for {type_name}: expected 8 bytes, got {}",
                data.len()
            ))
        })?;
    Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
}

pub fn append_raw_to_builder(
    builder: &mut dyn ArrayBuilder,
    data: &[u8],
    logical_type: &LogicalType,
) -> Result<()> {
    let arrow_type = logical_type_to_arrow_type(logical_type);
    match logical_type {
        LogicalType::Int64 => {
            let b = downcast_builder_mut::<Int64Builder>(builder, &arrow_type)?;
            b.append_value(read_i64_le(data, "Int64")?);
        }
        LogicalType::Int32 => {
            let b = downcast_builder_mut::<Int32Builder>(builder, &arrow_type)?;
            b.append_value(read_i32_le(data, "Int32")?);
        }
        LogicalType::Uint64 | LogicalType::Node(_) => {
            let b = downcast_builder_mut::<UInt64Builder>(builder, &arrow_type)?;
            b.append_value(read_u64_le(data, "UInt64")?);
        }
        LogicalType::Double => {
            let b = downcast_builder_mut::<Float64Builder>(builder, &arrow_type)?;
            b.append_value(read_f64_le(data, "Double")?);
        }
        LogicalType::Bool => {
            let b = downcast_builder_mut::<arrow::array::BooleanBuilder>(builder, &arrow_type)?;
            let val = *data.first().ok_or_else(|| {
                LightningError::Internal("Buffer too short for Bool: expected at least 1 byte".to_string())
            })?;
            b.append_value(val != 0);
        }
        LogicalType::String => {
            let b = downcast_builder_mut::<StringBuilder>(builder, &arrow_type)?;
            let prefix = *data.first().ok_or_else(|| {
                LightningError::Internal("Buffer too short for String: expected at least 1 byte".to_string())
            })?;
            let len = if prefix == 255 { 63 } else { prefix as usize };
            let actual_len = std::cmp::min(len, 63);
            let end = 1 + actual_len;
            if data.len() < end {
                return Err(LightningError::Internal(format!(
                    "Buffer too short for String: expected {end} bytes, got {}",
                    data.len()
                )));
            }
            b.append_value(std::str::from_utf8(&data[1..end]).unwrap_or(""));
        }
        LogicalType::Timestamp => {
            let b = downcast_builder_mut::<TimestampMicrosecondBuilder>(builder, &arrow_type)?;
            b.append_value(read_i64_le(data, "Timestamp")?);
        }
        LogicalType::Date => {
            let b = downcast_builder_mut::<Date32Builder>(builder, &arrow_type)?;
            b.append_value(read_i32_le(data, "Date")?);
        }
        LogicalType::List(_) => {
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
            array.as_any().downcast_ref::<Float64Array>().map(|a| Value::Number(a.value(i))).unwrap_or(Value::Null)
        }
        DataType::Utf8 => {
            array.as_any().downcast_ref::<StringArray>().map(|a| Value::String(a.value(i).to_string())).unwrap_or(Value::Null)
        }
        DataType::Boolean => {
            array.as_any().downcast_ref::<BooleanArray>().map(|a| Value::Boolean(a.value(i))).unwrap_or(Value::Null)
        }
        DataType::UInt64 => {
            array.as_any().downcast_ref::<UInt64Array>().map(|a| Value::Node(a.value(i))).unwrap_or(Value::Null)
        }
        DataType::Int64 => {
            array.as_any().downcast_ref::<Int64Array>().map(|a| Value::Number(a.value(i) as f64)).unwrap_or(Value::Null)
        }
        DataType::Int32 => {
            array.as_any().downcast_ref::<Int32Array>().map(|a| Value::Number(a.value(i) as f64)).unwrap_or(Value::Null)
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
    let dt = array.data_type();
    match dt {
        DataType::Int64 => {
            let a = downcast_array_ref::<Int64Array>(array.as_ref(), dt)?;
            downcast_builder_mut::<Int64Builder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Int32 => {
            let a = downcast_array_ref::<Int32Array>(array.as_ref(), dt)?;
            downcast_builder_mut::<Int32Builder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::UInt64 => {
            let a = downcast_array_ref::<UInt64Array>(array.as_ref(), dt)?;
            downcast_builder_mut::<UInt64Builder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Float64 => {
            let a = downcast_array_ref::<Float64Array>(array.as_ref(), dt)?;
            downcast_builder_mut::<Float64Builder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Boolean => {
            let a = downcast_array_ref::<BooleanArray>(array.as_ref(), dt)?;
            downcast_builder_mut::<BooleanBuilder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Utf8 => {
            let a = downcast_array_ref::<StringArray>(array.as_ref(), dt)?;
            downcast_builder_mut::<StringBuilder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Date32 => {
            let a = downcast_array_ref::<Date32Array>(array.as_ref(), dt)?;
            downcast_builder_mut::<Date32Builder>(builder, dt)?.append_value(a.value(idx));
        }
        DataType::Timestamp(_, _) => {
            let a = downcast_array_ref::<arrow::array::TimestampMicrosecondArray>(array.as_ref(), dt)?;
            downcast_builder_mut::<TimestampMicrosecondBuilder>(builder, dt)?.append_value(a.value(idx));
        }
        _ => {
            return Err(LightningError::Internal(format!(
                "Unsupported type for append_to_builder: {:?}",
                dt
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

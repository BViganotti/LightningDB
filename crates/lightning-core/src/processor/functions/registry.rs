use crate::processor::functions::ScalarFunction;
use arrow::array::{Array, ArrayRef};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct FunctionRegistry {
    pub scalar_functions: RwLock<HashMap<String, ScalarFunction>>,
    pub aggregate_functions: RwLock<
        HashMap<
            String,
            Box<dyn Fn() -> Box<dyn crate::processor::functions::AggregateFunction> + Send + Sync>,
        >,
    >,
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut scalar_functions = HashMap::new();
        let mut aggregate_functions: HashMap<
            String,
            Box<dyn Fn() -> Box<dyn crate::processor::functions::AggregateFunction> + Send + Sync>,
        > = HashMap::new();

        // Register Aggregates
        aggregate_functions.insert(
            "COUNT".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Count::new())),
        );
        aggregate_functions.insert(
            "COUNT_STAR".to_string(),
            Box::new(|| {
                Box::new(crate::processor::functions::aggregate_function::CountStar::new())
            }),
        );
        aggregate_functions.insert(
            "SUM".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Sum::new())),
        );
        aggregate_functions.insert(
            "AVG".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Avg::new())),
        );
        aggregate_functions.insert(
            "MIN".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Min::new())),
        );
        aggregate_functions.insert(
            "MAX".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Max::new())),
        );
        aggregate_functions.insert(
            "COLLECT".to_string(),
            Box::new(|| Box::new(crate::processor::functions::aggregate_function::Collect::new())),
        );
        aggregate_functions.insert(
            "COUNT_DISTINCT".to_string(),
            Box::new(|| {
                Box::new(
                    crate::processor::functions::aggregate_function::CountDistinct::new(),
                )
            }),
        );
        // Define UPPER — Unicode-aware via char::to_uppercase
        scalar_functions.insert(
            "UPPER".to_string(),
            ScalarFunction::new(
                "UPPER".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "UPPER requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "UPPER expects a String argument".into(),
                            )
                        })?;
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt| opt.map(|s| s.to_uppercase()))
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define LOWER — Unicode-aware via char::to_lowercase
        scalar_functions.insert(
            "LOWER".to_string(),
            ScalarFunction::new(
                "LOWER".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "LOWER requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "LOWER expects a String argument".into(),
                            )
                        })?;
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt| opt.map(|s| s.to_lowercase()))
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define LOWER — vectorized via direct buffer access
        scalar_functions.insert(
            "LOWER".to_string(),
            ScalarFunction::new(
                "LOWER".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "LOWER requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "LOWER expects a String argument".into(),
                            )
                        })?;
                    let offsets = string_array.value_offsets();
                    let values = string_array.value_data();
                    let mut out_values = Vec::<u8>::with_capacity(values.len());
                    let mut out_offsets = Vec::<i32>::with_capacity(offsets.len());
                    out_offsets.push(0);
                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            out_offsets.push(out_offsets.last().copied().unwrap_or(0));
                            continue;
                        }
                        let start = offsets[i] as usize;
                        let end = offsets[i + 1] as usize;
                        let s = &values[start..end];
                        for &b in s {
                            out_values.push(b.to_ascii_lowercase());
                        }
                        out_offsets.push(out_values.len() as i32);
                    }
                    let result = unsafe {
                        arrow::array::StringArray::new_unchecked(
                            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(out_offsets)),
                            out_values.into(),
                            None,
                        )
                    };
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define CAST
        scalar_functions.insert(
            "CAST".to_string(),
            ScalarFunction::new(
                "CAST".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "CAST requires 2 arguments (value, target_type)".into(),
                        ));
                    }
                    let target_type_str = args[1]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .and_then(|a| if a.len() > 0 { Some(a.value(0)) } else { None })
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "CAST expects second argument to be a type name string".into(),
                            )
                        })?;

                    let target_type = match target_type_str.to_uppercase().as_str() {
                        "INT64" | "BIGINT" => arrow::datatypes::DataType::Int64,
                        "INT32" | "INTEGER" => arrow::datatypes::DataType::Int32,
                        "DOUBLE" | "FLOAT8" => arrow::datatypes::DataType::Float64,
                        "FLOAT" | "FLOAT4" => arrow::datatypes::DataType::Float32,
                        "STRING" | "TEXT" => arrow::datatypes::DataType::Utf8,
                        "BOOL" | "BOOLEAN" => arrow::datatypes::DataType::Boolean,
                        "DATE" => arrow::datatypes::DataType::Date32,
                        "TIMESTAMP" => arrow::datatypes::DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                        _ => {
                            return Err(crate::LightningError::Internal(
                                format!("Unsupported target type for CAST: {target_type_str}"),
                            ))
                        }
                    };

                    arrow::compute::cast(&args[0], &target_type)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        // Define ABS
        scalar_functions.insert(
            "ABS".to_string(),
            ScalarFunction::new(
                "ABS".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "ABS requires 1 argument".into(),
                        ));
                    }
                    use arrow::compute::cast;
                    use arrow::datatypes::DataType;
                    let arg = cast(&args[0], &DataType::Float64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let f64_arg = arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let result: arrow::array::Float64Array =
                        f64_arg.iter().map(|opt_n| opt_n.map(|n| n.abs())).collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define CEIL, FLOOR, ROUND
        for name in &["CEIL", "FLOOR", "ROUND"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, _num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        use arrow::compute::cast;
                        use arrow::datatypes::DataType;
                        let arg = cast(&args[0], &DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let f64_arg = arg
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .expect("type mismatch in function");
                        let result: arrow::array::Float64Array = match *name {
                            "CEIL" => f64_arg
                                .iter()
                                .map(|opt_n| opt_n.map(|n| n.ceil()))
                                .collect(),
                            "FLOOR" => f64_arg
                                .iter()
                                .map(|opt_n| opt_n.map(|n| n.floor()))
                                .collect(),
                            "ROUND" => f64_arg
                                .iter()
                                .map(|opt_n| opt_n.map(|n| n.round()))
                                .collect(),
                            _ => unreachable!(),
                        };
                        Ok(Arc::new(result))
                    }),
                ),
            );
        }

        // Define COALESCE
        scalar_functions.insert(
            "COALESCE".to_string(),
            ScalarFunction::new(
                "COALESCE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.is_empty() {
                        return Err(crate::LightningError::Internal(
                            "COALESCE requires at least 1 argument".into(),
                        ));
                    }
                    let target_type = args.iter()
                        .find(|a| a.data_type() != &arrow::datatypes::DataType::Null)
                        .map(|a| a.data_type().clone())
                        .unwrap_or_else(|| args[0].data_type().clone());

                    // Cast ALL arguments to the target type ONCE (not per row).
                    // This turns O(num_rows × num_args) casts into O(num_args) casts.
                    let casted_args: Vec<ArrayRef> = args.iter()
                        .map(|arg| {
                            if arg.data_type() == &target_type {
                                Ok(arg.clone())
                            } else {
                                arrow::compute::cast(arg, &target_type)
                                    .map_err(|e| crate::LightningError::Internal(e.to_string()))
                            }
                        })
                        .collect::<std::result::Result<Vec<_>, crate::LightningError>>()?;

                    let mut builder = arrow::array::make_builder(&target_type, num_rows);
                    for i in 0..num_rows {
                        let mut found = false;
                        for arg in &casted_args {
                            if !arg.is_null(i) {
                                crate::processor::arrow_utils::append_to_builder(
                                    &mut *builder,
                                    arg,
                                    i,
                                )?;
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            crate::processor::arrow_utils::append_null_to_builder(
                                &mut *builder,
                                &target_type,
                            )?;
                        }
                    }
                    Ok(builder.finish())
                }),
            ),
        );

        // Define IFNULL, ISNULL — return first non-null argument
        for name in &["IFNULL", "ISNULL"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 2 {
                            return Err(crate::LightningError::Internal(format!(
                                "{name} requires 2 arguments"
                            )));
                        }
                        let target_type = args[0].data_type().clone();
                        let mut result = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if !args[0].is_null(i) {
                                result.push(crate::processor::Value::from_arrow(&args[0], i));
                            } else {
                                result.push(crate::processor::Value::from_arrow(&args[1], i));
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(&result, &target_type))
                    }),
                ),
            );
        }

        // Define NULLIF — return null if two arguments are equal
        scalar_functions.insert(
            "NULLIF".to_string(),
            ScalarFunction::new(
                "NULLIF".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "NULLIF requires 2 arguments".into(),
                        ));
                    }
                    let target_type = args[0].data_type().clone();
                    let mut result = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let v1 = crate::processor::Value::from_arrow(&args[0], i);
                        let v2 = crate::processor::Value::from_arrow(&args[1], i);
                        if v1 == v2 {
                            result.push(crate::processor::Value::Null);
                        } else {
                            result.push(v1);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(&result, &target_type))
                }),
            ),
        );

        // Define IF, IIF — inline conditional: IF(condition, true_val, false_val)
        for name in &["IF", "IIF"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 3 {
                            return Err(crate::LightningError::Internal(format!(
                                "{name} requires 3 arguments (condition, true_value, false_value)"
                            )));
                        }
                        let cond = args[0].as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .ok_or_else(|| crate::LightningError::Internal(
                                "IF/IIF first argument must be boolean".into()
                            ))?;
                        let target_type = args[1].data_type().clone();
                        let mut result = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if cond.is_null(i) || !cond.value(i) {
                                result.push(crate::processor::Value::from_arrow(&args[2], i));
                            } else {
                                result.push(crate::processor::Value::from_arrow(&args[1], i));
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(&result, &target_type))
                    }),
                ),
            );
        }

        // Define REVERSE
        scalar_functions.insert(
            "REVERSE".to_string(),
            ScalarFunction::new(
                "REVERSE".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "REVERSE requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "REVERSE expects a String argument".into(),
                            )
                        })?;
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt_str| opt_str.map(|s| s.chars().rev().collect::<String>()))
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define LENGTH / SIZE
        for name in &["LENGTH", "SIZE"] {
            scalar_functions.insert(
                name.to_string(),
                ScalarFunction::new(
                    name.to_string(),
                    Arc::new(move |args, _num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        if let Some(string_array) =
                            args[0].as_any().downcast_ref::<arrow::array::StringArray>()
                        {
                            let result: arrow::array::Int64Array = string_array
                                .iter()
                                .map(|opt_str| opt_str.map(|s| s.len() as i64))
                                .collect();
                            Ok(Arc::new(result))
                        } else if let Some(list_array) =
                            args[0].as_any().downcast_ref::<arrow::array::ListArray>()
                        {
                            let result: arrow::array::Int64Array = list_array
                                .iter()
                                .map(|opt_list| opt_list.map(|l| l.len() as i64))
                                .collect();
                            Ok(Arc::new(result))
                        } else {
                            Err(crate::LightningError::Internal(
                                format!("{name} expects a String or List argument"),
                            ))
                        }
                    }),
                ),
            );
        }

        // Define CONCAT
        scalar_functions.insert(
            "CONCAT".to_string(),
            ScalarFunction::new(
                "CONCAT".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.is_empty() {
                        return Err(crate::LightningError::Internal(
                            "CONCAT requires at least 1 argument".into(),
                        ));
                    }
                    let mut result_vec = vec![String::new(); args[0].len()];
                    for arg in args {
                        let string_array =
                            arrow::compute::cast(arg, &arrow::datatypes::DataType::Utf8)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let s_array = string_array
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .expect("type mismatch in function");
                        for i in 0..args[0].len() {
                            if !s_array.is_null(i) {
                                result_vec[i].push_str(s_array.value(i));
                            }
                        }
                    }
                    Ok(Arc::new(arrow::array::StringArray::from(result_vec)))
                }),
            ),
        );

        // Define DATE and TIMESTAMP stubs that parse strings
        scalar_functions.insert(
            "DATE".to_string(),
            ScalarFunction::new(
                "DATE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.is_empty() {
                        let now = chrono::Utc::now().date_naive();
                        let days = (now - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time"))
                            .num_days() as i32;
                        return Ok(Arc::new(arrow::array::Date32Array::from(vec![
                            days;
                            num_rows
                        ])));
                    }
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "DATE requires 0 or 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal("DATE expects a String argument".into())
                        })?;
                    let mut builder =
                        arrow::array::Date32Builder::with_capacity(string_array.len());
                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            builder.append_null();
                        } else {
                            let s = string_array.value(i);
                            // Simple parse: YYYY-MM-DD
                            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time");
                                let days = d.signed_duration_since(epoch).num_days();
                                builder.append_value(days as i32);
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }),
            ),
        );

        scalar_functions.insert(
            "TIMESTAMP".to_string(),
            ScalarFunction::new(
                "TIMESTAMP".to_string(),
                Arc::new(|args, num_rows| {
                    if args.is_empty() {
                        let now = chrono::Utc::now().timestamp_micros();
                        return Ok(Arc::new(arrow::array::TimestampMicrosecondArray::from(
                            vec![now; num_rows],
                        )));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "TIMESTAMP expects a String argument".into(),
                            )
                        })?;
                    let mut builder = arrow::array::TimestampMicrosecondBuilder::with_capacity(
                        string_array.len(),
                    );
                    for i in 0..string_array.len() {
                        if string_array.is_null(i) {
                            builder.append_null();
                        } else {
                            let s = string_array.value(i);
                            if let Ok(dt) =
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                            {
                                builder.append_value(dt.and_utc().timestamp_micros());
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }),
            ),
        );

        // Define TO_STRING / TO_INT
        scalar_functions.insert(
            "TO_STRING".to_string(),
            ScalarFunction::new(
                "TO_STRING".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_STRING requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        scalar_functions.insert(
            "TO_INT".to_string(),
            ScalarFunction::new(
                "TO_INT".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_INT requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Int64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        // Define SUBSTRING
        scalar_functions.insert(
            "SUBSTRING".to_string(),
            ScalarFunction::new(
                "SUBSTRING".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() < 2 || args.len() > 3 {
                        return Err(crate::LightningError::Internal(
                            "SUBSTRING requires 2 or 3 arguments".into(),
                        ));
                    }
                    let s_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let start_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let len_arg = if args.len() == 3 {
                        Some(
                            arrow::compute::cast(&args[2], &arrow::datatypes::DataType::Int64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?,
                        )
                    } else {
                        None
                    };

                    let s_arr = s_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let start_arr = start_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let mut result = arrow::array::StringBuilder::new();
                    for i in 0..s_arr.len() {
                        if s_arr.is_null(i) || start_arr.is_null(i) {
                            result.append_null();
                            continue;
                        }
                        let s = s_arr.value(i);
                        let start = start_arr.value(i) as usize;
                        let sub = if let Some(ref l) = len_arg {
                            let l_arr = l
                                .as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .expect("type mismatch in function");
                            if l_arr.is_null(i) {
                                result.append_null();
                                continue;
                            }
                            let len = l_arr.value(i) as usize;
                            s.chars().skip(start).take(len).collect::<String>()
                        } else {
                            s.chars().skip(start).collect::<String>()
                        };
                        result.append_value(sub);
                    }
                    Ok(Arc::new(result.finish()))
                }),
            ),
        );

        // Define REPLACE
        scalar_functions.insert(
            "REPLACE".to_string(),
            ScalarFunction::new(
                "REPLACE".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 3 {
                        return Err(crate::LightningError::Internal(
                            "REPLACE requires 3 arguments".into(),
                        ));
                    }
                    let s_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let from_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Utf8)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let to_arg = arrow::compute::cast(&args[2], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let s_arr = s_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let from_arr = from_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let to_arr = to_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let mut result = arrow::array::StringBuilder::new();
                    for i in 0..s_arr.len() {
                        if s_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                            result.append_null();
                            continue;
                        }
                        result.append_value(
                            s_arr.value(i).replace(from_arr.value(i), to_arr.value(i)),
                        );
                    }
                    Ok(Arc::new(result.finish()))
                }),
            ),
        );

        // Define ID / LABELS / KEYS
        scalar_functions.insert(
            "ID".to_string(),
            ScalarFunction::new(
                "ID".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "ID requires 1 argument".into(),
                        ));
                    }
                    Ok(args[0].clone())
                }),
            ),
        );

        scalar_functions.insert(
            "LABELS".to_string(),
            ScalarFunction::new(
                "LABELS".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "LABELS requires 1 argument".into(),
                        ));
                    }
                    // Return empty list as fallback
                    let num_rows = args[0].len();
                    let field = Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ));
                    let offsets = arrow::buffer::OffsetBuffer::from_lengths(vec![0; num_rows]);
                    let values = arrow::array::new_empty_array(&arrow::datatypes::DataType::Utf8);
                    let list_array = arrow::array::ListArray::try_new(field, offsets, values, None)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    Ok(Arc::new(list_array))
                }),
            ),
        );

        scalar_functions.insert(
            "KEYS".to_string(),
            ScalarFunction::new(
                "KEYS".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "KEYS requires 1 argument".into(),
                        ));
                    }
                    // Return empty list as fallback
                    let num_rows = args[0].len();
                    let field = Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ));
                    let offsets = arrow::buffer::OffsetBuffer::from_lengths(vec![0; num_rows]);
                    let values = arrow::array::new_empty_array(&arrow::datatypes::DataType::Utf8);
                    let list_array = arrow::array::ListArray::try_new(field, offsets, values, None)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    Ok(Arc::new(list_array))
                }),
            ),
        );

        // Define LIST_CONTAINS
        scalar_functions.insert(
            "LIST_CONTAINS".to_string(),
            ScalarFunction::new(
                "LIST_CONTAINS".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "LIST_CONTAINS requires 2 arguments".into(),
                        ));
                    }
                    let list_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "LIST_CONTAINS expects a List as first argument".into(),
                            )
                        })?;
                    let value_array = &args[1];
                    let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if list_array.is_null(i) || value_array.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        let list = list_array.value(i);
                        let val = crate::processor::Value::from_arrow(value_array, i);
                        let mut found = false;
                        for j in 0..list.len() {
                            let list_val = crate::processor::Value::from_arrow(&list, j);
                            if list_val == val {
                                found = true;
                                break;
                            }
                        }
                        results.append_value(found);
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define RANGE
        scalar_functions.insert(
            "RANGE".to_string(),
            ScalarFunction::new(
                "RANGE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() < 2 || args.len() > 3 {
                        return Err(crate::LightningError::Internal(
                            "RANGE requires 2 or 3 arguments".into(),
                        ));
                    }
                    let start_arg =
                        arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let end_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let step_arg = if args.len() == 3 {
                        Some(
                            arrow::compute::cast(&args[2], &arrow::datatypes::DataType::Int64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?,
                        )
                    } else {
                        None
                    };

                    let start_arr = start_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let end_arr = end_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let step_arr = step_arg.as_ref().map(|a| {
                        a.as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("type mismatch in function")
                    });

                    let mut list_builders = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if start_arr.is_null(i)
                            || end_arr.is_null(i)
                            || step_arr.map(|a| a.is_null(i)).unwrap_or(false)
                        {
                            list_builders.push(crate::processor::Value::Null);
                            continue;
                        }
                        let start = start_arr.value(i);
                        let end = end_arr.value(i);
                        let step = step_arr.map(|a| a.value(i)).unwrap_or(1);

                        if step == 0 {
                            return Err(crate::LightningError::Internal(
                                "RANGE step cannot be 0".into(),
                            ));
                        }

                        const RANGE_MAX_ELEMS: u64 = 10_000_000;
                        let estimated = if step > 0 {
                            if start > end { 0 } else { ((end as i128 - start as i128) / step as i128 + 1) as u64 }
                        } else {
                            if start < end { 0 } else { ((start as i128 - end as i128) / (-step as i128) + 1) as u64 }
                        };
                        if estimated > RANGE_MAX_ELEMS {
                            return Err(crate::LightningError::Query(
                                format!("RANGE would produce {} elements (max {})", estimated, RANGE_MAX_ELEMS)
                            ));
                        }
                        let mut range_vals = Vec::with_capacity(estimated as usize);
                        let mut curr = start;
                        if step > 0 {
                            while curr <= end {
                                range_vals.push(crate::processor::Value::Number(curr as f64));
                                curr += step;
                            }
                        } else {
                            while curr >= end {
                                range_vals.push(crate::processor::Value::Number(curr as f64));
                                curr += step;
                            }
                        }
                        list_builders.push(crate::processor::Value::List(range_vals));
                    }

                    Ok(crate::processor::arrow_utils::values_to_array(
                        &list_builders,
                        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                            "item",
                            arrow::datatypes::DataType::Int64,
                            true,
                        ))),
                    ))
                }),
            ),
        );

        // Define LIST_APPEND, LIST_PREPEND, LIST_CONCAT
        for name in &["LIST_APPEND", "LIST_PREPEND", "LIST_CONCAT"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 2 arguments"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let v1 = crate::processor::Value::from_arrow(&args[0], i);
                            let v2 = crate::processor::Value::from_arrow(&args[1], i);

                            match (v1, v2) {
                                (crate::processor::Value::List(mut l), val)
                                    if *name == "LIST_APPEND" =>
                                {
                                    l.push(val);
                                    results.push(crate::processor::Value::List(l));
                                }
                                (crate::processor::Value::List(mut l), val)
                                    if *name == "LIST_PREPEND" =>
                                {
                                    l.insert(0, val);
                                    results.push(crate::processor::Value::List(l));
                                }
                                (
                                    crate::processor::Value::List(mut l1),
                                    crate::processor::Value::List(l2),
                                ) if *name == "LIST_CONCAT" => {
                                    l1.extend(l2);
                                    results.push(crate::processor::Value::List(l1));
                                }
                                _ => results.push(crate::processor::Value::Null),
                            }
                        }
                        // Determine Arrow inner type for the result ListArray
                        let mut element_type = arrow::datatypes::DataType::Null;
                        for res in &results {
                            if let crate::processor::Value::List(l) = res {
                                if !l.is_empty() {
                                    // Picking a safe default for any-typed lists in Lightning
                                    element_type =
                                        crate::processor::arrow_utils::logical_type_to_arrow_type(
                                            &lightning_types::LogicalType::Any,
                                        );
                                    break;
                                }
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::List(Arc::new(
                                arrow::datatypes::Field::new("item", element_type, true),
                            )),
                        ))
                    }),
                ),
            );
        }

        // Define INITCAP
        scalar_functions.insert(
            "INITCAP".to_string(),
            ScalarFunction::new(
                "INITCAP".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "INITCAP requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "INITCAP expects a String argument".into(),
                            )
                        })?;
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt_str| {
                            opt_str.map(|s| {
                                let mut c = s.chars();
                                match c.next() {
                                    None => String::new(),
                                    Some(f) => {
                                        f.to_uppercase().collect::<String>()
                                            + &c.as_str().to_lowercase()
                                    }
                                }
                            })
                        })
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define TRIM, LTRIM, RTRIM
        for name in &["TRIM", "LTRIM", "RTRIM"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, _num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} expects a String argument"),
                            ));
                        }
                        let string_array = args[0]
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .ok_or_else(|| {
                                crate::LightningError::Internal(
                                    format!("{name} expects a String argument"),
                                )
                            })?;
                        let result: arrow::array::StringArray = string_array
                            .iter()
                            .map(|opt_str| {
                                opt_str.map(|s| match *name {
                                    "TRIM" => s.trim().to_string(),
                                    "LTRIM" => s.trim_start().to_string(),
                                    "RTRIM" => s.trim_end().to_string(),
                                    _ => unreachable!(),
                                })
                            })
                            .collect();
                        Ok(Arc::new(result))
                    }),
                ),
            );
        }

        // Define CONTAINS, STARTS_WITH, ENDS_WITH
        for name in &["CONTAINS", "STARTS_WITH", "ENDS_WITH"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 2 arguments"),
                            ));
                        }
                        let base =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let pattern =
                            arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Utf8)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let base_arr = base
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .expect("type mismatch in function");
                        let pattern_arr = pattern
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if base_arr.is_null(i) || pattern_arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let s = base_arr.value(i);
                            let p = pattern_arr.value(i);
                            if i == 0 {
                                tracing::debug!("CONTAINS: first row s='{}', p='{}', result={}", s, p, s.contains(p));
                            }
                            let res = match *name {
                                "CONTAINS" => s.contains(p),
                                "STARTS_WITH" => s.starts_with(p),
                                "ENDS_WITH" => s.ends_with(p),
                                _ => unreachable!(),
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define LEFT, RIGHT
        for name in &["LEFT", "RIGHT"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 2 arguments"),
                            ));
                        }
                        let s_arg =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let n_arg =
                            arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let s_arr = s_arg
                            .as_any()
                            .downcast_ref::<arrow::array::StringArray>()
                            .expect("type mismatch in function");
                        let n_arr = n_arg
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("type mismatch in function");
                        let mut results =
                            arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 8);
                        for i in 0..num_rows {
                            if s_arr.is_null(i) || n_arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let s = s_arr.value(i);
                            let n = std::cmp::max(0, n_arr.value(i)) as usize;
                            let res = if *name == "LEFT" {
                                s.chars().take(n).collect::<String>()
                            } else {
                                let len = s.chars().count();
                                let start = len.saturating_sub(n);
                                s.chars().skip(start).collect::<String>()
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define REPEAT
        scalar_functions.insert(
            "REPEAT".to_string(),
            ScalarFunction::new(
                "REPEAT".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "REPEAT requires 2 arguments".into(),
                        ));
                    }
                    let s_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let n_arg = arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let s_arr = s_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let n_arr = n_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for i in 0..num_rows {
                        if s_arr.is_null(i) || n_arr.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        let s = s_arr.value(i);
                        let n = std::cmp::max(0, n_arr.value(i)) as usize;
                        results.append_value(s.repeat(n));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define CURRENT_DATE, CURRENT_TIMESTAMP
        scalar_functions.insert(
            "CURRENT_DATE".to_string(),
            ScalarFunction::new(
                "CURRENT_DATE".to_string(),
                Arc::new(|_args, num_rows| {
                    let now = chrono::Utc::now().date_naive();
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time");
                    let days = now.signed_duration_since(epoch).num_days() as i32;
                    Ok(Arc::new(arrow::array::Date32Array::from(vec![
                        days;
                        num_rows
                    ])))
                }),
            ),
        );

        scalar_functions.insert(
            "CURRENT_TIMESTAMP".to_string(),
            ScalarFunction::new(
                "CURRENT_TIMESTAMP".to_string(),
                Arc::new(|_args, num_rows| {
                    let now = chrono::Utc::now().timestamp_micros();
                    Ok(Arc::new(arrow::array::TimestampMicrosecondArray::from(
                        vec![now; num_rows],
                    )))
                }),
            ),
        );

        // Define SQRT, LOG, LN, EXP, SIN, COS, TAN
        for name in &["SQRT", "LOG", "LN", "EXP", "SIN", "COS", "TAN", "ACOS", "ASIN", "ATAN", "COT", "SIGN"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let n_arg =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let n_arr = n_arg
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .expect("type mismatch in function");
                        // Vectorized: use Arrow unary kernel instead of per-row loop
                        use arrow::compute::kernels::arity::unary;
                        use arrow::datatypes::Float64Type;
                        let result: arrow::array::PrimitiveArray<Float64Type> = match *name {
                            "SQRT" => unary(n_arr, |n: f64| n.sqrt()),
                            "LOG" => unary(n_arr, |n: f64| n.log10()),
                            "LN" => unary(n_arr, |n: f64| n.ln()),
                            "EXP" => unary(n_arr, |n: f64| n.exp()),
                            "SIN" => unary(n_arr, |n: f64| n.sin()),
                            "COS" => unary(n_arr, |n: f64| n.cos()),
                            "TAN" => unary(n_arr, |n: f64| n.tan()),
                            "ACOS" => unary(n_arr, |n: f64| n.acos()),
                            "ASIN" => unary(n_arr, |n: f64| n.asin()),
                            "ATAN" => unary(n_arr, |n: f64| n.atan()),
                            "COT" => unary(n_arr, |n: f64| 1.0 / n.tan()),
                            "SIGN" => unary(n_arr, |n: f64| n.signum()),
                            _ => unreachable!(),
                        };
                        Ok(Arc::new(result))
                    }),
                ),
            );
        }

        // Define YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
        for name in &["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let val = crate::processor::Value::from_arrow(&args[0], i);
                            match val {
                                crate::processor::Value::Date(days) => {
                                    let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                        + chrono::Duration::days(days as i64);
                                    use chrono::Datelike;
                                    let res = match *name {
                                        "YEAR" => dt.year() as i64,
                                        "MONTH" => dt.month() as i64,
                                        "DAY" => dt.day() as i64,
                                        _ => 0,
                                    };
                                    results.append_value(res);
                                }
                                crate::processor::Value::Timestamp(micros) => {
                                    let dt = chrono::DateTime::from_timestamp_micros(micros)
                                        .expect("type mismatch in function")
                                        .naive_utc();
                                    use chrono::{Datelike, Timelike};
                                    let res = match *name {
                                        "YEAR" => dt.year() as i64,
                                        "MONTH" => dt.month() as i64,
                                        "DAY" => dt.day() as i64,
                                        "HOUR" => dt.hour() as i64,
                                        "MINUTE" => dt.minute() as i64,
                                        "SECOND" => dt.second() as i64,
                                        _ => 0,
                                    };
                                    results.append_value(res);
                                }
                                _ => results.append_null(),
                            }
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define POW, MOD
        scalar_functions.insert(
            "POW".to_string(),
            ScalarFunction::new(
                "POW".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "POW requires 2 arguments".into(),
                        ));
                    }
                    let b_arg =
                        arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let e_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let b_arr = b_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let e_arr = e_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if b_arr.is_null(i) || e_arr.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        results.append_value(b_arr.value(i).powf(e_arr.value(i)));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        scalar_functions.insert(
            "MOD".to_string(),
            ScalarFunction::new(
                "MOD".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MOD requires 2 arguments".into(),
                        ));
                    }
                    let n_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Int64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let d_arg = arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let n_arr = n_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let d_arr = d_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("type mismatch in function");
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if n_arr.is_null(i) || d_arr.is_null(i) || d_arr.value(i) == 0 {
                            results.append_null();
                            continue;
                        }
                        results.append_value(n_arr.value(i) % d_arr.value(i));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define SPLIT
        scalar_functions.insert(
            "SPLIT".to_string(),
            ScalarFunction::new(
                "SPLIT".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "SPLIT requires 2 arguments".into(),
                        ));
                    }
                    let s_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let d_arg = arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let s_arr = s_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let d_arr = d_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");

                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if s_arr.is_null(i) || d_arr.is_null(i) {
                            results.push(crate::processor::Value::Null);
                            continue;
                        }
                        let parts: Vec<crate::processor::Value> = s_arr
                            .value(i)
                            .split(d_arr.value(i))
                            .map(|p| crate::processor::Value::String(p.to_string()))
                            .collect();
                        results.push(crate::processor::Value::List(parts));
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                            "item",
                            arrow::datatypes::DataType::Utf8,
                            true,
                        ))),
                    ))
                }),
            ),
        );

        // Define RADIANS, DEGREES

        // Define BIT_NOT
        scalar_functions.insert(
            "MD5".to_string(),
            ScalarFunction::new(
                "MD5".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "MD5 requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal("MD5 expects a String argument".into())
                        })?;
                    use md5::{Digest, Md5};
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt_str| {
                            opt_str.map(|s| {
                                let mut hasher = Md5::new();
                                hasher.update(s.as_bytes());
                                format!("{:x}", hasher.finalize())
                            })
                        })
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define SHA256
        scalar_functions.insert(
            "SHA256".to_string(),
            ScalarFunction::new(
                "SHA256".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "SHA256 requires 1 argument".into(),
                        ));
                    }
                    let string_array = args[0]
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            crate::LightningError::Internal(
                                "SHA256 expects a String argument".into(),
                            )
                        })?;
                    use sha2::{Digest, Sha256};
                    let result: arrow::array::StringArray = string_array
                        .iter()
                        .map(|opt_str| {
                            opt_str.map(|s| {
                                let mut hasher = Sha256::new();
                                hasher.update(s.as_bytes());
                                format!("{:x}", hasher.finalize())
                            })
                        })
                        .collect();
                    Ok(Arc::new(result))
                }),
            ),
        );

        // Define LIST_EXTRACT
        scalar_functions.insert(
            "LIST_EXTRACT".to_string(),
            ScalarFunction::new(
                "LIST_EXTRACT".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "LIST_EXTRACT requires 2 arguments".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let list_val = crate::processor::Value::from_arrow(&args[0], i);
                        let idx_val = crate::processor::Value::from_arrow(&args[1], i);
                        match (list_val, idx_val) {
                            (
                                crate::processor::Value::List(l),
                                crate::processor::Value::Number(idx),
                            ) => {
                                let len = l.len();
                                // Handle negative index (Python/JS-style: -1 = last element)
                                let idx = if idx >= 0.0 {
                                    idx as usize
                                } else {
                                    let neg = (-idx) as usize;
                                    if neg > len {
                                        usize::MAX // will be caught by bounds check
                                    } else {
                                        len - neg
                                    }
                                };
                                if idx < len {
                                    results.push(l[idx].clone());
                                } else {
                                    results.push(crate::processor::Value::Null);
                                }
                            }
                            _ => results.push(crate::processor::Value::Null),
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define LIST_SLICE
        scalar_functions.insert(
            "LIST_SLICE".to_string(),
            ScalarFunction::new(
                "LIST_SLICE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() < 2 || args.len() > 3 {
                        return Err(crate::LightningError::Internal(
                            "LIST_SLICE requires 2 or 3 arguments".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let list_val = crate::processor::Value::from_arrow(&args[0], i);
                        let start_val = crate::processor::Value::from_arrow(&args[1], i);
                        let end_val = if args.len() == 3 {
                            Some(crate::processor::Value::from_arrow(&args[2], i))
                        } else {
                            None
                        };

                        match (list_val, start_val) {
                            (
                                crate::processor::Value::List(l),
                                crate::processor::Value::Number(start),
                            ) => {
                                let start = std::cmp::max(0, (start as i64) - 1) as usize; // 1-based indexing parity with Cypher/Ladybug
                                let end = if let Some(crate::processor::Value::Number(e)) = end_val
                                {
                                    std::cmp::min(l.len() as i64, e as i64) as usize
                                } else {
                                    l.len()
                                };
                                if start < end && start < l.len() {
                                    results.push(crate::processor::Value::List(
                                        l[start..end].to_vec(),
                                    ));
                                } else {
                                    results.push(crate::processor::Value::List(vec![]));
                                }
                            }
                            _ => results.push(crate::processor::Value::Null),
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                            "item",
                            arrow::datatypes::DataType::Null,
                            true,
                        ))),
                    ))
                }),
            ),
        );

        // Define LIST_DISTINCT, LIST_SORT, LIST_REVERSE
        for name in &["LIST_DISTINCT", "LIST_SORT", "LIST_REVERSE"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let val = crate::processor::Value::from_arrow(&args[0], i);
                            if let crate::processor::Value::List(mut l) = val {
                                match *name {
                                    "LIST_DISTINCT" => {
                                        let mut seen = std::collections::HashSet::new();
                                        l.retain(|x| seen.insert(x.clone()));
                                        results.push(crate::processor::Value::List(l));
                                    }
                                    "LIST_SORT" => {
                                        l.sort_by(|a, b| {
                                            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                        });
                                        results.push(crate::processor::Value::List(l));
                                    }
                                    "LIST_REVERSE" => {
                                        l.reverse();
                                        results.push(crate::processor::Value::List(l));
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::List(Arc::new(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Null,
                                    true,
                                ),
                            )),
                        ))
                    }),
                ),
            );
        }

        // Define MAP_KEYS, MAP_VALUES, MAP_EXTRACT
        for name in &["MAP_KEYS", "MAP_VALUES", "MAP_EXTRACT"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if *name == "MAP_EXTRACT" && args.len() != 2 {
                            return Err(crate::LightningError::Internal(
                                "MAP_EXTRACT requires 2 arguments".into(),
                            ));
                        }
                        if (*name == "MAP_KEYS" || *name == "MAP_VALUES") && args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }

                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let val = crate::processor::Value::from_arrow(&args[0], i);
                            if let crate::processor::Value::Map(m) = val {
                                match *name {
                                    "MAP_KEYS" => {
                                        let keys: Vec<crate::processor::Value> =
                                            m.keys().cloned().collect();
                                        results.push(crate::processor::Value::List(keys));
                                    }
                                    "MAP_VALUES" => {
                                        let values: Vec<crate::processor::Value> =
                                            m.values().cloned().collect();
                                        results.push(crate::processor::Value::List(values));
                                    }
                                    "MAP_EXTRACT" => {
                                        let key = crate::processor::Value::from_arrow(&args[1], i);
                                        results.push(
                                            m.get(&key)
                                                .cloned()
                                                .unwrap_or(crate::processor::Value::Null),
                                        );
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::Null,
                        ))
                    }),
                ),
            );
        }

        // Define DATE_ADD, DATE_SUB
        for name in &["DATE_ADD", "DATE_SUB"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 3 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 3 arguments (date, count, unit)"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let d_val = crate::processor::Value::from_arrow(&args[0], i);
                            let c_val = crate::processor::Value::from_arrow(&args[1], i);
                            let u_val = crate::processor::Value::from_arrow(&args[2], i);

                            if let (
                                crate::processor::Value::Date(days),
                                crate::processor::Value::Number(count),
                                crate::processor::Value::String(unit),
                            ) = (d_val, c_val, u_val)
                            {
                                let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                    + chrono::Duration::days(days as i64);
                                let count = if *name == "DATE_ADD" {
                                    count as i64
                                } else {
                                    -(count as i64)
                                };
                                let res_dt = match unit.to_uppercase().as_str() {
                                    "DAY" | "DAYS" => Some(dt + chrono::Duration::days(count)),
                                    "WEEK" | "WEEKS" => Some(dt + chrono::Duration::weeks(count)),
                                    "MONTH" | "MONTHS" => dt
                                        .checked_add_months(chrono::Months::new(count.unsigned_abs() as u32))
                                        .map(|d| {
                                            if count < 0 {
                                                dt.checked_sub_months(chrono::Months::new(
                                                    count.unsigned_abs() as u32,
                                                ))
                                                .expect("type mismatch in function")
                                            } else {
                                                d
                                            }
                                        }),
                                    _ => None,
                                };
                                if let Some(rdt) = res_dt {
                                    let epoch =
                                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time");
                                    results.push(crate::processor::Value::Date(
                                        rdt.signed_duration_since(epoch).num_days() as i32,
                                    ));
                                } else {
                                    results.push(crate::processor::Value::Null);
                                }
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::Date32,
                        ))
                    }),
                ),
            );
        }

        // Define MAP_CREATE
        scalar_functions.insert(
            "MAP_CREATE".to_string(),
            ScalarFunction::new(
                "MAP_CREATE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MAP_CREATE requires 2 arguments (keys, values)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let keys_val = crate::processor::Value::from_arrow(&args[0], i);
                        let values_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::List(ks),
                            crate::processor::Value::List(vs),
                        ) = (keys_val, values_val)
                        {
                            let mut m = std::collections::HashMap::new();
                            for (k, v) in ks.into_iter().zip(vs.into_iter()) {
                                m.insert(k, v);
                            }
                            results.push(crate::processor::Value::Map(m));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT
        for name in &[
            "BIT_AND",
            "BIT_OR",
            "BIT_XOR",
            "BIT_SHIFT_LEFT",
            "BIT_SHIFT_RIGHT",
        ] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 2 arguments"),
                            ));
                        }
                        let a1 = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let a2 = arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let arr1 = a1
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("type mismatch in function");
                        let arr2 = a2
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if arr1.is_null(i) || arr2.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let v1 = arr1.value(i);
                            let v2 = arr2.value(i);
                            let res = match *name {
                                "BIT_AND" => v1 & v2,
                                "BIT_OR" => v1 | v2,
                                "BIT_XOR" => v1 ^ v2,
                                "BIT_SHIFT_LEFT" => v1 << (v2 as u32),
                                "BIT_SHIFT_RIGHT" => v1 >> (v2 as u32),
                                _ => unreachable!(),
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define BIT_NOT, BIT_COUNT
        for name in &["BIT_NOT", "BIT_COUNT"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let a = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Int64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let arr = a
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let v = arr.value(i);
                            let res = match *name {
                                "BIT_NOT" => !v,
                                "BIT_COUNT" => v.count_ones() as i64,
                                _ => unreachable!(),
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define CONCAT_WS, LPAD, RPAD, SPLIT_PART
        scalar_functions.insert(
            "CONCAT_WS".to_string(),
            ScalarFunction::new(
                "CONCAT_WS".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() < 2 {
                        return Err(crate::LightningError::Internal(
                            "CONCAT_WS requires at least 2 arguments".into(),
                        ));
                    }
                    let sep_arg = arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let sep_arr = sep_arg
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("type mismatch in function");
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for i in 0..num_rows {
                        if sep_arr.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        let sep = sep_arr.value(i);
                        let mut parts = Vec::new();
                        for arg in &args[1..] {
                            let s_arg =
                                arrow::compute::cast(arg, &arrow::datatypes::DataType::Utf8)
                                    .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                            let s_arr = s_arg
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .expect("type mismatch in function");
                            if !s_arr.is_null(i) {
                                parts.push(s_arr.value(i).to_string());
                            }
                        }
                        results.append_value(parts.join(sep));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        for name in &["LPAD", "RPAD"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() < 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 2 or 3 arguments"),
                            ));
                        }
                        let mut results =
                            arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 16);
                        for i in 0..num_rows {
                            let s_val = crate::processor::Value::from_arrow(&args[0], i);
                            let n_val = crate::processor::Value::from_arrow(&args[1], i);
                            let fill_val = if args.len() > 2 {
                                crate::processor::Value::from_arrow(&args[2], i)
                            } else {
                                crate::processor::Value::String(" ".into())
                            };
                            if let (
                                crate::processor::Value::String(s),
                                crate::processor::Value::Number(n),
                                crate::processor::Value::String(f),
                            ) = (&s_val, &n_val, &fill_val)
                            {
                                let n = *n as usize;
                                let res = if s.len() >= n {
                                    s[..n].to_string()
                                } else {
                                    let mut res = s.clone();
                                    while res.len() < n {
                                        if *name == "LPAD" {
                                            res = format!("{f}{res}");
                                        } else {
                                            res.push_str(f);
                                        }
                                    }
                                    if res.len() > n {
                                        res.truncate(n);
                                    }
                                    res
                                };
                                results.append_value(res);
                            } else {
                                results.append_null();
                            }
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        scalar_functions.insert(
            "SPLIT_PART".to_string(),
            ScalarFunction::new(
                "SPLIT_PART".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 3 {
                        return Err(crate::LightningError::Internal(
                            "SPLIT_PART requires 3 arguments".into(),
                        ));
                    }
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 8);
                    for i in 0..num_rows {
                        let s_val = crate::processor::Value::from_arrow(&args[0], i);
                        let d_val = crate::processor::Value::from_arrow(&args[1], i);
                        let p_val = crate::processor::Value::from_arrow(&args[2], i);
                        if let (
                            crate::processor::Value::String(s),
                            crate::processor::Value::String(d),
                            crate::processor::Value::Number(p),
                        ) = (&s_val, &d_val, &p_val)
                        {
                            let parts: Vec<&str> = s.split(d).collect();
                            let p = *p as usize;
                            if p > 0 && p <= parts.len() {
                                results.append_value(parts[p - 1]);
                            } else {
                                results.append_null();
                            }
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define DATE_TRUNC, AGE
        scalar_functions.insert(
            "DATE_TRUNC".to_string(),
            ScalarFunction::new(
                "DATE_TRUNC".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "DATE_TRUNC requires 2 arguments (unit, date)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let u_val = crate::processor::Value::from_arrow(&args[0], i);
                        let d_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::String(unit),
                            crate::processor::Value::Date(days),
                        ) = (&u_val, &d_val)
                        {
                            let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                + chrono::Duration::days(*days as i64);
                            use chrono::Datelike;
                            let res_dt = match unit.to_lowercase().as_str() {
                                "year" => chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1),
                                "month" => {
                                    chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                                }
                                "day" => Some(dt),
                                _ => None,
                            };
                            if let Some(rdt) = res_dt {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time");
                                results.push(crate::processor::Value::Date(
                                    rdt.signed_duration_since(epoch).num_days() as i32,
                                ));
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Date32,
                    ))
                }),
            ),
        );

        scalar_functions.insert(
            "AGE".to_string(),
            ScalarFunction::new(
                "AGE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "AGE requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    let today = chrono::Utc::now().date_naive();
                    for i in 0..num_rows {
                        let d_val = crate::processor::Value::from_arrow(&args[0], i);
                        if let crate::processor::Value::Date(days) = d_val {
                            let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                + chrono::Duration::days(days as i64);
                            let age = today.signed_duration_since(dt).num_days() / 365;
                            results.append_value(age);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define ATAN2, SINH, COSH, TANH
        scalar_functions.insert(
            "ATAN2".to_string(),
            ScalarFunction::new(
                "ATAN2".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "ATAN2 requires 2 arguments".into(),
                        ));
                    }
                    let y_arg =
                        arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let x_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let y_arr = y_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let x_arr = x_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if y_arr.is_null(i) || x_arr.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        results.append_value(y_arr.value(i).atan2(x_arr.value(i)));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        for name in &["SINH", "COSH", "TANH"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let n_arg =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let n_arr = n_arg
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if n_arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let n = n_arr.value(i);
                            let res = match *name {
                                "SINH" => n.sinh(),
                                "COSH" => n.cosh(),
                                "TANH" => n.tanh(),
                                _ => unreachable!(),
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define EPOCH, MONTHNAME, DAYNAME
        scalar_functions.insert(
            "EPOCH".to_string(),
            ScalarFunction::new(
                "EPOCH".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "EPOCH requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        if let crate::processor::Value::Timestamp(micros) = v {
                            results.append_value(micros / 1_000_000);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        for name in &["MONTHNAME", "DAYNAME"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let mut results =
                            arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 8);
                        for i in 0..num_rows {
                            let v = crate::processor::Value::from_arrow(&args[0], i);
                            match v {
                                crate::processor::Value::Date(days) => {
                                    let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                        + chrono::Duration::days(days as i64);
                                    let res = if *name == "MONTHNAME" {
                                        dt.format("%B").to_string()
                                    } else {
                                        dt.format("%A").to_string()
                                    };
                                    results.append_value(res);
                                }
                                _ => results.append_null(),
                            }
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define TO_BOOL, TO_FLOAT
        scalar_functions.insert(
            "TO_BOOL".to_string(),
            ScalarFunction::new(
                "TO_BOOL".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_BOOL requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Boolean)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        scalar_functions.insert(
            "TO_FLOAT".to_string(),
            ScalarFunction::new(
                "TO_FLOAT".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_FLOAT requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        // Define DATE_PART
        scalar_functions.insert(
            "DATE_PART".to_string(),
            ScalarFunction::new(
                "DATE_PART".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "DATE_PART requires 2 arguments (unit, date/timestamp)".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let unit_val = crate::processor::Value::from_arrow(&args[0], i);
                        let d_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::String(unit),
                            crate::processor::Value::Date(days),
                        ) = (&unit_val, &d_val)
                        {
                            let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                + chrono::Duration::days(*days as i64);
                            use chrono::Datelike;
                            let res = match unit.to_lowercase().as_str() {
                                "year" => dt.year() as i64,
                                "month" => dt.month() as i64,
                                "day" => dt.day() as i64,
                                _ => 0,
                            };
                            results.append_value(res);
                        } else if let (
                            crate::processor::Value::String(unit),
                            crate::processor::Value::Timestamp(micros),
                        ) = (&unit_val, &d_val)
                        {
                            let dt = chrono::DateTime::from_timestamp_micros(*micros)
                                .expect("type mismatch in function")
                                .naive_utc();
                            use chrono::{Datelike, Timelike};
                            let res = match unit.to_lowercase().as_str() {
                                "year" => dt.year() as i64,
                                "month" => dt.month() as i64,
                                "day" => dt.day() as i64,
                                "hour" => dt.hour() as i64,
                                "minute" => dt.minute() as i64,
                                "second" => dt.second() as i64,
                                _ => 0,
                            };
                            results.append_value(res);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define DATE_DIFF, TIMESTAMP_DIFF
        for name in &["DATE_DIFF", "TIMESTAMP_DIFF"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 3 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 3 arguments (unit, start, end)"),
                            ));
                        }
                        let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let u_val = crate::processor::Value::from_arrow(&args[0], i)
                                .to_string()
                                .to_uppercase();
                            let start = crate::processor::Value::from_arrow(&args[1], i);
                            let end = crate::processor::Value::from_arrow(&args[2], i);

                            let diff_seconds = match (start, end) {
                                (
                                    crate::processor::Value::Date(s),
                                    crate::processor::Value::Date(e),
                                ) => (e as i64 - s as i64) * 86400,
                                (
                                    crate::processor::Value::Timestamp(s),
                                    crate::processor::Value::Timestamp(e),
                                ) => (e - s) / 1_000_000,
                                _ => {
                                    results.append_null();
                                    continue;
                                }
                            };

                            let res = match u_val.as_str() {
                                "SECOND" | "SECONDS" => diff_seconds,
                                "MINUTE" | "MINUTES" => diff_seconds / 60,
                                "HOUR" | "HOURS" => diff_seconds / 3600,
                                "DAY" | "DAYS" => diff_seconds / 86400,
                                "WEEK" | "WEEKS" => diff_seconds / (86400 * 7),
                                "MONTH" | "MONTHS" => diff_seconds / (86400 * 30),
                                "YEAR" | "YEARS" => diff_seconds / (86400 * 365),
                                _ => 0,
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define VERSION, CURRENT_USER
        scalar_functions.insert(
            "VERSION".to_string(),
            ScalarFunction::new(
                "VERSION".to_string(),
                Arc::new(|_args, num_rows| {
                    let v = format!("Lightning Engine v0.1.0 ({})", std::env::consts::OS);
                    Ok(Arc::new(arrow::array::StringArray::from(vec![v; num_rows])))
                }),
            ),
        );

        scalar_functions.insert(
            "CURRENT_USER".to_string(),
            ScalarFunction::new(
                "CURRENT_USER".to_string(),
                Arc::new({
                    let u = std::env::var("USER")
                        .or_else(|_| std::env::var("LOGNAME"))
                        .unwrap_or_else(|_| "lightning_user".to_string());
                    move |_args, num_rows| {
                        Ok(Arc::new(arrow::array::StringArray::from(vec![u.clone(); num_rows])))
                    }
                }),
            ),
        );

        // Define OCTET_LENGTH
        scalar_functions.insert(
            "OCTET_LENGTH".to_string(),
            ScalarFunction::new(
                "OCTET_LENGTH".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "OCTET_LENGTH requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        match v {
                            crate::processor::Value::String(s) => {
                                results.append_value(s.len() as i64)
                            }
                            crate::processor::Value::Null => results.append_null(),
                            _ => results.append_value(8), // Fixed size for others? Stub
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define TO_DATE, TO_TIMESTAMP
        scalar_functions.insert(
            "TO_DATE".to_string(),
            ScalarFunction::new(
                "TO_DATE".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_DATE requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Date32)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        scalar_functions.insert(
            "TO_TIMESTAMP".to_string(),
            ScalarFunction::new(
                "TO_TIMESTAMP".to_string(),
                Arc::new(|args, _num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_TIMESTAMP requires 1 argument".into(),
                        ));
                    }
                    arrow::compute::cast(
                        &args[0],
                        &arrow::datatypes::DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                    )
                    .map_err(|e| crate::LightningError::Internal(e.to_string()))
                }),
            ),
        );

        // Define LEVENSHTEIN
        scalar_functions.insert(
            "LEVENSHTEIN".to_string(),
            ScalarFunction::new(
                "LEVENSHTEIN".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "LEVENSHTEIN requires 2 arguments".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let s1_val = crate::processor::Value::from_arrow(&args[0], i);
                        let s2_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::String(s1),
                            crate::processor::Value::String(s2),
                        ) = (s1_val, s2_val)
                        {
                            let len1 = s1.chars().count();
                            let len2 = s2.chars().count();
                            let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];
                            for i in 0..=len1 {
                                matrix[i][0] = i;
                            }
                            for j in 0..=len2 {
                                matrix[0][j] = j;
                            }
                            let c1: Vec<char> = s1.chars().collect();
                            let c2: Vec<char> = s2.chars().collect();
                            for i in 1..=len1 {
                                for j in 1..=len2 {
                                    let cost = if c1[i - 1] == c2[j - 1] { 0 } else { 1 };
                                    matrix[i][j] = (matrix[i - 1][j] + 1)
                                        .min(matrix[i][j - 1] + 1)
                                        .min(matrix[i - 1][j - 1] + cost);
                                }
                            }
                            results.append_value(matrix[len1][len2] as i64);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define JSON_PARSE, JSON_SERIALIZE
        scalar_functions.insert(
            "JSON_PARSE".to_string(),
            ScalarFunction::new(
                "JSON_PARSE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "JSON_PARSE requires 1 argument".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let s_val = crate::processor::Value::from_arrow(&args[0], i);
                        if let crate::processor::Value::String(s) = s_val {
                            if let Ok(j) = serde_json::from_str::<serde_json::Value>(&s) {
                                results.push(crate::processor::Value::from_json(&j));
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        scalar_functions.insert(
            "JSON_SERIALIZE".to_string(),
            ScalarFunction::new(
                "JSON_SERIALIZE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "JSON_SERIALIZE requires 1 argument".into(),
                        ));
                    }
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        if let Ok(s) = serde_json::to_string(&v.to_json()) {
                            results.append_value(s);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define LIST_POSITION
        scalar_functions.insert(
            "LIST_POSITION".to_string(),
            ScalarFunction::new(
                "LIST_POSITION".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "LIST_POSITION requires 2 arguments (list, element)".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let l_val = crate::processor::Value::from_arrow(&args[0], i);
                        let e_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let crate::processor::Value::List(l) = l_val {
                            if let Some(pos) = l.iter().position(|x| x == &e_val) {
                                results.append_value((pos + 1) as i64); // 1-based index usually in Cypher
                            } else {
                                results.append_null();
                            }
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define RAND
        scalar_functions.insert(
            "RAND".to_string(),
            ScalarFunction::new(
                "RAND".to_string(),
                Arc::new(|_args, num_rows| {
                    let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                    // Simple LCG as a stub for RAND()
                    let mut state = (chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64)
                        % 1_000_000_007;
                    for _ in 0..num_rows {
                        state = (state.wrapping_mul(1664525).wrapping_add(1013904223)) % 4294967296;
                        results.append_value((state as f64) / 4294967296.0);
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // ACOS, ASIN, ATAN, COT, SIGN are now vectorized above
        // using Arrow unary kernels instead of per-row loops.

        // Define TO_HEX, FROM_HEX
        scalar_functions.insert(
            "TO_HEX".to_string(),
            ScalarFunction::new(
                "TO_HEX".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "TO_HEX requires 1 argument".into(),
                        ));
                    }
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 8);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        match v {
                            crate::processor::Value::Number(n) => {
                                results.append_value(format!("{:x}", n as i64))
                            }
                            _ => results.append_null(),
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        scalar_functions.insert(
            "FROM_HEX".to_string(),
            ScalarFunction::new(
                "FROM_HEX".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "FROM_HEX requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        if let crate::processor::Value::String(s) = v {
                            if let Ok(n) = i64::from_str_radix(&s, 16) {
                                results.append_value(n);
                            } else {
                                results.append_null();
                            }
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define LIST_INSERT
        scalar_functions.insert(
            "LIST_INSERT".to_string(),
            ScalarFunction::new(
                "LIST_INSERT".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 3 {
                        return Err(crate::LightningError::Internal(
                            "LIST_INSERT requires 3 arguments (list, index, element)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let l_val = crate::processor::Value::from_arrow(&args[0], i);
                        let idx_val = crate::processor::Value::from_arrow(&args[1], i);
                        let e_val = crate::processor::Value::from_arrow(&args[2], i);
                        if let (
                            crate::processor::Value::List(mut l),
                            crate::processor::Value::Number(idx),
                        ) = (l_val, idx_val)
                        {
                            let idx = idx as usize;
                            if idx > 0 && idx <= l.len() + 1 {
                                l.insert(idx - 1, e_val);
                                results.push(crate::processor::Value::List(l));
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define LIST_REPLACE, LIST_REMOVE
        scalar_functions.insert(
            "LIST_REPLACE".to_string(),
            ScalarFunction::new(
                "LIST_REPLACE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 3 {
                        return Err(crate::LightningError::Internal(
                            "LIST_REPLACE requires 3 arguments (list, old, new)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let l_val = crate::processor::Value::from_arrow(&args[0], i);
                        let old_val = crate::processor::Value::from_arrow(&args[1], i);
                        let new_val = crate::processor::Value::from_arrow(&args[2], i);
                        if let crate::processor::Value::List(l) = l_val {
                            let new_l: Vec<crate::processor::Value> = l
                                .into_iter()
                                .map(|item| {
                                    if item == old_val {
                                        new_val.clone()
                                    } else {
                                        item
                                    }
                                })
                                .collect();
                            results.push(crate::processor::Value::List(new_l));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        scalar_functions.insert(
            "LIST_REMOVE".to_string(),
            ScalarFunction::new(
                "LIST_REMOVE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "LIST_REMOVE requires 2 arguments (list, element)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let l_val = crate::processor::Value::from_arrow(&args[0], i);
                        let e_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let crate::processor::Value::List(l) = l_val {
                            let new_l: Vec<crate::processor::Value> =
                                l.into_iter().filter(|item| item != &e_val).collect();
                            results.push(crate::processor::Value::List(new_l));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define MAP_REMOVE
        scalar_functions.insert(
            "MAP_REMOVE".to_string(),
            ScalarFunction::new(
                "MAP_REMOVE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MAP_REMOVE requires 2 arguments (map, key)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let m_val = crate::processor::Value::from_arrow(&args[0], i);
                        let k_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let crate::processor::Value::Map(mut m) = m_val {
                            m.remove(&k_val);
                            results.push(crate::processor::Value::Map(m));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define LEAP_YEAR
        scalar_functions.insert(
            "LEAP_YEAR".to_string(),
            ScalarFunction::new(
                "LEAP_YEAR".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "LEAP_YEAR requires 1 argument (date)".into(),
                        ));
                    }
                    let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let d_val = crate::processor::Value::from_arrow(&args[0], i);
                        if let crate::processor::Value::Date(days) = d_val {
                            let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("infallible: valid date/time")
                                + chrono::Duration::days(days as i64);
                            use chrono::Datelike;
                            let y = dt.year();
                            results.append_value((y % 4 == 0 && y % 100 != 0) || y % 400 == 0);
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define Aliases
        for (alias, target) in &[
            ("TO_LOWER", "LOWER"),
            ("TO_UPPER", "UPPER"),
            ("STR", "TO_STRING"),
            ("BOOL", "TO_BOOL"),
            ("FLOAT", "TO_FLOAT"),
            ("INTEGER", "TO_INT"),
        ] {
            if let Some(f) = scalar_functions.get(*target) {
                scalar_functions.insert(alias.to_string(), f.clone());
            }
        }

        // Define MAP_CONTAINS_KEY, MAP_CONTAINS_VALUE
        scalar_functions.insert(
            "MAP_CONTAINS_KEY".to_string(),
            ScalarFunction::new(
                "MAP_CONTAINS_KEY".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MAP_CONTAINS_KEY requires 2 arguments (map, key)".into(),
                        ));
                    }
                    let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let m_val = crate::processor::Value::from_arrow(&args[0], i);
                        let k_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let crate::processor::Value::Map(m) = m_val {
                            results.append_value(m.contains_key(&k_val));
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        scalar_functions.insert(
            "MAP_CONTAINS_VALUE".to_string(),
            ScalarFunction::new(
                "MAP_CONTAINS_VALUE".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MAP_CONTAINS_VALUE requires 2 arguments (map, value)".into(),
                        ));
                    }
                    let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let m_val = crate::processor::Value::from_arrow(&args[0], i);
                        let v_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let crate::processor::Value::Map(m) = m_val {
                            results.append_value(m.values().any(|x| x == &v_val));
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define JARO_WINKLER
        scalar_functions.insert(
            "JARO_WINKLER".to_string(),
            ScalarFunction::new(
                "JARO_WINKLER".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "JARO_WINKLER requires 2 arguments".into(),
                        ));
                    }
                    let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let s1_val = crate::processor::Value::from_arrow(&args[0], i);
                        let s2_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::String(s1),
                            crate::processor::Value::String(s2),
                        ) = (s1_val, s2_val)
                        {
                            if s1.is_empty() || s2.is_empty() {
                                results.append_value(0.0);
                                continue;
                            }
                            let c1: Vec<char> = s1.chars().collect();
                            let c2: Vec<char> = s2.chars().collect();
                            let l1 = c1.len();
                            let l2 = c2.len();
                            let match_distance = (l1.max(l2) / 2).saturating_sub(1);
                            let mut s1_matches = vec![false; l1];
                            let mut s2_matches = vec![false; l2];
                            let mut matches = 0;
                            for i in 0..l1 {
                                let start = i.saturating_sub(match_distance);
                                let end = (i + match_distance + 1).min(l2);
                                for j in start..end {
                                    if !s2_matches[j] && c1[i] == c2[j] {
                                        s1_matches[i] = true;
                                        s2_matches[j] = true;
                                        matches += 1;
                                        break;
                                    }
                                }
                            }
                            if matches == 0 {
                                results.append_value(0.0);
                                continue;
                            }
                            let mut t = 0.0;
                            let mut k = 0;
                            for i in 0..l1 {
                                if s1_matches[i] {
                                    while !s2_matches[k] {
                                        k += 1;
                                    }
                                    if c1[i] != c2[k] {
                                        t += 0.5;
                                    }
                                    k += 1;
                                }
                            }
                            let m = matches as f64;
                            let j = (m / l1 as f64 + m / l2 as f64 + (m - t) / m) / 3.0;
                            let mut p = 0;
                            for i in 0..l1.min(l2).min(4) {
                                if c1[i] == c2[i] {
                                    p += 1;
                                } else {
                                    break;
                                }
                            }
                            results.append_value(j + p as f64 * 0.1 * (1.0 - j));
                        } else {
                            results.append_null();
                        }
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define LOG2, LOG10
        for name in &["LOG2", "LOG10"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let n_arg =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let n_arr = n_arg
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if n_arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let n = n_arr.value(i);
                            let res = match *name {
                                "LOG2" => n.log2(),
                                "LOG10" => n.log10(),
                                _ => unreachable!(),
                            };
                            results.append_value(res);
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define POWER
        scalar_functions.insert(
            "POWER".to_string(),
            ScalarFunction::new(
                "POWER".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "POWER requires 2 arguments (base, exp)".into(),
                        ));
                    }
                    let b_arg =
                        arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let e_arg =
                        arrow::compute::cast(&args[1], &arrow::datatypes::DataType::Float64)
                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                    let b_arr = b_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let e_arr = e_arg
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .expect("type mismatch in function");
                    let mut results = arrow::array::Float64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        if b_arr.is_null(i) || e_arr.is_null(i) {
                            results.append_null();
                            continue;
                        }
                        results.append_value(b_arr.value(i).powf(e_arr.value(i)));
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define HASH
        scalar_functions.insert(
            "HASH".to_string(),
            ScalarFunction::new(
                "HASH".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "HASH requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let v = crate::processor::Value::from_arrow(&args[0], i);
                        use std::hash::{Hash, Hasher};
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        format!("{v:?}").hash(&mut hasher); // Quick & dirty stable? hash
                        results.append_value(hasher.finish() as i64);
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define IS_NAN, IS_INF
        for name in &["IS_NAN", "IS_INF"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let n_arg =
                            arrow::compute::cast(&args[0], &arrow::datatypes::DataType::Float64)
                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                        let n_arr = n_arg
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .expect("type mismatch in function");
                        let mut results = arrow::array::BooleanBuilder::with_capacity(num_rows);
                        for i in 0..num_rows {
                            if n_arr.is_null(i) {
                                results.append_null();
                                continue;
                            }
                            let n = n_arr.value(i);
                            results.append_value(if *name == "IS_NAN" {
                                n.is_nan()
                            } else {
                                n.is_infinite()
                            });
                        }
                        Ok(Arc::new(results.finish()))
                    }),
                ),
            );
        }

        // Define IS_NULL, IS_NOT_NULL
        for name in &["IS_NULL", "IS_NOT_NULL"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(format!(
                                "{name} requires 1 argument"
                            )));
                        }
                        if *name == "IS_NOT_NULL" {
                            let nulls = args[0].nulls();
                            let result = match nulls {
                                Some(n) => {
                                    let mut vec = Vec::with_capacity(num_rows);
                                    for i in 0..num_rows {
                                        vec.push(n.is_valid(i));
                                    }
                                    arrow::array::BooleanArray::from(vec)
                                }
                                None => arrow::array::BooleanArray::from(vec![true; num_rows]),
                            };
                            Ok(Arc::new(result))
                        } else {
                            let nulls = args[0].nulls();
                            let result = match nulls {
                                Some(n) => {
                                    let mut vec = Vec::with_capacity(num_rows);
                                    for i in 0..num_rows {
                                        vec.push(n.is_null(i));
                                    }
                                    arrow::array::BooleanArray::from(vec)
                                }
                                None => arrow::array::BooleanArray::from(vec![false; num_rows]),
                            };
                            Ok(Arc::new(result))
                        }
                    }),
                ),
            );
        }

        // Define PI, E, PHI, INFINITY
        for name in &["PI", "E", "PHI", "INFINITY"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |_args, num_rows| {
                        let val = match *name {
                            "PI" => std::f64::consts::PI,
                            "E" => std::f64::consts::E,
                            "PHI" => 1.618033988749895,
                            "INFINITY" => std::f64::INFINITY,
                            _ => 0.0,
                        };
                        Ok(Arc::new(arrow::array::Float64Array::from(vec![
                            val;
                            num_rows
                        ])))
                    }),
                ),
            );
        }

        // Define Aliases
        for (alias, target) in &[
            ("TO_LOWER", "LOWER"),
            ("TO_UPPER", "UPPER"),
            ("STR", "TO_STRING"),
            ("BOOL", "TO_BOOL"),
            ("FLOAT", "TO_FLOAT"),
            ("INTEGER", "TO_INT"),
            ("STR_LEN", "LENGTH"),
            ("ARRAY_SIZE", "SIZE"),
            ("LIST_SIZE", "SIZE"),
            ("DATETIME", "TIMESTAMP"),
            ("TO_DATE", "DATE"),
            ("TO_TIMESTAMP", "TIMESTAMP"),
        ] {
            if let Some(f) = scalar_functions.get(*target) {
                scalar_functions.insert(alias.to_string(), f.clone());
            }
        }

        // Define MAP_KV_LIST
        scalar_functions.insert(
            "MAP_KV_LIST".to_string(),
            ScalarFunction::new(
                "MAP_KV_LIST".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "MAP_KV_LIST requires 2 arguments (keys, values)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let ks = crate::processor::Value::from_arrow(&args[0], i);
                        let vs = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::List(k_list),
                            crate::processor::Value::List(v_list),
                        ) = (ks, vs)
                        {
                            let mut m = std::collections::HashMap::new();
                            for (k, v) in k_list.into_iter().zip(v_list.into_iter()) {
                                m.insert(k, v);
                            }
                            results.push(crate::processor::Value::Map(m));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define OFFSET
        scalar_functions.insert(
            "OFFSET".to_string(),
            ScalarFunction::new(
                "OFFSET".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 1 {
                        return Err(crate::LightningError::Internal(
                            "OFFSET requires 1 argument".into(),
                        ));
                    }
                    let mut results = arrow::array::Int64Builder::with_capacity(num_rows);
                    for i in 0..num_rows {
                        results.append_value(i as i64); // Row offset in batch
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define GEN_RANDOM_UUID
        scalar_functions.insert(
            "GEN_RANDOM_UUID".to_string(),
            ScalarFunction::new(
                "GEN_RANDOM_UUID".to_string(),
                Arc::new(|_args, num_rows| {
                    let mut results =
                        arrow::array::StringBuilder::with_capacity(num_rows, num_rows * 36);
                    let mut state = (chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64)
                        ^ 0x9e3779b97f4a7c15;
                    for _ in 0..num_rows {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let a = (state >> 32) as u32;
                        let b = state as u32;
                        let c = (a.wrapping_mul(1103515245).wrapping_add(12345) >> 16) as u16;
                        let d = (b.wrapping_mul(1103515245).wrapping_add(12345) >> 16) as u16;
                        let uuid = format!(
                            "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                            a,
                            (c >> 4) as u16,
                            (c & 0x0fff) as u16,
                            0x8000u16 | ((d >> 2) as u16 & 0x3fff),
                            ((b as u64) << 32) | (a as u64)
                        );
                        results.append_value(uuid);
                    }
                    Ok(Arc::new(results.finish()))
                }),
            ),
        );

        // Define REGEXP_MATCH, REGEXP_REPLACE, REGEXP_COUNT, REGEXP_EXTRACT
        for name in &[
            "REGEXP_MATCH",
            "REGEXP_REPLACE",
            "REGEXP_COUNT",
            "REGEXP_EXTRACT",
        ] {
            let func_name = name.to_string();
            let re_cache: std::sync::Mutex<Option<(String, regex::Regex)>> =
                std::sync::Mutex::new(None);
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() < 2 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires at least 2 arguments"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let s_val = crate::processor::Value::from_arrow(&args[0], i);
                            let p_val = crate::processor::Value::from_arrow(&args[1], i);
                            if let (
                                crate::processor::Value::String(s),
                                crate::processor::Value::String(p),
                            ) = (s_val, p_val)
                            {
                                let re = {
                                    let mut cache = re_cache.lock().unwrap();
                                    if let Some((ref cached_pat, ref cached_re)) = *cache {
                                        if *cached_pat == p {
                                            (*cached_re).clone()
                                        } else {
                                            let compiled = regex::Regex::new(&p)
                                                .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                                            *cache = Some((p.clone(), compiled.clone()));
                                            compiled
                                        }
                                    } else {
                                        let compiled = regex::Regex::new(&p)
                                            .map_err(|e| crate::LightningError::Internal(e.to_string()))?;
                                        *cache = Some((p.clone(), compiled.clone()));
                                        compiled
                                    }
                                };
                                match *name {
                                    "REGEXP_MATCH" => results
                                        .push(crate::processor::Value::Boolean(re.is_match(&s))),
                                    "REGEXP_COUNT" => {
                                        results.push(crate::processor::Value::Number(
                                            re.find_iter(&s).count() as f64,
                                        ))
                                    }
                                    "REGEXP_EXTRACT" => {
                                        let cap = re
                                            .captures(&s)
                                            .map(|c| {
                                                c.get(0)
                                                    .map(|m| m.as_str().to_string())
                                                    .unwrap_or_default()
                                            })
                                            .unwrap_or_default();
                                        results.push(crate::processor::Value::String(cap));
                                    }
                                    "REGEXP_REPLACE" => {
                                        if args.len() != 3 {
                                            return Err(crate::LightningError::Internal(
                                                "REGEXP_REPLACE requires 3 arguments".into(),
                                            ));
                                        }
                                        let r_val =
                                            crate::processor::Value::from_arrow(&args[2], i);
                                        if let crate::processor::Value::String(r) = r_val {
                                            results.push(crate::processor::Value::String(
                                                re.replace_all(&s, r).to_string(),
                                            ));
                                        } else {
                                            results.push(crate::processor::Value::Null);
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::Null,
                        ))
                    }),
                ),
            );
        }

        // Define LIST_SUM, LIST_AVG, LIST_MIN, LIST_MAX
        for name in &["LIST_SUM", "LIST_AVG", "LIST_MIN", "LIST_MAX"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let l_val = crate::processor::Value::from_arrow(&args[0], i);
                            if let crate::processor::Value::List(l) = l_val {
                                let mut sum = 0.0;
                                let mut min = f64::INFINITY;
                                let mut max = f64::NEG_INFINITY;
                                let mut count = 0;
                                for v in l {
                                    if let crate::processor::Value::Number(n) = v {
                                        sum += n;
                                        if n < min {
                                            min = n;
                                        }
                                        if n > max {
                                            max = n;
                                        }
                                        count += 1;
                                    }
                                }
                                if count == 0 {
                                    results.push(crate::processor::Value::Null);
                                    continue;
                                }
                                match *name {
                                    "LIST_SUM" => {
                                        results.push(crate::processor::Value::Number(sum))
                                    }
                                    "LIST_AVG" => results
                                        .push(crate::processor::Value::Number(sum / count as f64)),
                                    "LIST_MIN" => {
                                        results.push(crate::processor::Value::Number(min))
                                    }
                                    "LIST_MAX" => {
                                        results.push(crate::processor::Value::Number(max))
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::Null,
                        ))
                    }),
                ),
            );
        }

        // Define STRUCT_PACK
        scalar_functions.insert(
            "STRUCT_PACK".to_string(),
            ScalarFunction::new(
                "STRUCT_PACK".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "STRUCT_PACK requires 2 arguments (keys, values)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let ks = crate::processor::Value::from_arrow(&args[0], i);
                        let vs = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::List(k_list),
                            crate::processor::Value::List(v_list),
                        ) = (ks, vs)
                        {
                            let mut s = Vec::new();
                            for (k, v) in k_list.into_iter().zip(v_list.into_iter()) {
                                s.push((k.to_string(), v));
                            }
                            results.push(crate::processor::Value::Struct(s));
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define STRUCT_EXTRACT
        scalar_functions.insert(
            "STRUCT_EXTRACT".to_string(),
            ScalarFunction::new(
                "STRUCT_EXTRACT".to_string(),
                Arc::new(|args, num_rows| {
                    if args.len() != 2 {
                        return Err(crate::LightningError::Internal(
                            "STRUCT_EXTRACT requires 2 arguments (struct, key)".into(),
                        ));
                    }
                    let mut results = Vec::with_capacity(num_rows);
                    for i in 0..num_rows {
                        let s_val = crate::processor::Value::from_arrow(&args[0], i);
                        let k_val = crate::processor::Value::from_arrow(&args[1], i);
                        if let (
                            crate::processor::Value::Struct(s),
                            crate::processor::Value::String(k),
                        ) = (s_val, k_val)
                        {
                            let val = s
                                .into_iter()
                                .find(|(name, _)| name == &k)
                                .map(|(_, v)| v)
                                .unwrap_or(crate::processor::Value::Null);
                            results.push(val);
                        } else {
                            results.push(crate::processor::Value::Null);
                        }
                    }
                    Ok(crate::processor::arrow_utils::values_to_array(
                        &results,
                        &arrow::datatypes::DataType::Null,
                    ))
                }),
            ),
        );

        // Define NODES, RELATIONSHIPS
        for name in &["NODES", "RELATIONSHIPS"] {
            let func_name = name.to_string();
            scalar_functions.insert(
                func_name.clone(),
                ScalarFunction::new(
                    func_name,
                    Arc::new(move |args, num_rows| {
                        if args.len() != 1 {
                            return Err(crate::LightningError::Internal(
                                format!("{name} requires 1 argument (path)"),
                            ));
                        }
                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let p_val = crate::processor::Value::from_arrow(&args[0], i);
                            if let crate::processor::Value::Path(p) = p_val {
                                let mut components = Vec::new();
                                let is_node_extraction = *name == "NODES";
                                for (idx, v) in p.into_iter().enumerate() {
                                    if is_node_extraction && idx % 2 == 0 {
                                        components.push(v);
                                    } else if !is_node_extraction && idx % 2 != 0 {
                                        components.push(v);
                                    }
                                }
                                results.push(crate::processor::Value::List(components));
                            } else {
                                results.push(crate::processor::Value::Null);
                            }
                        }
                        Ok(crate::processor::arrow_utils::values_to_array(
                            &results,
                            &arrow::datatypes::DataType::Null,
                        ))
                    }),
                ),
            );
        }

        Self {
            scalar_functions: RwLock::new(scalar_functions),
            aggregate_functions: RwLock::new(aggregate_functions),
    }
}

pub fn register_scalar(&self, func: ScalarFunction) {
        let name = func.name.to_uppercase();
        let mut map = self.scalar_functions.write();
        if map.contains_key(&name) {
            tracing::warn!("Duplicate scalar function registration: '{}' — overwriting previous", name);
        }
        map.insert(name, func);
    }

    pub fn has_scalar(&self, name: &str) -> bool {
        self.scalar_functions.read().contains_key(&name.to_uppercase())
    }

    pub fn get_scalar_function(&self, name: &str) -> Option<ScalarFunction> {
        self.scalar_functions.read().get(&name.to_uppercase()).cloned()
    }

    pub fn get_aggregate_function(
        &self,
        name: &str,
    ) -> Option<Box<dyn crate::processor::functions::AggregateFunction>> {
        self.aggregate_functions
            .read()
            .get(&name.to_uppercase())
            .map(|f| f())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use std::sync::Arc;

    #[test]
    fn test_register_scalar_duplicate_warning() {
        let mut registry = FunctionRegistry::new();
        let exec: crate::processor::functions::ScalarFunctionExec = Arc::new(
            |_args: &[ArrayRef], _num_rows: usize| {
                Ok(arrow::array::new_null_array(
                    &arrow::datatypes::DataType::Int64,
                    1,
                ))
            },
        );

        let first = ScalarFunction::new("MY_FUNC".to_string(), Arc::clone(&exec));
        let second = ScalarFunction::new("my_func".to_string(), Arc::clone(&exec));

        registry.register_scalar(first);
        registry.register_scalar(second);

        assert!(registry.has_scalar("MY_FUNC"));
    }
}

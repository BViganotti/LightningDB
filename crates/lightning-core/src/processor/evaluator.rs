use crate::parser::ast::Literal;
use crate::planner::binder::BoundExpression;
use crate::processor::Value;
use crate::{LightningError, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use arrow::array::MutableArrayData;
use arrow::compute::cast;
use arrow::compute::kernels::boolean::{and, not, or};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::{DataType, Field, Schema};
use lightning_types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn evaluate(
        expr: &BoundExpression,
        batch: Option<&RecordBatch>,
        params: Option<&HashMap<String, Value>>,
        num_rows: usize,
        registry: &crate::processor::functions::FunctionRegistry,
        database: &crate::Database,
    ) -> Result<ArrayRef> {
        match expr {
            BoundExpression::Literal(lit) => match lit {
                Literal::Number(n) => Ok(Arc::new(Float64Array::from_value(*n, num_rows))),
                Literal::String(s) => Ok(Arc::new(StringArray::from_iter_values(
                    std::iter::repeat_n(s.as_str(), num_rows),
                ))),
                Literal::Boolean(b) => {
                    let fill = if *b { 0xFFu8 } else { 0x00 };
                    let byte_count = num_rows.div_ceil(8);
                    let mut buf = arrow::buffer::MutableBuffer::from_len_zeroed(byte_count);
                    buf.as_mut().fill(fill);
                    let values = arrow::buffer::BooleanBuffer::new(buf.into(), 0, num_rows);
                    Ok(Arc::new(BooleanArray::new(values, None)))
                }
                Literal::Null => Ok(arrow::array::new_null_array(&DataType::Float64, num_rows)),
            },
            BoundExpression::PropertyLookup(_, idx, _) => {
                if let Some(b) = batch {
                    if *idx >= b.num_columns() {
                        return Err(LightningError::Internal(format!(
                            "PropertyLookup index {} out of bounds for batch with {} columns",
                            idx,
                            b.num_columns()
                        )));
                    }
                    Ok(b.column(*idx).clone())
                } else {
                    Err(LightningError::Internal(
                        "PropertyLookup requires a RecordBatch".to_string(),
                    ))
                }
            }
            BoundExpression::Variable(name, _) => {
                if let Some(b) = batch {
                    let schema = b.schema();
                    if let Ok(idx) = schema.index_of(name) {
                        return Ok(b.column(idx).clone());
                    }
                }
                Err(LightningError::Internal(format!(
                    "Variable {name} not found in batch"
                )))
            }
            BoundExpression::Comparison(left, op, right) => {
                // Fast paths for Column op Literal / Literal op Column (avoids sub-expression eval)
                if let Some(b) = batch {
                    // Column op Literal
                    if let (
                        BoundExpression::PropertyLookup(_, col_idx, _),
                        BoundExpression::Literal(lit),
                    ) = (&**left, &**right)
                    {
                        if *col_idx < b.num_columns() {
                            if let Some(result) = Self::compare_column_literal(
                                b.column(*col_idx), lit, op, num_rows,
                            ) {
                                return result;
                            }
                        }
                    }
                    // Literal op Column
                    if let (
                        BoundExpression::Literal(lit),
                        BoundExpression::PropertyLookup(_, col_idx, _),
                    ) = (&**left, &**right)
                    {
                        if *col_idx < b.num_columns() {
                            // For symmetric comparisons (eq/neq), just swap
                            let swapped_op = match op {
                                crate::parser::ast::ComparisonOperator::Equal => Some(*op),
                                crate::parser::ast::ComparisonOperator::NotEqual => Some(*op),
                                crate::parser::ast::ComparisonOperator::LessThan =>
                                    Some(crate::parser::ast::ComparisonOperator::GreaterThan),
                                crate::parser::ast::ComparisonOperator::LessThanOrEqual =>
                                    Some(crate::parser::ast::ComparisonOperator::GreaterThanOrEqual),
                                crate::parser::ast::ComparisonOperator::GreaterThan =>
                                    Some(crate::parser::ast::ComparisonOperator::LessThan),
                                crate::parser::ast::ComparisonOperator::GreaterThanOrEqual =>
                                    Some(crate::parser::ast::ComparisonOperator::LessThanOrEqual),
                            };
                            if let Some(swapped) = swapped_op {
                                if let Some(result) = Self::compare_column_literal(
                                    b.column(*col_idx), lit, &swapped, num_rows,
                                ) {
                                    return result;
                                }
                            }
                        }
                    }
                }

                let left_arr = Self::evaluate(left, batch, params, num_rows, registry, database)?;
                let right_arr = Self::evaluate(right, batch, params, num_rows, registry, database)?;

                // Optimization: Use the more specific type to avoid unnecessary casts
                // For numeric comparisons, prefer Int64 over Float64 when possible
                let common_type = if left_arr.data_type() == right_arr.data_type() {
                    left_arr.data_type().clone()
                } else {
                    // Check if both are integer types - use Int64 to avoid float precision loss
                    let left_is_int = matches!(
                        left_arr.data_type(),
                        DataType::Int64 | DataType::Int32 | DataType::UInt64
                    );
                    let right_is_int = matches!(
                        right_arr.data_type(),
                        DataType::Int64 | DataType::Int32 | DataType::UInt64
                    );
                    if left_is_int && right_is_int {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                };

                // Optimization: Skip cast if types already match
                let l = if left_arr.data_type() == &common_type {
                    left_arr
                } else {
                    cast(&left_arr, &common_type)
                        .map_err(|e| LightningError::Internal(e.to_string()))?
                };
                let r = if right_arr.data_type() == &common_type {
                    right_arr
                } else {
                    cast(&right_arr, &common_type)
                        .map_err(|e| LightningError::Internal(e.to_string()))?
                };

                let res: BooleanArray = match op {
                    crate::parser::ast::ComparisonOperator::Equal => {
                        eq(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ComparisonOperator::NotEqual => {
                        neq(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ComparisonOperator::LessThan => {
                        lt(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ComparisonOperator::LessThanOrEqual => {
                        lt_eq(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ComparisonOperator::GreaterThan => {
                        gt(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ComparisonOperator::GreaterThanOrEqual => {
                        gt_eq(&l, &r).map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                };
                Ok(Arc::new(res))
            }
            BoundExpression::Arithmetic(left, op, right) => {
                let left_arr =
                    Self::evaluate(left, batch, params, num_rows, registry, database)?;
                let right_arr =
                    Self::evaluate(right, batch, params, num_rows, registry, database)?;

                if left_arr.data_type() == &DataType::Int64
                    && right_arr.data_type() == &DataType::Int64
                {
                    return Self::evaluate_arith_int64(&left_arr, &right_arr, op);
                }

                if left_arr.data_type() == &DataType::UInt64
                    && right_arr.data_type() == &DataType::UInt64
                {
                    return Self::evaluate_arith_uint64(&left_arr, &right_arr, op);
                }

                let l = cast(&left_arr, &DataType::Float64)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let r = cast(&right_arr, &DataType::Float64)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;

                let l_f64 = l
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| LightningError::Internal("Expected Float64Array".into()))?;
                let r_f64 = r
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| LightningError::Internal("Expected Float64Array".into()))?;

                let res = match op {
                    crate::parser::ast::ArithmeticOperator::Add => {
                        arrow::compute::kernels::numeric::add(l_f64, r_f64)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ArithmeticOperator::Subtract => {
                        arrow::compute::kernels::numeric::sub(l_f64, r_f64)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ArithmeticOperator::Multiply => {
                        arrow::compute::kernels::numeric::mul(l_f64, r_f64)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ArithmeticOperator::Divide => {
                        arrow::compute::kernels::numeric::div(l_f64, r_f64)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                    crate::parser::ast::ArithmeticOperator::Modulo => {
                        arrow::compute::kernels::numeric::rem(l_f64, r_f64)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    }
                };
                Ok(Arc::new(res))
            }
            BoundExpression::Logical(left, op, right) => {
                let l = cast(
                    &Self::evaluate(left, batch, params, num_rows, registry, database)?,
                    &DataType::Boolean,
                )
                .map_err(|e| LightningError::Internal(e.to_string()))?;
                let l_bool = l
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;

                match op {
                    crate::parser::ast::LogicalOperator::And => {
                        let true_count = l_bool.values().count_set_bits();
                        if true_count == 0 {
                            return Ok(Arc::new(l_bool.clone()));
                        }
                        let r = cast(
                            &Self::evaluate(right, batch, params, num_rows, registry, database)?,
                            &DataType::Boolean,
                        )
                        .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let r_bool = r
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;
                        let res = and(l_bool, r_bool)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        Ok(Arc::new(res))
                    }
                    crate::parser::ast::LogicalOperator::Xor => {
                        let r = cast(
                            &Self::evaluate(right, batch, params, num_rows, registry, database)?,
                            &DataType::Boolean,
                        )
                        .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let r_bool = r
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;
                        // XOR as (l AND NOT r) OR (NOT l AND r)
                        let not_l = not(l_bool)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let not_r = not(r_bool)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let l_and_not_r = and(l_bool, &not_r)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let not_l_and_r = and(&not_l, r_bool)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let res = or(&l_and_not_r, &not_l_and_r)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        Ok(Arc::new(res))
                    }
                    _ => {
                        let r = cast(
                            &Self::evaluate(right, batch, params, num_rows, registry, database)?,
                            &DataType::Boolean,
                        )
                        .map_err(|e| LightningError::Internal(e.to_string()))?;
                        let r_bool = r
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;
                        let res = or(l_bool, r_bool)
                            .map_err(|e| LightningError::Internal(e.to_string()))?;
                        Ok(Arc::new(res))
                    }
                }
            }
            BoundExpression::Not(expr) => {
                let arr = Self::evaluate(expr, batch, params, num_rows, registry, database)?;
                let arr = cast(&arr, &DataType::Boolean)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let arr = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;

                let res = not(arr)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                Ok(Arc::new(res))
            }
            BoundExpression::Function(name, args, _) => {
                // Handle list functions with lambdas BEFORE generic arg evaluation
                match name.as_str() {
                    "LIST_FILTER" => {
                        let list_array = Self::evaluate(
                            &args[0], batch, params, num_rows, registry, database,
                        )?;
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            return Self::evaluate_list_filter(
                                &list_array, var, body, params, registry, database,
                            );
                        }
                    }
                    "LIST_TRANSFORM" => {
                        let list_array = Self::evaluate(
                            &args[0], batch, params, num_rows, registry, database,
                        )?;
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            return Self::evaluate_list_transform(
                                &list_array, var, body, params, registry, database,
                            );
                        }
                    }
                    "LIST_ANY" | "LIST_ALL" | "LIST_SINGLE" | "LIST_NONE" => {
                        let list_array = Self::evaluate(
                            &args[0], batch, params, num_rows, registry, database,
                        )?;
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            return Self::evaluate_list_predicate(
                                &list_array, var, body, name.as_str(),
                                params, registry, database,
                            );
                        }
                    }
                    _ => {}
                }

                let mut arg_arrays = Vec::new();
                for arg in args {
                    arg_arrays.push(Self::evaluate(
                        arg, batch, params, num_rows, registry, database,
                    )?);
                }

                if let Some(func) = registry.get_scalar_function(name) {
                    return func.execute(&arg_arrays, num_rows);
                }

                Err(LightningError::Internal(format!(
                    "Function {name} not implemented"
                )))
            }
            BoundExpression::List(exprs, list_type) => {
                let mut arrays = Vec::new();
                for e in exprs {
                    arrays.push(Self::evaluate(
                        e, batch, params, num_rows, registry, database,
                    )?);
                }

                if arrays.is_empty() {
                    let field = Arc::new(Field::new("item", DataType::Null, true));
                    let offsets = arrow::buffer::OffsetBuffer::from_lengths(vec![0; num_rows]);
                    let values = arrow::array::new_empty_array(&DataType::Null);
                    let list_array = ListArray::try_new(field, offsets, values, None)
                        .map_err(|e| LightningError::Internal(e.to_string()))?;
                    return Ok(Arc::new(list_array));
                }

                let values_arr =
                    arrow::compute::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                        .map_err(|e| LightningError::Internal(e.to_string()))?;

                let field = if let LogicalType::List(inner) = list_type {
                    Arc::new(Field::new(
                        "item",
                        crate::processor::arrow_utils::logical_type_to_arrow_type(inner),
                        true,
                    ))
                } else {
                    Arc::new(Field::new(
                        "item",
                        crate::processor::arrow_utils::logical_type_to_arrow_type(list_type),
                        true,
                    ))
                };
                let offsets =
                    arrow::buffer::OffsetBuffer::from_lengths(vec![exprs.len(); num_rows]);
                let list_array = ListArray::try_new(field, offsets, values_arr, None)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                Ok(Arc::new(list_array))
            }
            BoundExpression::Map(entries, struct_type) => {
                let field_defs = if let LogicalType::Struct(fds) = struct_type {
                    fds
                } else {
                    return Err(LightningError::Internal("Map must have Struct type".into()));
                };
                if field_defs.is_empty() {
                    return Ok(arrow::array::new_null_array(
                        &arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::default()),
                        num_rows,
                    ));
                }
                let mut fields = Vec::new();
                let mut arrays = Vec::new();
                for ((_key, expr), field_def) in entries.iter().zip(field_defs.iter()) {
                    let arr = Self::evaluate(
                        expr, batch, params, num_rows, registry, database,
                    )?;
                    let arrow_type = crate::processor::arrow_utils::logical_type_to_arrow_type(
                        &field_def.type_,
                    );
                    // Cast if needed
                    let cast_arr = if arr.data_type() != &arrow_type {
                        arrow::compute::kernels::cast::cast(&arr, &arrow_type)
                            .map_err(|e| LightningError::Internal(e.to_string()))?
                    } else {
                        arr
                    };
                    fields.push(Arc::new(arrow::datatypes::Field::new(
                        &field_def.name,
                        arrow_type,
                        true,
                    )));
                    arrays.push(cast_arr);
                }
                let struct_array = arrow::array::StructArray::try_new(fields.into(), arrays, None)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                Ok(Arc::new(struct_array))
            }
            BoundExpression::Case {
                expression,
                when_then,
                else_expression,
                ..
            } => {
                let num_rows = batch.map(|b| b.num_rows()).unwrap_or(1);

                if when_then.is_empty() {
                    return if let Some(ref expr) = else_expression {
                        Self::evaluate(expr, batch, params, num_rows, registry, database)
                    } else {
                        Ok(arrow::array::new_null_array(&arrow::datatypes::DataType::Null, num_rows))
                    };
                }

                let case_val = if let Some(ref expr) = expression {
                    Some(Self::evaluate(expr, batch, params, num_rows, registry, database)?)
                } else {
                    None
                };

                let evaluated: Vec<(ArrayRef, ArrayRef)> = when_then
                    .iter()
                    .map(|(when, then)| {
                        let when_arr = Self::evaluate(when, batch, params, num_rows, registry, database)?;
                        let then_arr = Self::evaluate(then, batch, params, num_rows, registry, database)?;
                        Ok((when_arr, then_arr))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_arr = if let Some(ref expr) = else_expression {
                    Some(Self::evaluate(expr, batch, params, num_rows, registry, database)?)
                } else {
                    None
                };

                let match_masks: Vec<ArrayRef> = if let Some(ref cv) = case_val {
                    evaluated
                        .iter()
                        .map(|(when_arr, _)| {
                            let eq_arr = arrow::compute::kernels::cmp::eq(cv, when_arr)
                                .map_err(|e| LightningError::Internal(e.to_string()))?;
                            Ok(Arc::new(eq_arr) as ArrayRef)
                        })
                        .collect::<Result<Vec<_>>>()?
                } else {
                    evaluated.iter().map(|(when_arr, _)| when_arr.clone()).collect()
                };

                let mut sources: Vec<ArrayRef> = Vec::new();
                for (_, then_arr) in &evaluated {
                    sources.push(then_arr.clone());
                }
                let else_idx = sources.len();
                if let Some(ref arr) = else_arr {
                    sources.push(arr.clone());
                }

                let source_datas: Vec<arrow::data::ArrayData> =
                    sources.iter().map(|a| a.to_data()).collect();
                let source_refs: Vec<&arrow::data::ArrayData> = source_datas.iter().collect();
                let mut mutable = MutableArrayData::new(source_refs, false, num_rows);

                for i in 0..num_rows {
                    let mut matched = false;
                    for (j, mask) in match_masks.iter().enumerate() {
                        let bool_mask = mask
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| LightningError::Internal("CASE WHEN must be boolean".into()))?;
                        if !bool_mask.is_null(i) && bool_mask.value(i) {
                            mutable.extend(j, i, i + 1);
                            matched = true;
                            break;
                        }
                    }
                    if !matched {
                        if else_arr.is_some() {
                            mutable.extend(else_idx, i, i + 1);
                        } else {
                            mutable.extend_nulls(1);
                        }
                    }
                }

                let result_data = mutable.finish();
                let result_arr = arrow::array::make_array(result_data);
                Ok(result_arr)
            }
            BoundExpression::Parameter(name) => {
                let val = params.and_then(|p| p.get(name)).ok_or_else(|| {
                    LightningError::Query(format!("Parameter {name} not found"))
                })?;
                match val {
                    Value::Number(n) => Ok(Arc::new(Float64Array::from_value(*n, num_rows))),
                    Value::String(s) => Ok(Arc::new(StringArray::from_iter_values(
                        std::iter::repeat_n(s.as_str(), num_rows),
                    ))),
                    Value::Boolean(b) => {
                        let fill = if *b { 0xFFu8 } else { 0x00 };
                        let byte_count = num_rows.div_ceil(8);
                        let mut buf = arrow::buffer::MutableBuffer::from_len_zeroed(byte_count);
                        buf.as_mut().fill(fill);
                        let values = arrow::buffer::BooleanBuffer::new(buf.into(), 0, num_rows);
                        Ok(Arc::new(BooleanArray::new(values, None)))
                    }
                    _ => Err(LightningError::Internal(format!(
                        "Parameter type not implemented for evaluation: {val:?}"
                    ))),
                }
            }
            BoundExpression::CountSubquery(steps) => {
                let count = if let Some((sub_match, _sub_where)) = steps.first() {
                    // Count by scanning the first node table in the subquery
                    let mut total: i64 = 0;
                    for element in &sub_match.elements {
                        if let crate::planner::binder::BoundMatchElement::Node(table_name, _, _) = element {
                            let storage = database.storage_manager.read();
                            if let Some(table) = storage.get_table(table_name) {
                                let num_rows = table.next_row_id.load(std::sync::atomic::Ordering::Relaxed);
                                total += num_rows as i64;
                            }
                        }
                    }
                    total
                } else {
                    0
                };
                Ok(Arc::new(arrow::array::Int64Array::from_value(count, num_rows)))
            }
            _ => Err(LightningError::Internal(format!(
                "Expression evaluation not implemented: {expr:?}"
            ))),
        }
    }

    fn evaluate_arith_int64(
        left: &ArrayRef,
        right: &ArrayRef,
        op: &crate::parser::ast::ArithmeticOperator,
    ) -> Result<ArrayRef> {
        let l = left
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LightningError::Internal("Expected Int64Array for left operand".into()))?;
        let r = right
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| LightningError::Internal("Expected Int64Array for right operand".into()))?;

        use crate::parser::ast::ArithmeticOperator::*;
        let res: ArrayRef = match op {
            Add => {
                let raw = arrow::compute::kernels::numeric::add(l, r)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                // Check for overflow: if any value wrapped, promote to Float64
                let has_overflow = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => a.overflowing_add(b).1,
                        _ => false,
                    }
                });
                if has_overflow {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 + b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    Arc::new(raw)
                }
            }
            Subtract => {
                let raw = arrow::compute::kernels::numeric::sub(l, r)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let has_overflow = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => a.overflowing_sub(b).1,
                        _ => false,
                    }
                });
                if has_overflow {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 - b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    Arc::new(raw)
                }
            }
            Multiply => {
                let raw = arrow::compute::kernels::numeric::mul(l, r)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let has_overflow = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => a.overflowing_mul(b).1,
                        _ => false,
                    }
                });
                if has_overflow {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 * b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    Arc::new(raw)
                }
            }
            Divide => {
                let result: arrow::array::Int64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                    match (a, b) {
                        (Some(_), Some(0)) => None,
                        (Some(a), Some(b)) => match a.checked_div(b) {
                            Some(val) => Some(val),
                            None => None,
                        }
                        _ => None,
                    }
                }).collect();
                Arc::new(result)
            }
            Modulo => {
                let result: arrow::array::Int64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                    match (a, b) {
                        (Some(_), Some(0)) => None,
                        (Some(a), Some(b)) => Some(a % b),
                        _ => None,
                    }
                }).collect();
                Arc::new(result)
            }
        };
        Ok(res)
    }

    fn evaluate_arith_uint64(
        left: &ArrayRef,
        right: &ArrayRef,
        op: &crate::parser::ast::ArithmeticOperator,
    ) -> Result<ArrayRef> {
        let l = left
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| LightningError::Internal("Expected UInt64Array for left operand".into()))?;
        let r = right
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| LightningError::Internal("Expected UInt64Array for right operand".into()))?;

        use crate::parser::ast::ArithmeticOperator::*;
        let res: ArrayRef = match op {
            Add => {
                let raw = arrow::compute::kernels::numeric::add(l, r)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let has_overflow = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => a.overflowing_add(b).1,
                        _ => false,
                    }
                });
                if has_overflow {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 + b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    Arc::new(raw)
                }
            }
            Subtract => {
                let has_neg = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => b > a,
                        _ => false,
                    }
                });
                if has_neg {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 - b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    let raw = arrow::compute::kernels::numeric::sub(l, r)
                        .map_err(|e| LightningError::Internal(e.to_string()))?;
                    Arc::new(raw)
                }
            }
            Multiply => {
                let raw = arrow::compute::kernels::numeric::mul(l, r)
                    .map_err(|e| LightningError::Internal(e.to_string()))?;
                let has_overflow = l.iter().zip(r.iter()).any(|(a, b)| {
                    match (a, b) {
                        (Some(a), Some(b)) => a.overflowing_mul(b).1,
                        _ => false,
                    }
                });
                if has_overflow {
                    let f: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                        match (a, b) {
                            (Some(a), Some(b)) => Some(a as f64 * b as f64),
                            _ => None,
                        }
                    }).collect();
                    Arc::new(f)
                } else {
                    Arc::new(raw)
                }
            }
            Divide => {
                let result: arrow::array::Float64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                    match (a, b) {
                        (Some(_), Some(0)) => None,
                        (Some(a), Some(b)) => Some(a as f64 / b as f64),
                        _ => None,
                    }
                }).collect();
                Arc::new(result)
            }
            Modulo => {
                let result: arrow::array::UInt64Array = l.iter().zip(r.iter()).map(|(a, b)| {
                    match (a, b) {
                        (Some(_), Some(0)) => None,
                        (Some(a), Some(b)) => Some(a % b),
                        _ => None,
                    }
                }).collect();
                Arc::new(result)
            }
        };
        Ok(res)
    }

    fn compare_column_literal(
        col: &ArrayRef,
        lit: &Literal,
        op: &crate::parser::ast::ComparisonOperator,
        _num_rows: usize,
    ) -> Option<Result<ArrayRef>> {
        use crate::parser::ast::ComparisonOperator::*;
        if let Literal::Number(n) = lit {
            let val = *n as i64;
            let scalar = arrow::array::Int64Array::new_scalar(val);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                let res = match op {
                    Equal => eq(arr, &scalar),
                    NotEqual => neq(arr, &scalar),
                    LessThan => lt(arr, &scalar),
                    LessThanOrEqual => lt_eq(arr, &scalar),
                    GreaterThan => gt(arr, &scalar),
                    GreaterThanOrEqual => gt_eq(arr, &scalar),
                };
                return Some(res.map(|a| Arc::new(a) as ArrayRef).map_err(|e| LightningError::Internal(e.to_string())));
            }
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                let scalar_u = arrow::array::UInt64Array::new_scalar(val as u64);
                let res = match op {
                    Equal => eq(arr, &scalar_u),
                    NotEqual => neq(arr, &scalar_u),
                    LessThan => lt(arr, &scalar_u),
                    LessThanOrEqual => lt_eq(arr, &scalar_u),
                    GreaterThan => gt(arr, &scalar_u),
                    GreaterThanOrEqual => gt_eq(arr, &scalar_u),
                };
                return Some(res.map(|a| Arc::new(a) as ArrayRef).map_err(|e| LightningError::Internal(e.to_string())));
            }
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                let scalar_f = arrow::array::Float64Array::new_scalar(*n);
                let res = match op {
                    Equal => eq(arr, &scalar_f),
                    NotEqual => neq(arr, &scalar_f),
                    LessThan => lt(arr, &scalar_f),
                    LessThanOrEqual => lt_eq(arr, &scalar_f),
                    GreaterThan => gt(arr, &scalar_f),
                    GreaterThanOrEqual => gt_eq(arr, &scalar_f),
                };
                return Some(res.map(|a| Arc::new(a) as ArrayRef).map_err(|e| LightningError::Internal(e.to_string())));
            }
        }
        if let Literal::String(s) = lit {
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                let scalar_s = arrow::array::StringArray::new_scalar(s);
                let res = match op {
                    Equal => eq(arr, &scalar_s),
                    NotEqual => neq(arr, &scalar_s),
                    _ => return None,
                };
                return Some(res.map(|a| Arc::new(a) as ArrayRef).map_err(|e| LightningError::Internal(e.to_string())));
            }
        }
        None
    }

    pub fn evaluate_list_filter(
        list_array: &ArrayRef,
        var: &str,
        body: &BoundExpression,
        params: Option<&HashMap<String, Value>>,
        registry: &crate::processor::functions::FunctionRegistry,
        database: &crate::Database,
    ) -> Result<ArrayRef> {
        let list_arr = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| LightningError::Internal("Expected ListArray".into()))?;

        let mut filtered_values = Vec::new();
        let mut new_offsets = Vec::with_capacity(list_arr.len() + 1);
        new_offsets.push(0);

        // Build the schema once from the first non-empty list element.
        // All elements share the same data type in a fixed-type list column.
        let canned_schema: Option<Arc<Schema>> = 'schema: {
            for i in 0..list_arr.len() {
                let values = list_arr.value(i);
                if !values.is_empty() {
                    break 'schema Some(Arc::new(Schema::new(vec![Field::new(
                        var,
                        values.data_type().clone(),
                        true,
                    )])));
                }
            }
            None
        };

        for i in 0..list_arr.len() {
            let values = list_arr.value(i);
            let prev = *new_offsets.last().unwrap_or(&0);
            if values.is_empty() {
                new_offsets.push(prev);
                continue;
            }

            let schema = canned_schema.clone().unwrap_or_else(|| Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )])));
            let sub_batch = RecordBatch::try_new(schema, vec![values.clone()])?;
            let res = Self::evaluate(
                body,
                Some(&sub_batch),
                params,
                values.len(),
                registry,
                database,
            )?;
            new_offsets.push(prev + res.len() as i32);
            filtered_values.push(res);
        }

        let flat_values = arrow::compute::concat(
            &filtered_values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| LightningError::Internal(e.to_string()))?;

        let field = Arc::new(Field::new("item", flat_values.data_type().clone(), true));
        let offset_buffer =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(new_offsets));

        let result = ListArray::try_new(field, offset_buffer, flat_values, None)
            .map_err(|e| LightningError::Internal(e.to_string()))?;
        Ok(Arc::new(result))
    }

    pub fn evaluate_list_transform(
        list_array: &ArrayRef,
        var: &str,
        body: &BoundExpression,
        params: Option<&HashMap<String, Value>>,
        registry: &crate::processor::functions::FunctionRegistry,
        database: &crate::Database,
    ) -> Result<ArrayRef> {
        let list_arr = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| LightningError::Internal("Expected ListArray".into()))?;

        let mut transformed_values = Vec::new();
        let mut new_offsets = Vec::with_capacity(list_arr.len() + 1);
        new_offsets.push(0);

        // Build schema once from the first non-empty element
        let canned_schema: Option<Arc<Schema>> = 'schema: {
            for i in 0..list_arr.len() {
                let values = list_arr.value(i);
                if !values.is_empty() {
                    break 'schema Some(Arc::new(Schema::new(vec![Field::new(
                        var,
                        values.data_type().clone(),
                        true,
                    )])));
                }
            }
            None
        };

        for i in 0..list_arr.len() {
            let values = list_arr.value(i);
            let prev = *new_offsets.last().unwrap_or(&0);
            if values.is_empty() {
                new_offsets.push(prev);
                continue;
            }

            let schema = canned_schema.clone().unwrap_or_else(|| Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )])));
            let sub_batch = RecordBatch::try_new(schema, vec![values.clone()])?;

            let res = Self::evaluate(
                body,
                Some(&sub_batch),
                params,
                values.len(),
                registry,
                database,
            )?;
            new_offsets.push(prev + res.len() as i32);
            transformed_values.push(res);
        }

        let flat_values = arrow::compute::concat(
            &transformed_values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| LightningError::Internal(e.to_string()))?;

        let field = Arc::new(Field::new("item", flat_values.data_type().clone(), true));
        let offset_buffer =
            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(new_offsets));

        let result = ListArray::try_new(field, offset_buffer, flat_values, None)
            .map_err(|e| LightningError::Internal(e.to_string()))?;
        Ok(Arc::new(result))
    }

    fn evaluate_list_predicate(
        list_array: &ArrayRef,
        var: &str,
        body: &BoundExpression,
        op: &str,
        params: Option<&HashMap<String, Value>>,
        registry: &crate::processor::functions::FunctionRegistry,
        database: &crate::Database,
    ) -> Result<ArrayRef> {
        let list_arr = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| LightningError::Internal("Expected ListArray".into()))?;

        let mut results = Vec::with_capacity(list_arr.len());

        // Build schema once from the first non-empty element.
        let canned_schema: Option<Arc<Schema>> = 'schema: {
            for i in 0..list_arr.len() {
                let values = list_arr.value(i);
                if !values.is_empty() {
                    break 'schema Some(Arc::new(Schema::new(vec![Field::new(
                        var,
                        values.data_type().clone(),
                        true,
                    )])));
                }
            }
            None
        };

        for i in 0..list_arr.len() {
            let values = list_arr.value(i);
            if values.is_empty() {
                results.push(match op {
                    "LIST_ALL" => true,
                    "LIST_NONE" => true,
                    _ => false,
                });
                continue;
            }

            let schema = canned_schema.clone().unwrap_or_else(|| Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )])));
            let sub_batch = RecordBatch::try_new(schema, vec![values.clone()])?;

            let bool_res = Self::evaluate(
                body,
                Some(&sub_batch),
                params,
                values.len(),
                registry,
                database,
            )?;
            let bool_arr = bool_res
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| LightningError::Internal("Expected BooleanArray".into()))?;

            let mut true_count = 0;
            for k in 0..bool_arr.len() {
                if bool_arr.value(k) {
                    true_count += 1;
                }
            }

            results.push(match op {
                "LIST_ANY" => true_count > 0,
                "LIST_ALL" => true_count == values.len(),
                "LIST_SINGLE" => true_count == 1,
                "LIST_NONE" => true_count == 0,
                _ => false,
            });
        }

        Ok(Arc::new(BooleanArray::from(results)))
    }
}

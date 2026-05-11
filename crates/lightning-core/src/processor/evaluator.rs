use crate::parser::ast::Literal;
use crate::planner::binder::BoundExpression;
use crate::processor::Value;
use crate::{LightningError, Result};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, Float64Array, ListArray, RecordBatch,
};
use arrow::compute::cast;
use arrow::compute::kernels::boolean::{and, or};
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
                Literal::String(s) => Ok(Arc::new(arrow::array::StringArray::from_iter_values(
                    std::iter::repeat(s.as_str()).take(num_rows),
                ))),
                Literal::Boolean(b) => Ok(Arc::new(BooleanArray::from(vec![*b; num_rows]))),
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
                    "Variable {} not found in batch",
                    name
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
                        if let Some(result) = Self::compare_column_literal(
                            b.column(*col_idx), lit, op, num_rows,
                        ) {
                            return result;
                        }
                    }
                    // Literal op Column
                    if let (
                        BoundExpression::Literal(lit),
                        BoundExpression::PropertyLookup(_, col_idx, _),
                    ) = (&**left, &**right)
                    {
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
                let l = cast(
                    &Self::evaluate(left, batch, params, num_rows, registry, database)?,
                    &DataType::Float64,
                )
                .map_err(|e| LightningError::Internal(e.to_string()))?;
                let r = cast(
                    &Self::evaluate(right, batch, params, num_rows, registry, database)?,
                    &DataType::Float64,
                )
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
                        let false_count = l_bool.values().count_set_bits();
                        if false_count == 0 {
                            return Ok(Arc::new(BooleanArray::from(vec![false; num_rows])));
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

                let mut result = BooleanBufferBuilder::new(arr.len());
                for i in 0..arr.len() {
                    result.append(!arr.value(i));
                }
                Ok(Arc::new(BooleanArray::from(result.finish())))
            }
            BoundExpression::Function(name, args, _) => {
                let mut arg_arrays = Vec::new();
                for arg in args {
                    arg_arrays.push(Self::evaluate(
                        arg, batch, params, num_rows, registry, database,
                    )?);
                }

                if let Some(func) = registry.get_scalar_function(name) {
                    return func.execute(&arg_arrays, num_rows);
                }

                match name.as_str() {
                    "LIST_FILTER" => {
                        let list_array = arg_arrays[0].clone();
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            Self::evaluate_list_filter(
                                &list_array,
                                var,
                                body,
                                params,
                                registry,
                                database,
                            )
                        } else {
                            Err(LightningError::Internal(
                                "LIST_FILTER requires lambda".into(),
                            ))
                        }
                    }
                    "LIST_TRANSFORM" => {
                        let list_array = arg_arrays[0].clone();
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            Self::evaluate_list_transform(
                                &list_array,
                                var,
                                body,
                                params,
                                registry,
                                database,
                            )
                        } else {
                            Err(LightningError::Internal(
                                "LIST_TRANSFORM requires lambda".into(),
                            ))
                        }
                    }
                    "LIST_ANY" | "LIST_ALL" | "LIST_SINGLE" | "LIST_NONE" => {
                        let list_array = arg_arrays[0].clone();
                        let lambda = &args[1];
                        if let BoundExpression::Lambda(var, body) = lambda {
                            Self::evaluate_list_predicate(
                                &list_array,
                                var,
                                body,
                                name.as_str(),
                                params,
                                registry,
                                database,
                            )
                        } else {
                            Err(LightningError::Internal(
                                format!("{} requires lambda", name).into(),
                            ))
                        }
                    }
                    _ => Err(LightningError::Internal(format!(
                        "Function {} not implemented",
                        name
                    ))),
                }
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
            BoundExpression::Parameter(name) => {
                let val = params.and_then(|p| p.get(name)).ok_or_else(|| {
                    LightningError::Query(format!("Parameter {} not found", name))
                })?;
                match val {
                    Value::Number(n) => Ok(Arc::new(Float64Array::from(vec![*n; num_rows]))),
                    Value::String(s) => Ok(Arc::new(arrow::array::StringArray::from(vec![
                        s.as_str();
                        num_rows
                    ]))),
                    Value::Boolean(b) => Ok(Arc::new(BooleanArray::from(vec![*b; num_rows]))),
                    _ => Err(LightningError::Internal(format!(
                        "Parameter type not implemented for evaluation: {:?}",
                        val
                    ))),
                }
            }
            _ => Err(LightningError::Internal(format!(
                "Expression evaluation not implemented: {:?}",
                expr
            ))),
        }
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

        for i in 0..list_arr.len() {
            let values = list_arr.value(i);
            if values.is_empty() {
                new_offsets.push(*new_offsets.last().unwrap());
                continue;
            }

            let schema = Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )]));
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

            let filtered = arrow::compute::filter(&values, bool_arr)
                .map_err(|e| LightningError::Internal(e.to_string()))?;
            new_offsets.push(*new_offsets.last().unwrap() + filtered.len() as i32);
            filtered_values.push(filtered);
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

        for i in 0..list_arr.len() {
            let values = list_arr.value(i);
            if values.is_empty() {
                new_offsets.push(*new_offsets.last().unwrap());
                continue;
            }

            let schema = Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )]));
            let sub_batch = RecordBatch::try_new(schema, vec![values.clone()])?;

            let res = Self::evaluate(
                body,
                Some(&sub_batch),
                params,
                values.len(),
                registry,
                database,
            )?;
            new_offsets.push(*new_offsets.last().unwrap() + res.len() as i32);
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

            let schema = Arc::new(Schema::new(vec![Field::new(
                var,
                values.data_type().clone(),
                true,
            )]));
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

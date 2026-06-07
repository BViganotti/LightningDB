use lightning_core::processor::Value;
use lightning_core::processor::evaluator::ExpressionEvaluator;
use lightning_core::planner::binder::BoundExpression;
use lightning_core::parser::ast::{ArithmeticOperator, ComparisonOperator, Literal};
use lightning_types::LogicalType;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use lightning_core::processor::arrow_utils::values_to_array;

#[test]
fn test_expression_evaluator() {
    // Construct a mocked RecordBatch directly
    let col0 = values_to_array(&[Value::Number(10.0), Value::Number(20.0), Value::Number(30.0)], &DataType::Float64);
    let col1 = values_to_array(&[Value::String("a".into()), Value::String("B".into()), Value::String("c".into())], &DataType::Utf8);
    let schema = Arc::new(Schema::new(vec![
        Field::new("col0", DataType::Float64, false),
        Field::new("col1", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![col0, col1]).unwrap();
    let db_arc = lightning_core::Database::new(":memory:", lightning_core::SystemConfig::default()).unwrap();
    let database = &*db_arc;
    let fr = database.function_registry.read();
    let registry = &*fr;

    // 1. Literal Evaluation
    let expr1 = BoundExpression::Literal(Literal::Number(5.0));
    let res1 = ExpressionEvaluator::evaluate(&expr1, Some(&batch), None, 3, registry, database).unwrap();
    assert_eq!(Value::from_arrow(&res1, 0), Value::Number(5.0));
    assert_eq!(Value::from_arrow(&res1, 1), Value::Number(5.0));
    assert_eq!(Value::from_arrow(&res1, 2), Value::Number(5.0));

    // 2. Variable/Property Evaluation
    let expr2 = BoundExpression::PropertyLookup("p".into(), 0, LogicalType::Int64);
    let res2 = ExpressionEvaluator::evaluate(&expr2, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res2, 0), Value::Number(10.0));
    assert_eq!(Value::from_arrow(&res2, 1), Value::Number(20.0));
    assert_eq!(Value::from_arrow(&res2, 2), Value::Number(30.0));

    // 3. Arithmetic Evaluation (col0 + 5)
    let expr3 = BoundExpression::Arithmetic(
        Box::new(BoundExpression::PropertyLookup("p".into(), 0, LogicalType::Int64)),
        ArithmeticOperator::Add,
        Box::new(BoundExpression::Literal(Literal::Number(5.0))),
    );
    let res3 = ExpressionEvaluator::evaluate(&expr3, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res3, 0), Value::Number(15.0));
    assert_eq!(Value::from_arrow(&res3, 1), Value::Number(25.0));
    assert_eq!(Value::from_arrow(&res3, 2), Value::Number(35.0));

    // 4. Comparison Evaluation (col0 > 15)
    let expr4 = BoundExpression::Comparison(
        Box::new(BoundExpression::PropertyLookup("p".into(), 0, LogicalType::Int64)),
        ComparisonOperator::GreaterThan,
        Box::new(BoundExpression::Literal(Literal::Number(15.0))),
    );
    let res4 = ExpressionEvaluator::evaluate(&expr4, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res4, 0), Value::Boolean(false));
    assert_eq!(Value::from_arrow(&res4, 1), Value::Boolean(true));
    assert_eq!(Value::from_arrow(&res4, 2), Value::Boolean(true));

    // 5. Function Evaluation: UPPER(col1)
    let expr5 = BoundExpression::Function(
        "UPPER".into(),
        vec![BoundExpression::PropertyLookup("p".into(), 1, LogicalType::String)],
        LogicalType::String,
    );
    let res5 = ExpressionEvaluator::evaluate(&expr5, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res5, 0), Value::String("A".into()));
    assert_eq!(Value::from_arrow(&res5, 1), Value::String("B".into()));
    assert_eq!(Value::from_arrow(&res5, 2), Value::String("C".into()));

    // 6. Function Evaluation: LOWER(col1)
    let expr6 = BoundExpression::Function(
        "LOWER".into(),
        vec![BoundExpression::PropertyLookup("p".into(), 1, LogicalType::String)],
        LogicalType::String,
    );
    let res6 = ExpressionEvaluator::evaluate(&expr6, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res6, 0), Value::String("a".into()));
    assert_eq!(Value::from_arrow(&res6, 1), Value::String("b".into()));
    assert_eq!(Value::from_arrow(&res6, 2), Value::String("c".into()));

    // 7. Function Evaluation: CAST(col0 AS STRING) aka TO_STRING
    let expr7 = BoundExpression::Function(
        "TO_STRING".into(),
        vec![BoundExpression::PropertyLookup("p".into(), 0, LogicalType::Int64)],
        LogicalType::String,
    );
    let res7 = ExpressionEvaluator::evaluate(&expr7, Some(&batch), None, 3, registry, &database).unwrap();
    assert_eq!(Value::from_arrow(&res7, 0), Value::String("10.0".into()));
    assert_eq!(Value::from_arrow(&res7, 1), Value::String("20.0".into()));
    assert_eq!(Value::from_arrow(&res7, 2), Value::String("30.0".into()));
    assert_eq!(Value::from_arrow(&res7, 0), Value::String("10.0".into()));
    assert_eq!(Value::from_arrow(&res7, 1), Value::String("20.0".into()));
    assert_eq!(Value::from_arrow(&res7, 2), Value::String("30.0".into()));
}

use lightning_core::Database;
use lightning_core::planner::logical_plan::LogicalOperator;
use lightning_core::planner::binder::BoundExpression;
use lightning_types::LogicalType;
use tempfile::tempdir;
use arrow::array::AsArray;
// use std::sync::Arc;

#[test]
fn test_unwind_dedup_manual_plan() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), Default::default()).unwrap();
    
    // 1. Setup a manual plan: 
    // SingleRow -> Unwind([1, 1, 2, 2, 3], "x") -> UnwindDedup("x")
    
    let list_expr = BoundExpression::List(vec![
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(1.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(1.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(2.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(2.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(3.0)),
    ], LogicalType::List(Box::new(LogicalType::Double)));

    let unwind = LogicalOperator::Unwind(
        Box::new(LogicalOperator::SingleRow),
        list_expr,
        "x".to_string()
    );

    // Key to dedup on is "x" (the only column in the output of unwind)
    // In physical planning, this will be resolved to index 0.
    let key_expr = BoundExpression::Variable("x".to_string(), LogicalType::Double);

    let dedup = LogicalOperator::UnwindDedup(
        Box::new(unwind),
        key_expr
    );

    let undo_buffer = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());
    let mut planner = lightning_core::processor::physical_plan::PhysicalPlanner::new(db.clone(), 0, 0, undo_buffer);
    let mut physical_plan = planner.plan(dedup).unwrap();

    let tx = db.transaction_manager().begin(false).unwrap();
    let mut all_values = Vec::new();
    while let Some(chunk) = physical_plan.get_next(&db, &tx, None).unwrap() {
        let batch = chunk.batch;
        assert_eq!(batch.num_columns(), 1);
        let col = batch.column(0).as_primitive::<arrow::datatypes::Float64Type>();
        for i in 0..batch.num_rows() {
            all_values.push(col.value(i));
        }
    }

    // 3. Verify
    all_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(all_values, vec![1.0, 2.0, 3.0]);
}

#[test]
fn test_unwind_dedup_multiple_batches() {
    let dir = tempdir().unwrap();
    let db = Database::new(dir.path(), Default::default()).unwrap();
    
    // This is a bit more complex. UnwindDedup state persists across batches.
    // We'll use two Unwind operations or just verify it handles multiple chunks if they occur.
    // Our PhysicalUnwind currently emits one row per batch (it's a row-by-row operator effectively).
    // So it will definitely emit multiple batches.

    let list_expr = BoundExpression::List(vec![
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(1.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(1.0)),
        BoundExpression::Literal(lightning_core::parser::ast::Literal::Number(2.0)),
    ], LogicalType::List(Box::new(LogicalType::Double)));

    let unwind = LogicalOperator::Unwind(
        Box::new(LogicalOperator::SingleRow),
        list_expr,
        "x".to_string()
    );

    let key_expr = BoundExpression::Variable("x".to_string(), LogicalType::Double);
    let dedup = LogicalOperator::UnwindDedup(Box::new(unwind), key_expr);

    let undo_buffer = std::sync::Arc::new(lightning_core::storage::undo_buffer::UndoBuffer::new());
    let mut planner = lightning_core::processor::physical_plan::PhysicalPlanner::new(db.clone(), 0, 0, undo_buffer);
    let mut physical_plan = planner.plan(dedup).unwrap();

    let tx = db.transaction_manager().begin(false).unwrap();
    let mut count = 0;
    while let Some(chunk) = physical_plan.get_next(&db, &tx, None).unwrap() {
        count += chunk.batch.num_rows();
    }
    assert_eq!(count, 2); // 1.0, 2.0 (second 1.0 is deduped)
}

use lightning_core::processor::operators::hash_join::HashJoin;
use lightning_core::processor::{PhysicalOperator, DataChunk};
use arrow::record_batch::RecordBatch;
use arrow::array::{UInt64Array, StringArray, Float64Array};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

#[derive(Clone)]
struct MockOperator {
    batches: Vec<RecordBatch>,
    pos: usize,
}

impl MockOperator {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches, pos: 0 }
    }
}

impl PhysicalOperator for MockOperator {
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> { Box::new((*self).clone()) }
    fn get_next(&mut self, _database: &lightning_core::Database, _tx: &lightning_core::transaction::transaction_manager::Transaction, _params: Option<&std::collections::HashMap<String, lightning_core::processor::Value>>) -> lightning_core::Result<Option<DataChunk>> {
        if self.pos < self.batches.len() {
            let batch = self.batches[self.pos].clone();
            self.pos += 1;
            Ok(Some(DataChunk { batch }))
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_hash_join_vectorized() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    let left_batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(UInt64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["A", "B", "C"])),
    ]).unwrap();
    
    let right_batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(UInt64Array::from(vec![2, 3, 4])),
        Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
    ]).unwrap();
    
    let left = Box::new(MockOperator::new(vec![left_batch]));
    let right = Box::new(MockOperator::new(vec![right_batch]));
    
    // Join on 'id' (index 0)
    let mut join = HashJoin::new(left, right, 0, 0);
    
    let db = lightning_core::Database::new(tempfile::tempdir().unwrap().path(), Default::default()).unwrap();
    let tx = db.transaction_manager.begin(false).unwrap();
    let res = join.get_next(&db, &tx, None).unwrap().expect("Should have output");
    let batch = res.batch;
    
    assert_eq!(batch.num_rows(), 2); // 2 and 3 match
    assert_eq!(batch.num_columns(), 4);
    
    // Check results
    let ids = batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    assert!(ids.value(0) == 2 || ids.value(1) == 2);
    assert!(ids.value(0) == 3 || ids.value(1) == 3);
}

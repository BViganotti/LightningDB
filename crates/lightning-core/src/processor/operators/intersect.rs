use crate::processor::arrow_utils::values_to_array;
use crate::processor::{DataChunk, PhysicalOperator, Value};
use crate::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhysicalIntersect {
    child: Box<dyn PhysicalOperator + Send + Sync>,
    probe_key_indices: Vec<usize>,
    build_operators: Vec<Box<dyn PhysicalOperator + Send + Sync>>,
    build_key_indices: Vec<usize>,
    build_intersect_indices: Vec<usize>,
    build_hts: Vec<Arc<RwLock<HashMap<Value, Vec<Vec<Value>>>>>>,
    intersect_var_name: String,

    // Internal state
    build_done: Arc<std::sync::atomic::AtomicBool>,
    current_probe_chunk: Option<DataChunk>,
    probe_row_idx: usize,
    results: Vec<Vec<Value>>, // Collected rows for the current batch
    probe_schema: Option<Arc<Schema>>,
}

impl PhysicalIntersect {
    pub fn new(
        child: Box<dyn PhysicalOperator + Send + Sync>,
        probe_key_indices: Vec<usize>,
        build_operators: Vec<Box<dyn PhysicalOperator + Send + Sync>>,
        build_key_indices: Vec<usize>,
        build_intersect_indices: Vec<usize>,
        intersect_var_name: String,
    ) -> Self {
        let mut build_hts = Vec::new();
        for _ in 0..build_operators.len() {
            build_hts.push(Arc::new(RwLock::new(HashMap::new())));
        }
        Self {
            child,
            probe_key_indices,
            build_operators,
            build_key_indices,
            build_intersect_indices,
            build_hts,
            intersect_var_name,
            build_done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            current_probe_chunk: None,
            probe_row_idx: 0,
            results: Vec::new(),
            probe_schema: None,
        }
    }

    fn build(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        if self
            .build_done
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(());
        }
        for i in 0..self.build_operators.len() {
            let ht_arc = self.build_hts[i].clone();
            let mut ht = ht_arc.write();
            let key_idx = self.build_key_indices[i];

            while let Some(chunk) = self.build_operators[i].get_next(database, tx, params)? {
                let batch = &chunk.batch;
                let num_rows = batch.num_rows();
                let num_cols = batch.num_columns();
                for row_idx in 0..num_rows {
                    let key = Value::from_arrow(batch.column(key_idx), row_idx);
                    let mut row = Vec::with_capacity(num_cols);
                    for col_idx in 0..num_cols {
                        row.push(Value::from_arrow(batch.column(col_idx), row_idx));
                    }
                    ht.entry(key).or_insert_with(Vec::new).push(row);
                }
            }
        }
        Ok(())
    }
}

impl PhysicalOperator for PhysicalIntersect {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>> {
        self.build(database, tx, params)?;
        loop {
            if self.current_probe_chunk.is_none() {
                self.current_probe_chunk = self.child.get_next(database, tx, params)?;
                if let Some(chunk) = &self.current_probe_chunk {
                    if self.probe_schema.is_none() {
                        self.probe_schema = Some(chunk.batch.schema());
                    }
                }
                self.probe_row_idx = 0;
                if self.current_probe_chunk.is_none() {
                    return Ok(None);
                }
            }

            let mut output_ready = false;
            {
                let chunk = self.current_probe_chunk.as_ref()
                    .expect("current_probe_chunk should be Some when processing");
                let batch = &chunk.batch;
                let num_rows = batch.num_rows();
                let num_cols = batch.num_columns();

                while self.probe_row_idx < num_rows {
                    let mut out_rows = Vec::new();
                    {
                        let mut matches_per_ht = Vec::new();
                        let mut min_size = usize::MAX;
                        let mut smallest_idx = 0;
                        let mut possible = true;

                        let ht_read_locks: Vec<_> =
                            self.build_hts.iter().map(|ht| ht.read()).collect();

                        for (i, ht) in ht_read_locks.iter().enumerate() {
                            let key = Value::from_arrow(
                                batch.column(self.probe_key_indices[i]),
                                self.probe_row_idx,
                            );
                            if let Some(matches) = ht.get(&key) {
                                if matches.is_empty() {
                                    possible = false;
                                    break;
                                }
                                if matches.len() < min_size {
                                    min_size = matches.len();
                                    smallest_idx = i;
                                }
                                matches_per_ht.push(matches);
                            } else {
                                possible = false;
                                break;
                            }
                        }

                        if possible
                            && matches_per_ht.len() == self.build_hts.len()
                            && !matches_per_ht.is_empty()
                        {
                            let base_matches = matches_per_ht[smallest_idx];
                            let base_intersect_idx = self.build_intersect_indices[smallest_idx];

                            for candidate_row in base_matches {
                                let candidate_val = &candidate_row[base_intersect_idx];

                                let mut in_all = true;
                                for (i, matches) in matches_per_ht.iter().enumerate() {
                                    if i == smallest_idx {
                                        continue;
                                    }
                                    let intersect_idx = self.build_intersect_indices[i];
                                    if !matches.iter().any(|r| &r[intersect_idx] == candidate_val) {
                                        in_all = false;
                                        break;
                                    }
                                }

                                if in_all {
                                    let mut out_row = Vec::with_capacity(num_cols + 1);
                                    for col_idx in 0..num_cols {
                                        out_row.push(Value::from_arrow(
                                            batch.column(col_idx),
                                            self.probe_row_idx,
                                        ));
                                    }
                                    out_row.push(candidate_val.clone());
                                    out_rows.push(out_row);
                                }
                            }
                        }
                    }
                    self.results.extend(out_rows);
                    self.probe_row_idx += 1;
                    if self.results.len() >= 1024 {
                        output_ready = true;
                        break;
                    }
                }
            }

            if !output_ready {
                self.current_probe_chunk = None;
            }

            if !self.results.is_empty() {
                let mut final_columns = Vec::new();
                let mut fields = Vec::new();

                if let Some(schema) = &self.probe_schema {
                    let num_probe_cols = schema.fields().len();
                    for col_idx in 0..num_probe_cols {
                        let col_values: Vec<Value> = self
                            .results
                            .iter()
                            .map(|row| row[col_idx].clone())
                            .collect();
                        let field = schema.field(col_idx);
                        final_columns.push(values_to_array(&col_values, field.data_type()));
                        fields.push((*field).clone());
                    }

                    // Intersected column
                    let col_idx = num_probe_cols;
                    let col_values: Vec<Value> = self
                        .results
                        .iter()
                        .map(|row| row[col_idx].clone())
                        .collect();
                    let data_type = DataType::UInt64;
                    final_columns.push(values_to_array(&col_values, &data_type));
                    fields.push(Field::new(&self.intersect_var_name, data_type, true));

                    self.results.clear();
                    let schema_obj = Arc::new(Schema::new(fields));
                    let batch = RecordBatch::try_new(schema_obj, final_columns)
                        .map_err(|e| crate::LightningError::Internal(e.to_string()))?;

                    return Ok(Some(DataChunk { batch }));
                }
            }

            if self.current_probe_chunk.is_none() {
                return Ok(None);
            }
        }
    }

    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync> {
        Box::new(Self {
            child: self.child.clone_box(),
            probe_key_indices: self.probe_key_indices.clone(),
            build_operators: self
                .build_operators
                .iter()
                .map(|op| {
                    let boxed: Box<dyn PhysicalOperator + Send + Sync> = op.clone_box();
                    boxed
                })
                .collect(),
            build_key_indices: self.build_key_indices.clone(),
            build_intersect_indices: self.build_intersect_indices.clone(),
            build_hts: self.build_hts.clone(),
            intersect_var_name: self.intersect_var_name.clone(),
            build_done: self.build_done.clone(),
            current_probe_chunk: None,
            probe_row_idx: 0,
            results: Vec::new(),
            probe_schema: self.probe_schema.clone(),
        })
    }
}

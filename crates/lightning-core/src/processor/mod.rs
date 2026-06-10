pub mod aggregate;
pub mod arrow_utils;
pub mod evaluator;
pub mod functions;
pub mod operators;
pub mod physical_plan;
pub mod scheduler;

pub use operators::*;

use crate::Result;

use arrow::array::{ArrayRef, AsArray};
use arrow::record_batch::RecordBatch;
use crossbeam::channel::Receiver;
use std::sync::Arc;

#[derive(Clone)]
pub struct DataChunk {
    pub batch: RecordBatch,
}

impl DataChunk {
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

pub trait PhysicalOperator: Send + Sync {
    fn get_next(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
        params: Option<&std::collections::HashMap<String, Value>>,
    ) -> Result<Option<DataChunk>>;
    fn clone_box(&self) -> Box<dyn PhysicalOperator + Send + Sync>;
    fn is_single_row(&self) -> bool {
        false
    }

    /// Whether this operator is safe to execute in parallel.
    /// Parallel-safe operators are stateless: different workers can process
    /// disjoint row ranges independently. Returns `true` for Scan, Filter,
    /// Projection, Map, Expression evaluation. Returns `false` for operators
    /// that must see all data (Sort, Aggregate, Join, Limit, DML, DDL).
    fn is_parallel_safe(&self) -> bool {
        false
    }

    /// When executing in parallel, informs this operator (and its children)
    /// which partition of the data it should process. `index` is the partition
    /// number (0-based) and `total` is the total number of partitions.
    /// Only meaningful for operators that wrap a PhysicalScan leaf.
    /// Default implementation is a no-op — operators that wrap a child
    /// should forward the call.
    fn set_partition(&mut self, _index: usize, _total: usize) {}

    /// If this operator can be parallelized into N workers + a merge step,
    /// returns the merged operator tree. Default returns None.
    /// Sort implements this to create N partition-sorted workers + NWayMerge.
    fn try_parallelize(
        &self,
        _num_workers: usize,
    ) -> Result<Option<Box<dyn PhysicalOperator + Send + Sync>>> {
        Ok(None)
    }

    /// Return the output schema of this operator, if known.
    /// Useful for optimizers and EXPLAIN to understand the data flow.
    fn output_schema(&self) -> Option<Arc<arrow::datatypes::Schema>> {
        None
    }

    /// Whether this operator performs writes (DML, DDL, checkpoint, vacuum, COPY FROM).
    /// Read-only operators (Scan, Filter, Projection, Sort, etc.) can have their
    /// physical plans cached and shared across transactions without read_ts/tx_id
    /// in the cache key. Write operators must either not be cached or use a
    /// transaction-specific key.
    fn is_read_only(&self) -> bool {
        true
    }
}

pub struct Processor {
    pub root: Box<dyn PhysicalOperator>,
}

impl Processor {
    pub fn new(root: Box<dyn PhysicalOperator>) -> Self {
        Self { root }
    }

    pub fn execute_simple(
        &mut self,
        database: &crate::Database,
        tx: &crate::transaction::transaction_manager::Transaction,
    ) -> Result<Option<DataChunk>> {
        self.root.get_next(database, tx, None)
    }

    #[tracing::instrument(skip(self, database, tx, params))]
    pub fn execute(
        &mut self,
        database: Arc<crate::Database>,
        tx: Arc<crate::transaction::transaction_manager::Transaction>,
        params: Option<std::collections::HashMap<String, Value>>,
    ) -> Result<Vec<DataChunk>> {
        let rx = self.execute_stream(database, tx, params)?;
        let mut results = Vec::new();
        while let Ok(res) = rx.recv() {
            let chunk = res?;
            results.push(chunk);
        }
        Ok(results)
    }

    /// Execute the query and return a channel receiver that yields chunks
    /// as they are produced. This enables streaming processing of large
    /// result sets without buffering everything in memory.
    ///
    /// The receiver yields `Result<DataChunk>`. When the query is complete,
    /// the channel is closed and `recv()` will return `Err(RecvError)`.
    pub fn execute_stream(
        &mut self,
        database: Arc<crate::Database>,
        tx: Arc<crate::transaction::transaction_manager::Transaction>,
        params: Option<std::collections::HashMap<String, Value>>,
    ) -> Result<Receiver<Result<DataChunk>>> {
        let num_threads = if database._config.max_num_threads == 0 {
            num_cpus::get()
        } else {
            database._config.max_num_threads as usize
        };

        let root = std::mem::replace(
            &mut self.root,
            Box::new(crate::processor::operators::PhysicalSingleRow::new()),
        );
        let scheduler = crate::processor::scheduler::Scheduler::new(num_threads);
        scheduler.execute_operator(root, database, tx, params)
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Number(f64),
    Boolean(bool),
    Null,
    Node(u64),         // ID
    Relationship(u64), // ID
    Path(Vec<Value>),  // List of Nodes and Relationships
    Date(i32),         // Days since epoch
    Timestamp(i64),    // Microseconds since epoch
    List(Vec<Value>),
    Struct(Vec<(String, Value)>),
    Map(std::collections::HashMap<Value, Value>),
}

impl Value {
    pub fn to_le_bytes(&self) -> Vec<u8> {
        match self {
            Value::String(s) => {
                let mut bytes = vec![0u8; 64];
                let s_bytes = s.as_bytes();
                let len = std::cmp::min(s_bytes.len(), 64);
                bytes[0..len].copy_from_slice(&s_bytes[0..len]);
                bytes
            }
            Value::Number(n) => n.to_le_bytes().to_vec(),
            Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
            Value::Null => vec![0u8; 8],
            Value::Node(id) | Value::Relationship(id) => id.to_le_bytes().to_vec(),
            Value::Date(d) => d.to_le_bytes().to_vec(),
            Value::Timestamp(t) => t.to_le_bytes().to_vec(),
            _ => vec![0u8; 8], // Complex types not yet serializable to raw bytes for internal storage
        }
    }
    pub fn to_arrow(&self, num_elements: usize) -> ArrayRef {
        match self {
            Value::String(s) => Arc::new(arrow::array::StringArray::from_iter_values(
                std::iter::repeat_n(s, num_elements),
            )),
            Value::Number(n) => Arc::new(arrow::array::Float64Array::from_iter_values(
                std::iter::repeat_n(*n, num_elements),
            )),
            Value::Boolean(b) => Arc::new(arrow::array::BooleanArray::from_iter(
                std::iter::repeat_n(Some(*b), num_elements),
            )),
            Value::Null => Arc::new(arrow::array::NullArray::new(num_elements)),
            Value::Node(id) | Value::Relationship(id) => {
                Arc::new(arrow::array::UInt64Array::from_iter_values(
                    std::iter::repeat_n(*id, num_elements),
                ))
            }
            Value::Path(p) => {
                // Convert Path to List for Arrow projection for now
                let v = Value::List(p.clone());
                v.to_arrow(num_elements)
            }
            Value::Date(d) => Arc::new(arrow::array::Date32Array::from_iter_values(
                std::iter::repeat_n(*d, num_elements),
            )),
            Value::Timestamp(t) => {
                Arc::new(arrow::array::TimestampMicrosecondArray::from_iter_values(
                    std::iter::repeat_n(*t, num_elements),
                ))
            }
            Value::List(l) => {
                if l.is_empty() || num_elements == 0 {
                    return Arc::new(arrow::array::NullArray::new(num_elements));
                }
                let first_arr = l[0].to_arrow(1);
                let elem_data_type = first_arr.data_type().clone();
                let elem_arrays: Vec<ArrayRef> = l.iter()
                    .map(|v| {
                        let arr = v.to_arrow(1);
                        if arr.data_type() != &elem_data_type {
                            arrow::compute::kernels::cast::cast(&arr, &elem_data_type)
                                .unwrap_or_else(|_| arrow::array::new_null_array(&elem_data_type, 1))
                        } else {
                            arr
                        }
                    })
                    .collect();
                let elem_refs: Vec<&dyn arrow::array::Array> =
                    elem_arrays.iter().map(|a| a.as_ref()).collect();
                let flat = arrow::compute::concat(&elem_refs)
                    .unwrap_or_else(|_| arrow::array::new_null_array(&elem_data_type, l.len()));
                let raw_offsets: Vec<i32> = (0..=num_elements as i32)
                    .map(|i| i * l.len() as i32)
                    .collect();
                let offset_buf =
                    arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(raw_offsets));
                let field = Arc::new(arrow::datatypes::Field::new("item", elem_data_type, true));
                Arc::new(arrow::array::ListArray::new(field, offset_buf, flat, None))
            }
            _ => Arc::new(arrow::array::NullArray::new(num_elements)),
        }
    }
}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::String(s1), Value::String(s2)) => s1 == s2,
            (Value::Number(n1), Value::Number(n2)) => n1.to_bits() == n2.to_bits(),
            (Value::Boolean(b1), Value::Boolean(b2)) => b1 == b2,
            (Value::Null, Value::Null) => true,
            (Value::Node(id1), Value::Node(id2)) => id1 == id2,
            (Value::Relationship(id1), Value::Relationship(id2)) => id1 == id2,
            (Value::Path(p1), Value::Path(p2)) => p1 == p2,
            (Value::Date(d1), Value::Date(d2)) => d1 == d2,
            (Value::Timestamp(t1), Value::Timestamp(t2)) => t1 == t2,
            (Value::List(l1), Value::List(l2)) => l1 == l2,
            (Value::Struct(s1), Value::Struct(s2)) => s1 == s2,
            (Value::Map(m1), Value::Map(m2)) => m1 == m2,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{s}"),
            Value::Number(n) => write!(f, "{n}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Null => write!(f, "null"),
            Value::Node(id) => write!(f, "node({id})"),
            Value::Relationship(id) => write!(f, "rel({id})"),
            Value::Path(p) => write!(f, "path({p:?})"),
            Value::Date(d) => write!(f, "date({d})"),
            Value::Timestamp(t) => write!(f, "timestamp({t})"),
            Value::List(l) => write!(f, "list({l:?})"),
            Value::Struct(s) => write!(f, "struct({s:?})"),
            Value::Map(m) => write!(f, "map({m:?})"),
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::String(s) => s.hash(state),
            Value::Number(n) => n.to_bits().hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Null => 0.hash(state),
            Value::Node(id) | Value::Relationship(id) => id.hash(state),
            Value::Path(p) => p.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Timestamp(t) => t.hash(state),
            Value::List(l) => l.hash(state),
            Value::Struct(s) => s.hash(state),
            Value::Map(m) => {
                // Sort by deterministic key hash to ensure equal maps
                // always produce the same hash regardless of insertion order.
                let mut entries: Vec<(u64, &Value, &Value)> = m.iter()
                    .map(|(k, v)| {
                        let mut kh = std::collections::hash_map::DefaultHasher::new();
                        k.hash(&mut kh);
                        (std::hash::Hasher::finish(&kh), k, v)
                    })
                    .collect();
                entries.sort_by_key(|(h, _, _)| *h);
                let mut h = 0u64;
                for (_, k, v) in entries {
                    let mut s = std::collections::hash_map::DefaultHasher::new();
                    k.hash(&mut s);
                    v.hash(&mut s);
                    h = h.wrapping_add(std::hash::Hasher::finish(&s));
                }
                h.hash(state);
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::String(s1), Value::String(s2)) => s1.partial_cmp(s2),
            (Value::Number(n1), Value::Number(n2)) => n1.partial_cmp(n2),
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.partial_cmp(b2),
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Node(id1), Value::Node(id2)) => id1.partial_cmp(id2),
            (Value::Date(d1), Value::Date(d2)) => d1.partial_cmp(d2),
            (Value::Timestamp(t1), Value::Timestamp(t2)) => t1.partial_cmp(t2),
            _ => None,
        }
    }
}

impl Value {
    pub fn from_arrow(array: &ArrayRef, i: usize) -> Self {
        if array.is_null(i) {
            return Value::Null;
        }

        match array.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                Value::String(array.as_string::<i32>().value(i).to_string())
            }
            arrow::datatypes::DataType::Float64 => Value::Number(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(i),
            ),
            arrow::datatypes::DataType::Boolean => Value::Boolean(array.as_boolean().value(i)),
            arrow::datatypes::DataType::UInt64 => Value::Node(
                array
                    .as_primitive::<arrow::datatypes::UInt64Type>()
                    .value(i),
            ),
            arrow::datatypes::DataType::Int64 => {
                Value::Number(array.as_primitive::<arrow::datatypes::Int64Type>().value(i) as f64)
            }
            arrow::datatypes::DataType::Int32 => {
                Value::Number(array.as_primitive::<arrow::datatypes::Int32Type>().value(i) as f64)
            }
            arrow::datatypes::DataType::Date32 => Value::Date(
                array
                    .as_primitive::<arrow::datatypes::Date32Type>()
                    .value(i),
            ),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                Value::Timestamp(
                    array
                        .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                        .value(i),
                )
            }
            arrow::datatypes::DataType::List(_field) => {
                let list_arr = array.as_list::<i32>();
                let values = list_arr.value(i);
                let mut result = Vec::with_capacity(values.len());
                for k in 0..values.len() {
                    result.push(Value::from_arrow(&values, k));
                }
                Value::List(result)
            }
            arrow::datatypes::DataType::Struct(fields) => {
                let struct_arr = array.as_struct();
                let mut result = Vec::with_capacity(fields.len());
                for (k, field) in fields.iter().enumerate() {
                    result.push((
                        field.name().clone(),
                        Value::from_arrow(struct_arr.column(k), i),
                    ));
                }
                Value::Struct(result)
            }
            _ => Value::Null,
        }
    }

    pub fn as_number(&self) -> f64 {
        match self {
            Value::Number(n) => *n,
            Value::Node(id) => *id as f64,
            _ => 0.0,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<Value>> {
        if let Value::List(l) = self {
            Some(l)
        } else {
            None
        }
    }

    pub fn as_node(&self) -> u64 {
        match self {
            Value::Node(id) => *id,
            Value::Number(n) => *n as u64,
            _ => 0,
        }
    }

    pub fn from_json(json: &serde_json::Value) -> Self {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Number(n) => Value::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(a) => Value::List(a.iter().map(Value::from_json).collect()),
            serde_json::Value::Object(o) => {
                let mut map = std::collections::HashMap::new();
                for (k, v) in o {
                    map.insert(Value::String(k.clone()), Value::from_json(v));
                }
                Value::Map(map)
            }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::Number(n) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*n).expect("internal invariant violated"))
            }
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Node(id) | Value::Relationship(id) => serde_json::Value::Number((*id).into()),
            Value::Date(d) => serde_json::Value::Number((*d).into()),
            Value::Timestamp(t) => serde_json::Value::Number((*t).into()),
            Value::List(l) => serde_json::Value::Array(l.iter().map(|v| v.to_json()).collect()),
            Value::Struct(s) => {
                let mut map = serde_json::Map::new();
                for (k, v) in s {
                    map.insert(k.clone(), v.to_json());
                }
                serde_json::Value::Object(map)
            }
            Value::Map(m) => {
                let mut map = serde_json::Map::new();
                for (k, v) in m {
                    map.insert(k.to_string(), v.to_json());
                }
                serde_json::Value::Object(map)
            }
            Value::Path(p) => serde_json::Value::Array(p.iter().map(|v| v.to_json()).collect()),
        }
    }
}

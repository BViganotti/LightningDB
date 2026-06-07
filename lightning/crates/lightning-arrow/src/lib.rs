use arrow::array::{Array, StructArray};
use arrow::record_batch::RecordBatch;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArrowError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Type conversion error: {0}")]
    TypeConversion(String),
}

pub type Result<T> = std::result::Result<T, ArrowError>;

pub struct ArrowResult {
    batches: Vec<RecordBatch>,
    current_batch: usize,
    schema: arrow::datatypes::SchemaRef,
}

impl ArrowResult {
    pub fn new(batches: Vec<RecordBatch>, schema: arrow::datatypes::SchemaRef) -> Self {
        Self {
            batches,
            current_batch: 0,
            schema,
        }
    }

    pub fn get_arrow_schema(&self) -> Result<FFI_ArrowSchema> {
        FFI_ArrowSchema::try_from(self.schema.as_ref())
            .map_err(|e| ArrowError::TypeConversion(e.to_string()))
    }

    pub fn get_next_arrow_chunk(&mut self) -> Result<Option<FFI_ArrowArray>> {
        if self.current_batch >= self.batches.len() {
            return Ok(None);
        }
        let batch = &self.batches[self.current_batch];
        self.current_batch += 1;
        
        // A RecordBatch is exported as a Struct array in the C Data Interface
        let struct_array: StructArray = batch.clone().into();
        let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
        Ok(Some(ffi_array))
    }
}

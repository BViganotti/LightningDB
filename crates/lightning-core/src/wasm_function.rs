use crate::processor::functions::ScalarFunction;
use crate::Result;
use arrow::array::{Array, ArrayRef, Float64Array};
use std::sync::Arc;

/// A WebAssembly function that can be called from Cypher queries.
///
/// Users write a WAT file that exports a function with signature
/// `(f64) -> f64`. The database compiles the WAT to WASM at load time.
///
/// Example WAT file (double.wat):
/// ```wat
/// (module
///   (func (export "double") (param f64) (result f64)
///     local.get 0
///     f64.const 2.0
///     f64.mul
///   )
/// )
/// ```
/// Usage from Cypher:
///   RETURN wasm_double(t.val)
pub struct WasmFunction {
    name: String,
    wasm_bytes: Vec<u8>,
    func_name: String,
}

impl WasmFunction {
    /// Load a WAT file, compile it to WASM, and extract the exported function.
    /// The file is compiled at load time using the `wat` crate.
    pub fn load<P: AsRef<std::path::Path>>(
        wat_path: P,
        func_name: &str,
    ) -> Result<Self> {
        let path = wat_path.as_ref();
        let wat_source = std::fs::read_to_string(path)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to read WAT file: {}", e
            )))?;

        let wasm_bytes = wat::parse_str(&wat_source)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to compile WAT to WASM: {}", e
            )))?;

        let name = format!("WASM_{}", func_name.to_uppercase());
        Ok(Self {
            name,
            wasm_bytes,
            func_name: func_name.to_string(),
        })
    }

    /// Load from a WAT string directly (for testing / inline definitions).
    pub fn from_wat(wat_source: &str, func_name: &str) -> Result<Self> {
        let wasm_bytes = wat::parse_str(wat_source)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to compile WAT to WASM: {}", e
            )))?;

        let name = format!("WASM_{}", func_name.to_uppercase());
        Ok(Self {
            name,
            wasm_bytes,
            func_name: func_name.to_string(),
        })
    }

    /// Convert this WASM function into a ScalarFunction for the query engine.
    /// The WASM function must accept (f64) -> f64.
    ///
    /// Engine and module are compiled once at registration time and reused
    /// across all invocations via cheap Clone. Each batch still creates a
    /// fresh Store + Instance for isolation.
    pub fn to_scalar_function(&self) -> ScalarFunction {
        let wasm = self.wasm_bytes.clone();
        let func_name = self.func_name.clone();
        let name = self.name.clone();

        // Compile engine and module once — the expensive part of WASM setup.
        // Module validation/compilation is the dominant cost; cloning Engine
        // and Module is cheap (internally Arc-backed).
        let engine = wasmi::Engine::default();
        let module = wasmi::Module::new(&engine, &wasm)
            .expect("WASM module compilation failed at registration time");

        let exec: crate::processor::functions::ScalarFunctionExec = Arc::new(
            move |args: &[ArrayRef], num_rows: usize| -> Result<ArrayRef> {
                let input = args[0].as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| crate::LightningError::Internal(
                        "WASM function requires Float64 input".into()
                    ))?;

                let mut store = wasmi::Store::new(&engine, ());
                let instance = wasmi::Instance::new(&mut store, &module, &[])
                    .map_err(|e| crate::LightningError::Internal(format!(
                        "WASM instantiation failed: {}", e
                    )))?;

                let func = instance.get_typed_func::<f64, f64>(&mut store, &func_name)
                    .map_err(|e| crate::LightningError::Internal(format!(
                        "WASM function '{}' not found: {}", func_name, e
                    )))?;

                let mut results = Vec::with_capacity(num_rows);
                for i in 0..num_rows {
                    let val = if input.is_valid(i) {
                        func.call(&mut store, input.value(i))
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM call failed: {}", e
                            )))?
                    } else {
                        f64::NAN
                    };
                    results.push(val);
                }

                Ok(Arc::new(Float64Array::from(results)))
            },
        );

        ScalarFunction::new(name, exec)
    }
}

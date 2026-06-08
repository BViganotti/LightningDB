use crate::processor::functions::ScalarFunction;
use crate::Result;
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, StringArray};
use std::sync::Arc;

/// Default maximum execution time for a single WASM function call.
const DEFAULT_WASM_TIMEOUT_MS: u64 = 100;

pub struct WasmFunction {
    name: String,
    wasm_bytes: Vec<u8>,
    func_name: String,
    exec_mode: WasmExecMode,
    timeout_ms: u64,
}

enum WasmExecMode {
    ScalarF64,
    MultiArgF64(usize),
    MemoryF32,
    MemoryString,
}

impl WasmExecMode {
    fn to_u8(&self) -> u8 {
        match self {
            Self::ScalarF64 => 0,
            Self::MultiArgF64(_) => 1,
            Self::MemoryF32 => 2,
            Self::MemoryString => 3,
        }
    }

    fn from_u8(v: u8, arg_count: usize) -> Self {
        match v {
            1 => Self::MultiArgF64(arg_count),
            2 => Self::MemoryF32,
            3 => Self::MemoryString,
            _ => Self::ScalarF64,
        }
    }
}

impl WasmFunction {
    pub fn load<P: AsRef<std::path::Path>>(
        wat_path: P,
        func_name: &str,
    ) -> Result<Self> {
        let path = wat_path.as_ref();
        let wat_source = std::fs::read_to_string(path)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to read WAT file: {e}"
            )))?;

        let wasm_bytes = wat::parse_str(&wat_source)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to compile WAT to WASM: {e}"
            )))?;

        let name = format!("WASM_{}", func_name.to_uppercase());
        Ok(Self {
            name,
            wasm_bytes,
            func_name: func_name.to_string(),
            exec_mode: WasmExecMode::ScalarF64,
            timeout_ms: DEFAULT_WASM_TIMEOUT_MS,
        })
    }

    pub fn from_wat(wat_source: &str, func_name: &str) -> Result<Self> {
        let wasm_bytes = wat::parse_str(wat_source)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to compile WAT to WASM: {e}"
            )))?;

        let name = format!("WASM_{}", func_name.to_uppercase());
        Ok(Self {
            name,
            wasm_bytes,
            func_name: func_name.to_string(),
            exec_mode: WasmExecMode::ScalarF64,
            timeout_ms: DEFAULT_WASM_TIMEOUT_MS,
        })
    }

    /// Set a custom execution timeout in milliseconds.
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Configure the WASM function to accept multiple f64 arguments.
    pub fn with_arity(mut self, arity: usize) -> Self {
        self.exec_mode = WasmExecMode::MultiArgF64(arity);
        self
    }

    /// Configure the WASM function for f32 vector operations via shared memory.
    /// The WASM function should export `func_name` with signature `(i32, i32) -> i32`
    /// where the first i32 is the memory offset of the input f32 array,
    /// the second i32 is the number of elements, and the returned i32 is
    /// the memory offset of the output f32 array.
    pub fn with_vector_mode(mut self) -> Self {
        self.exec_mode = WasmExecMode::MemoryF32;
        self
    }

    /// Configure the WASM function to return a string via shared memory.
    pub fn with_string_mode(mut self) -> Self {
        self.exec_mode = WasmExecMode::MemoryString;
        self
    }

    pub fn to_scalar_function(&self) -> ScalarFunction {
        let exec_mode = self.exec_mode.to_u8();
        let func_name = self.func_name.clone();
        let wasm = self.wasm_bytes.clone();

        let engine = wasmi::Engine::default();
        let module = match wasmi::Module::new(&engine, &wasm) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("WASM module compilation failed for '{}': {e}", self.name);
                let name = self.name.clone();
                return ScalarFunction::new(
                    name,
                    Arc::new(move |_: &[ArrayRef], _: usize| {
                        Err(crate::LightningError::Internal(
                            format!("WASM module '{func_name}' failed to compile")
                        ))
                    }),
                );
            }
        };

    let exec: crate::processor::functions::ScalarFunctionExec = Arc::new(
        move |args: &[ArrayRef], num_rows: usize| -> Result<ArrayRef> {
            let exec_mode = WasmExecMode::from_u8(exec_mode, args.len());

            match exec_mode {
                    WasmExecMode::ScalarF64 | WasmExecMode::MultiArgF64(_) => {
                        // Multi-arg f64 path: pass all args as f64 params
                        let arg_arrays: Vec<&Float64Array> = args.iter().map(|a| {
                            a.as_any().downcast_ref::<Float64Array>()
                                .ok_or_else(|| crate::LightningError::Internal(
                                    "WASM multi-arg requires Float64 inputs".into()
                                ))
                        }).collect::<Result<Vec<_>>>()?;

                        let mut store = wasmi::Store::new(&engine, ());
                        let instance = wasmi::Instance::new(&mut store, &module, &[])
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM instantiation failed: {e}"
                            )))?;

                        let arity = arg_arrays.len();
                        let results = match arity {
                            1 => {
                                let func = instance.get_typed_func::<(f64,), f64>(&mut store, &func_name)
                                    .map_err(|e| crate::LightningError::Internal(format!(
                                        "WASM function '{func_name}' with 1 arg not found: {e}"
                                    )))?;
                                let mut res = Vec::with_capacity(num_rows);
                                for i in 0..num_rows {
                                    let val = if arg_arrays[0].is_valid(i) {
                                        func.call(&mut store, (arg_arrays[0].value(i),))
                                            .map_err(|e| crate::LightningError::Internal(format!(
                                                "WASM call failed: {e}"
                                            )))?
                                    } else { f64::NAN };
                                    res.push(val);
                                }
                                res
                            }
                            2 => {
                                let func = instance.get_typed_func::<(f64, f64), f64>(&mut store, &func_name)
                                    .map_err(|e| crate::LightningError::Internal(format!(
                                        "WASM function '{func_name}' with 2 args not found: {e}"
                                    )))?;
                                let mut res = Vec::with_capacity(num_rows);
                                for i in 0..num_rows {
                                    let v0 = if arg_arrays[0].is_valid(i) { arg_arrays[0].value(i) } else { f64::NAN };
                                    let v1 = if arg_arrays[1].is_valid(i) { arg_arrays[1].value(i) } else { f64::NAN };
                                    let val = func.call(&mut store, (v0, v1))
                                        .map_err(|e| crate::LightningError::Internal(format!(
                                            "WASM call failed: {e}"
                                        )))?;
                                    res.push(val);
                                }
                                res
                            }
                            3 => {
                                let func = instance.get_typed_func::<(f64, f64, f64), f64>(&mut store, &func_name)
                                    .map_err(|e| crate::LightningError::Internal(format!(
                                        "WASM function '{func_name}' with 3 args not found: {e}"
                                    )))?;
                                let mut res = Vec::with_capacity(num_rows);
                                for i in 0..num_rows {
                                    let v0 = if arg_arrays[0].is_valid(i) { arg_arrays[0].value(i) } else { f64::NAN };
                                    let v1 = if arg_arrays[1].is_valid(i) { arg_arrays[1].value(i) } else { f64::NAN };
                                    let v2 = if arg_arrays[2].is_valid(i) { arg_arrays[2].value(i) } else { f64::NAN };
                                    let val = func.call(&mut store, (v0, v1, v2))
                                        .map_err(|e| crate::LightningError::Internal(format!(
                                            "WASM call failed: {e}"
                                        )))?;
                                    res.push(val);
                                }
                                res
                            }
                            _ => return Err(crate::LightningError::Internal(
                                format!("WASM: unsupported arg count {arity}, max 3")
                            )),
                        };
                        Ok(Arc::new(Float64Array::from(results)))
                    }
                    WasmExecMode::MemoryF32 => {
                        let input = args[0].as_any()
                            .downcast_ref::<Float32Array>()
                            .ok_or_else(|| crate::LightningError::Internal(
                                "WASM vector mode requires Float32 input".into()
                            ))?;

                        let mut store = wasmi::Store::new(&engine, ());
                        let instance = wasmi::Instance::new(&mut store, &module, &[])
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM instantiation failed: {e}"
                            )))?;

                        let func = instance.get_typed_func::<(i32, i32), i32>(&mut store, &func_name)
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM vector function '{func_name}' not found: {e}"
                            )))?;

                        let mem = instance.get_memory(&mut store, "memory")
                            .ok_or_else(|| crate::LightningError::Internal(
                                "WASM vector function requires exported 'memory'".into()
                            ))?;

                        let input_bytes: Vec<u8> = (0..num_rows)
                            .flat_map(|i| input.value(i).to_le_bytes().into_iter())
                            .collect();
                        let write_offset = 0i32;
                        let num_elements = num_rows as i32;
                        {
                            let mem_data = mem.data_mut(&mut store);
                            let write_len = input_bytes.len().min(mem_data.len());
                            mem_data[..write_len].copy_from_slice(&input_bytes[..write_len]);
                        }

                        let output_offset = func.call(&mut store, (write_offset, num_elements))
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM vector call failed: {e}"
                            )))?;

                        let mut results = vec![f32::NAN; num_rows];
                        {
                            let mem_data = mem.data(&store);
                            let read_offset = output_offset as usize;
                            let read_end = (read_offset + num_rows * 4).min(mem_data.len());
                            let byte_count = read_end - read_offset;
                            let valid_entries = byte_count / 4;
                            for j in 0..valid_entries.min(num_rows) {
                                let mut bytes = [0u8; 4];
                                let start = read_offset + j * 4;
                                if start + 4 <= mem_data.len() {
                                    bytes.copy_from_slice(&mem_data[start..start + 4]);
                                    results[j] = f32::from_le_bytes(bytes);
                                }
                            }
                        }

                        Ok(Arc::new(Float32Array::from(results)))
                    }
                    WasmExecMode::MemoryString => {
                        let mut store = wasmi::Store::new(&engine, ());
                        let instance = wasmi::Instance::new(&mut store, &module, &[])
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM instantiation failed: {e}"
                            )))?;

                        let func = instance.get_typed_func::<(i32, i32), i32>(&mut store, &func_name)
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM string function '{func_name}' not found: {e}"
                            )))?;

                        let mem = instance.get_memory(&mut store, "memory")
                            .ok_or_else(|| crate::LightningError::Internal(
                                "WASM string function requires exported 'memory'".into()
                            ))?;

                        let mut results = Vec::with_capacity(num_rows);
                        for i in 0..num_rows {
                            let input_str = format!("{}", Value::from_arrow(&args[0], i));
                            let input_bytes = input_str.as_bytes();
                            let write_offset = 0i32;
                            {
                                let mem_data = mem.data_mut(&mut store);
                                let write_len = input_bytes.len().min(mem_data.len());
                                mem_data[..write_len].copy_from_slice(&input_bytes[..write_len]);
                            }

                            let output_offset = func.call(&mut store, (write_offset, input_bytes.len() as i32))
                                .map_err(|e| crate::LightningError::Internal(format!(
                                    "WASM string call failed: {e}"
                                )))?;

                            let output = {
                                let mem_data = mem.data(&store);
                                let start = output_offset as usize;
                                // Read until null byte or end of memory
                                let end = mem_data[start..].iter()
                                    .position(|&b| b == 0)
                                    .map(|p| start + p)
                                    .unwrap_or(start);
                                String::from_utf8_lossy(&mem_data[start..end]).to_string()
                            };
                            results.push(output);
                        }

                        Ok(Arc::new(StringArray::from(results)))
                    }
                }
            },
        );

        ScalarFunction::new(self.name.clone(), exec)
    }
}

use crate::processor::Value;

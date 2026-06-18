use crate::processor::functions::ScalarFunction;
use crate::Result;
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, StringArray};
use std::sync::Arc;

/// Default maximum execution time for a single WASM function call.
const DEFAULT_WASM_TIMEOUT_MS: u64 = 100;

/// Fuel units per millisecond of timeout. Each wasm instruction consumes 1 fuel unit.
const WASM_FUEL_PER_MS: u64 = 100_000;

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

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::MultiArgF64(0), // arity set at call time via check_arity
            2 => Self::MemoryF32,
            3 => Self::MemoryString,
            _ => Self::ScalarF64,
        }
    }
}

impl WasmExecMode {
    fn check_arity(&self, actual: usize) -> Result<()> {
        if let Self::MultiArgF64(expected) = self {
            if *expected != 0 && *expected != actual {
                return Err(crate::LightningError::Internal(format!(
                    "WASM function expected {} arguments, got {}",
                    expected, actual
                )));
            }
        }
        Ok(())
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

    /// Load a WASM function from pre-validated WAT source bytes.
    /// This avoids the TOCTOU race between path validation and file reading.
    /// Use this when the caller has already validated the file path and
    /// read the content; the bytes are passed directly without re-opening.
    pub fn from_wat_bytes(wat_bytes: Vec<u8>, func_name: &str) -> Result<Self> {
        let wat_source = String::from_utf8(wat_bytes)
            .map_err(|e| crate::LightningError::Database(format!(
                "Failed to decode WAT bytes as UTF-8: {e}"
            )))?;
        Self::from_wat(&wat_source, func_name)
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
        let timeout_ms = self.timeout_ms;

        let mut config = wasmi::Config::default();
        config.consume_fuel(true);
        let engine = wasmi::Engine::new(&config);
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

        // Validate WASM memory declarations to prevent unbounded allocation.
        const MAX_WASM_MEMORY_PAGES: u64 = 4096; // 256MB (64KB per page)
        for export in module.exports() {
            if export.name() == "memory" {
                if let wasmi::ExternType::Memory(mem_type) = export.ty() {
                    let min_pages = mem_type.minimum();
                    let max_pages = mem_type.maximum().unwrap_or(min_pages);
                    if max_pages > MAX_WASM_MEMORY_PAGES {
                        tracing::error!(
                            "WASM module '{}' requests {} pages ({}MB), exceeding limit of {} pages ({}MB)",
                            self.name, max_pages, max_pages * 64 / 1024,
                            MAX_WASM_MEMORY_PAGES, MAX_WASM_MEMORY_PAGES * 64 / 1024
                        );
                        let name = self.name.clone();
                        return ScalarFunction::new(
                            name,
                            Arc::new(move |_: &[ArrayRef], _: usize| {
                                Err(crate::LightningError::Internal(
                                    format!("WASM module '{func_name}' exceeds memory limit")
                                ))
                            }),
                        );
                    }
                }
            }
        }

    let exec: crate::processor::functions::ScalarFunctionExec = Arc::new(
        move |args: &[ArrayRef], num_rows: usize| -> Result<ArrayRef> {
            let exec_mode = WasmExecMode::from_u8(exec_mode);
            exec_mode.check_arity(args.len())?;

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
                        store.set_fuel(timeout_ms * WASM_FUEL_PER_MS)
                            .map_err(|e| crate::LightningError::Internal(format!(
                                "WASM fuel metering failed: {e}"
                            )))?;
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
                                            .map_err(|e| {
                                                let msg = if matches!(e.kind(), wasmi::errors::ErrorKind::Fuel(wasmi::errors::FuelError::OutOfFuel { .. })) {
                                                    format!("WASM function '{}' timed out (fuel exhausted)", func_name)
                                                } else {
                                                    format!("WASM call failed: {e}")
                                                };
                                                crate::LightningError::Internal(msg)
                                            })?
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
                                        .map_err(|e| {
                                            let msg = if matches!(e.kind(), wasmi::errors::ErrorKind::Fuel(wasmi::errors::FuelError::OutOfFuel { .. })) {
                                                format!("WASM function '{}' timed out (fuel exhausted)", func_name)
                                            } else {
                                                format!("WASM call failed: {e}")
                                            };
                                            crate::LightningError::Internal(msg)
                                        })?;
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
                                        .map_err(|e| {
                                            let msg = if matches!(e.kind(), wasmi::errors::ErrorKind::Fuel(wasmi::errors::FuelError::OutOfFuel { .. })) {
                                                format!("WASM function '{}' timed out (fuel exhausted)", func_name)
                                            } else {
                                                format!("WASM call failed: {e}")
                                            };
                                            crate::LightningError::Internal(msg)
                                        })?;
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

                        let results = self::_exec_memory_f32(
                            &engine, &module, input, num_rows, timeout_ms, &func_name,
                        )?;
                        Ok(Arc::new(Float32Array::from(results)))
                    }
                    WasmExecMode::MemoryString => {
                        let results = _exec_memory_string(
                            &engine, &module, &args[0], num_rows, timeout_ms, &func_name,
                        )?;
                        Ok(Arc::new(StringArray::from(results)))
                    }
                }
            },
        );

        ScalarFunction::new(self.name.clone(), exec)
    }
}

/// Execute a MemoryF32 WASM function with a cached store+instance.
/// #63: Uses thread-local storage to avoid per-batch wasmi::Instance::new.
fn _exec_memory_f32(
    engine: &wasmi::Engine,
    module: &wasmi::Module,
    input: &Float32Array,
    num_rows: usize,
    timeout_ms: u64,
    func_name: &str,
) -> Result<Vec<f32>> {
    use std::cell::RefCell;
    std::thread_local! {
        static MEM_F32_CACHE: RefCell<Option<(wasmi::Store<()>, wasmi::Instance)>>
            = const { RefCell::new(None) };
    }
    MEM_F32_CACHE.with(|cell| -> Result<Vec<f32>> {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let mut s = wasmi::Store::new(engine, ());
            let inst = wasmi::Instance::new(&mut s, module, &[])
                .unwrap_or_else(|e| panic!("WASM MemoryF32 instantiation failed: {e}"));
            *borrow = Some((s, inst));
        }
        let (store, instance) = borrow.as_mut()
            .expect("WASM MemoryF32 cache: just initialized if was None");

        store.set_fuel(timeout_ms * WASM_FUEL_PER_MS)
            .map_err(|e| LightningError::Internal(format!("WASM fuel metering failed: {e}")))?;

        let func = instance.get_typed_func::<(i32, i32), i32>(&mut *store, func_name)
            .map_err(|e| LightningError::Internal(format!("WASM vector function '{func_name}' not found: {e}")))?;

        let mem = instance.get_memory(&mut *store, "memory")
            .ok_or_else(|| LightningError::Internal(
                "WASM vector function requires exported 'memory'".into()
            ))?;

        let mem_size = mem.data(&*store).len();
        let input_byte_len = num_rows * 4;
        if input_byte_len > mem_size {
            return Err(LightningError::Internal(format!(
                "WASM: input size {input_byte_len} exceeds WASM memory size {mem_size}"
            )));
        }
        let input_bytes: Vec<u8> = (0..num_rows)
            .flat_map(|i| input.value(i).to_le_bytes())
            .collect();
        let write_offset = 0i32;
        let num_elements = num_rows as i32;
        mem.data_mut(&mut *store)[..input_byte_len].copy_from_slice(&input_bytes);

        let output_offset = func.call(&mut *store, (write_offset, num_elements))
            .map_err(|e| {
                let msg = if matches!(e.kind(), wasmi::errors::ErrorKind::Fuel(wasmi::errors::FuelError::OutOfFuel { .. })) {
                    format!("WASM function '{func_name}' timed out (fuel exhausted)")
                } else {
                    format!("WASM vector call failed: {e}")
                };
                LightningError::Internal(msg)
            })?;

        let mut results = vec![f32::NAN; num_rows];
        let read_offset = output_offset as usize;
        let output_byte_len = num_rows * 4;
        if read_offset < mem_size && read_offset.saturating_add(output_byte_len) <= mem_size {
            let mem_data = mem.data(&*store);
            for j in 0..num_rows {
                let start = read_offset + j * 4;
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&mem_data[start..start + 4]);
                results[j] = f32::from_le_bytes(bytes);
            }
        }

        // Zero out memory to prevent data leakage
        mem.data_mut(&mut *store).fill(0);

        Ok(results)
    })
}

/// Execute a MemoryString WASM function with a cached store+instance.
/// #63: Uses thread-local storage to avoid per-batch wasmi::Instance::new.
fn _exec_memory_string(
    engine: &wasmi::Engine,
    module: &wasmi::Module,
    input_arr: &ArrayRef,
    num_rows: usize,
    timeout_ms: u64,
    func_name: &str,
) -> Result<Vec<String>> {
    use std::cell::RefCell;
    std::thread_local! {
        static MEM_STRING_CACHE: RefCell<Option<(wasmi::Store<()>, wasmi::Instance)>>
            = const { RefCell::new(None) };
    }
    MEM_STRING_CACHE.with(|cell| -> Result<Vec<String>> {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let mut s = wasmi::Store::new(engine, ());
            let inst = wasmi::Instance::new(&mut s, module, &[])
                .unwrap_or_else(|e| panic!("WASM MemoryString instantiation failed: {e}"));
            *borrow = Some((s, inst));
        }
        let (store, instance) = borrow.as_mut()
            .expect("WASM MemoryString cache: just initialized if was None");

        store.set_fuel(timeout_ms * WASM_FUEL_PER_MS)
            .map_err(|e| LightningError::Internal(format!("WASM fuel metering failed: {e}")))?;

        let func = instance.get_typed_func::<(i32, i32), i32>(&mut *store, func_name)
            .map_err(|e| LightningError::Internal(format!("WASM string function '{func_name}' not found: {e}")))?;

        let mem = instance.get_memory(&mut *store, "memory")
            .ok_or_else(|| LightningError::Internal(
                "WASM string function requires exported 'memory'".into()
            ))?;

        let mem_size = mem.data(&*store).len();
        let mut results = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let input_str = format!("{}", Value::from_arrow(input_arr, i));
            let input_bytes = input_str.as_bytes();
            let input_len = input_bytes.len();
            if input_len > mem_size {
                results.push(input_str);
                continue;
            }
            let write_offset = 0i32;
            {
                let mem_data = mem.data_mut(&mut *store);
                // Zero the entire memory before each invocation
                // to prevent data leakage from previous rows
                mem_data.fill(0);
                mem_data[..input_len].copy_from_slice(input_bytes);
            }

            let output_offset = func.call(&mut *store, (write_offset, input_len as i32))
                .map_err(|e| {
                    let msg = if matches!(e.kind(), wasmi::errors::ErrorKind::Fuel(wasmi::errors::FuelError::OutOfFuel { .. })) {
                        format!("WASM function '{func_name}' timed out (fuel exhausted)")
                    } else {
                        format!("WASM string call failed: {e}")
                    };
                    LightningError::Internal(msg)
                })?;

            let output = {
                let mem_data = mem.data(&*store);
                let start = output_offset as usize;
                if start < mem_size {
                    let end = mem_data[start..].iter()
                        .position(|&b| b == 0)
                        .map(|p| start + p)
                        .unwrap_or(mem_size);
                    let clamped_end = end.min(mem_size);
                    if clamped_end > start {
                        String::from_utf8_lossy(&mem_data[start..clamped_end]).to_string()
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                }
            };
            results.push(output);
        }

        // Zero out memory to prevent data leakage between batches
        mem.data_mut(&mut *store).fill(0);

        Ok(results)
    })
}

use crate::processor::Value;
use crate::LightningError;

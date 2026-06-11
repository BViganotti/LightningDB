use crate::processor::Value;
use crate::Result;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use super::aggregate_function::AggregateFunction;

pub struct StdDevPop {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Default for StdDevPop {
    fn default() -> Self {
        Self::new()
    }
}

impl StdDevPop {
    pub fn new() -> Self {
        Self { count: 0, mean: 0.0, m2: 0.0 }
    }
}

impl AggregateFunction for StdDevPop {
    fn name(&self) -> &str { "STDDEV_POP" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let arr = &values[0];
        if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for &idx in row_indices { if !f.is_null(idx) { let x = f.value(idx); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for &idx in row_indices { if !int_arr.is_null(idx) { let x = int_arr.value(idx) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(f) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..values.len() { if !f.is_null(i) { let x = f.value(i); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..values.len() { if !int_arr.is_null(i) { let x = int_arr.value(i) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<StdDevPop>() {
            let cc = self.count + o.count; if cc == 0 { return Ok(()); }
            let d = o.mean - self.mean;
            self.mean = (self.count as f64 * self.mean + o.count as f64 * o.mean) / cc as f64;
            self.m2 = self.m2 + o.m2 + d * d * self.count as f64 * o.count as f64 / cc as f64;
            self.count = cc;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.count < 2 { return Ok(Value::Null); }
        Ok(Value::Number((self.m2 / self.count as f64).sqrt()))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { count: self.count, mean: self.mean, m2: self.m2 }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct StdDevSamp {
    count: u64, mean: f64, m2: f64,
}

impl Default for StdDevSamp {
    fn default() -> Self {
        Self::new()
    }
}

impl StdDevSamp {
    pub fn new() -> Self { Self { count: 0, mean: 0.0, m2: 0.0 } }
}

impl AggregateFunction for StdDevSamp {
    fn name(&self) -> &str { "STDDEV_SAMP" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let arr = &values[0];
        if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for &idx in row_indices { if !f.is_null(idx) { let x = f.value(idx); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for &idx in row_indices { if !int_arr.is_null(idx) { let x = int_arr.value(idx) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(f) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..values.len() { if !f.is_null(i) { let x = f.value(i); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..values.len() { if !int_arr.is_null(i) { let x = int_arr.value(i) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<StdDevSamp>() {
            let cc = self.count + o.count; if cc == 0 { return Ok(()); }
            let d = o.mean - self.mean;
            self.mean = (self.count as f64 * self.mean + o.count as f64 * o.mean) / cc as f64;
            self.m2 = self.m2 + o.m2 + d * d * self.count as f64 * o.count as f64 / cc as f64;
            self.count = cc;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.count < 2 { return Ok(Value::Null); }
        Ok(Value::Number((self.m2 / (self.count - 1) as f64).sqrt()))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { count: self.count, mean: self.mean, m2: self.m2 }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct VarPop {
    count: u64, mean: f64, m2: f64,
}

impl Default for VarPop {
    fn default() -> Self {
        Self::new()
    }
}

impl VarPop {
    pub fn new() -> Self { Self { count: 0, mean: 0.0, m2: 0.0 } }
}

impl AggregateFunction for VarPop {
    fn name(&self) -> &str { "VAR_POP" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let arr = &values[0];
        if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for &idx in row_indices { if !f.is_null(idx) { let x = f.value(idx); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for &idx in row_indices { if !int_arr.is_null(idx) { let x = int_arr.value(idx) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(f) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..values.len() { if !f.is_null(i) { let x = f.value(i); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..values.len() { if !int_arr.is_null(i) { let x = int_arr.value(i) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<VarPop>() {
            let cc = self.count + o.count; if cc == 0 { return Ok(()); }
            let d = o.mean - self.mean;
            self.mean = (self.count as f64 * self.mean + o.count as f64 * o.mean) / cc as f64;
            self.m2 = self.m2 + o.m2 + d * d * self.count as f64 * o.count as f64 / cc as f64;
            self.count = cc;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.count < 2 { return Ok(Value::Null); }
        Ok(Value::Number(self.m2 / self.count as f64))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { count: self.count, mean: self.mean, m2: self.m2 }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct VarSamp {
    count: u64, mean: f64, m2: f64,
}

impl Default for VarSamp {
    fn default() -> Self {
        Self::new()
    }
}

impl VarSamp {
    pub fn new() -> Self { Self { count: 0, mean: 0.0, m2: 0.0 } }
}

impl AggregateFunction for VarSamp {
    fn name(&self) -> &str { "VAR_SAMP" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let arr = &values[0];
        if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for &idx in row_indices { if !f.is_null(idx) { let x = f.value(idx); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for &idx in row_indices { if !int_arr.is_null(idx) { let x = int_arr.value(idx) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(f) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..values.len() { if !f.is_null(i) { let x = f.value(i); self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..values.len() { if !int_arr.is_null(i) { let x = int_arr.value(i) as f64; self.count += 1; let d = x - self.mean; self.mean += d / self.count as f64; self.m2 += d * (x - self.mean); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<VarSamp>() {
            let cc = self.count + o.count; if cc == 0 { return Ok(()); }
            let d = o.mean - self.mean;
            self.mean = (self.count as f64 * self.mean + o.count as f64 * o.mean) / cc as f64;
            self.m2 = self.m2 + o.m2 + d * d * self.count as f64 * o.count as f64 / cc as f64;
            self.count = cc;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.count < 2 { return Ok(Value::Null); }
        Ok(Value::Number(self.m2 / (self.count - 1) as f64))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { count: self.count, mean: self.mean, m2: self.m2 }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct GroupConcat {
    values: Vec<String>,
}

impl Default for GroupConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupConcat {
    pub fn new() -> Self { Self { values: Vec::new() } }
}

impl AggregateFunction for GroupConcat {
    fn name(&self) -> &str { "GROUP_CONCAT" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        if let Some(s) = values[0].as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in row_indices { if !s.is_null(*i) { self.values.push(s.value(*i).to_string()); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(s) = values.as_any().downcast_ref::<arrow::array::StringArray>() {
            for i in 0..values.len() { if !s.is_null(i) { self.values.push(s.value(i).to_string()); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<GroupConcat>() { self.values.extend(o.values.clone()); }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> { Ok(Value::String(self.values.join(", "))) }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { values: self.values.clone() }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct Median {
    values: Vec<f64>,
}

impl Default for Median {
    fn default() -> Self {
        Self::new()
    }
}

impl Median {
    pub fn new() -> Self { Self { values: Vec::new() } }
}

impl AggregateFunction for Median {
    fn name(&self) -> &str { "MEDIAN" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let arr = &values[0];
        if let Some(f) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for &idx in row_indices { if !f.is_null(idx) { self.values.push(f.value(idx)); } }
        } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for &idx in row_indices { if !int_arr.is_null(idx) { self.values.push(int_arr.value(idx) as f64); } }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(f) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in 0..values.len() { if !f.is_null(i) { self.values.push(f.value(i)); } }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in 0..values.len() { if !int_arr.is_null(i) { self.values.push(int_arr.value(i) as f64); } }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<Median>() { self.values.extend(&o.values); }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() { return Ok(Value::Null); }
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        if sorted.len() % 2 == 0 { Ok(Value::Number((sorted[mid - 1] + sorted[mid]) / 2.0)) }
        else { Ok(Value::Number(sorted[mid])) }
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { values: self.values.clone() }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

pub struct CollectDistinct {
    values: HashSet<String>,
}

impl Default for CollectDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectDistinct {
    pub fn new() -> Self { Self { values: HashSet::new() } }
}

impl AggregateFunction for CollectDistinct {
    fn name(&self) -> &str { "COLLECT_DISTINCT" }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        for i in row_indices { if !values[0].is_null(*i) { self.values.insert(Value::from_arrow(&values[0], *i).to_string()); } }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        for i in 0..values.len() { if !values.is_null(i) { self.values.insert(Value::from_arrow(values, i).to_string()); } }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<CollectDistinct>() { self.values.extend(o.values.clone()); }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        let list: Vec<Value> = self.values.iter().map(|s| Value::String(s.clone())).collect();
        Ok(Value::List(list))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> { Box::new(Self { values: self.values.clone() }) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}

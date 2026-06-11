use crate::processor::Value;
use crate::Result;
use arrow::array::{Array, ArrayRef};

pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()>;
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()>;
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()>;
    fn finalize(&self) -> Result<Value>;
    fn clone_box(&self) -> Box<dyn AggregateFunction>;
    fn as_any(&self) -> &dyn std::any::Any;
}

pub struct Count {
    count: u64,
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl AggregateFunction for Count {
    fn name(&self) -> &str {
        "COUNT"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        for i in row_indices {
            if !values[0].is_null(*i) {
                self.count += 1;
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        self.count += (values.len() - values.null_count()) as u64;
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_count) = other.as_any().downcast_ref::<Count>() {
            self.count += other_count.count;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(Value::Number(self.count as f64))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self { count: self.count })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct CountStar {
    count: u64,
}

impl Default for CountStar {
    fn default() -> Self {
        Self::new()
    }
}

impl CountStar {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl AggregateFunction for CountStar {
    fn name(&self) -> &str {
        "COUNT_STAR"
    }
    fn update(&mut self, _values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        self.count += row_indices.len() as u64;
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        self.count += values.len() as u64;
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_count) = other.as_any().downcast_ref::<CountStar>() {
            self.count += other_count.count;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(Value::Number(self.count as f64))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self { count: self.count })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

use std::collections::HashSet;

pub struct CountDistinct {
    values: HashSet<String>,
}

impl Default for CountDistinct {
    fn default() -> Self {
        Self::new()
    }
}

impl CountDistinct {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }
}

impl AggregateFunction for CountDistinct {
    fn name(&self) -> &str {
        "COUNT_DISTINCT"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        for &i in row_indices {
            if !values[0].is_null(i) {
                let val = Value::from_arrow(&values[0], i);
                self.values.insert(format!("{val:?}"));
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        for i in 0..values.len() {
            if !values.is_null(i) {
                let val = Value::from_arrow(values, i);
                self.values.insert(format!("{val:?}"));
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_distinct) = other.as_any().downcast_ref::<CountDistinct>() {
            self.values.extend(other_distinct.values.clone());
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(Value::Number(self.values.len() as f64))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self {
            values: self.values.clone(),
        })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct Sum {
    sum: f64,
    int_sum: i128,
    is_integer: bool,
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl Sum {
    pub fn new() -> Self {
        Self { sum: 0.0, int_sum: 0, is_integer: false }
    }
}

impl AggregateFunction for Sum {
    fn name(&self) -> &str {
        "SUM"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        if let Some(arr_float) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            self.is_integer = false;
            for i in row_indices {
                if !arr_float.is_null(*i) {
                    self.sum += arr_float.value(*i);
                }
            }
        } else if let Some(arr_int) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            self.is_integer = true;
            for i in row_indices {
                if !arr_int.is_null(*i) {
                    self.int_sum += arr_int.value(*i) as i128;
                }
            }
        } else if let Some(arr_int32) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
            self.is_integer = true;
            for i in row_indices {
                if !arr_int32.is_null(*i) {
                    self.int_sum += arr_int32.value(*i) as i128;
                }
            }
        } else if let Some(arr_int16) = arr.as_any().downcast_ref::<arrow::array::Int16Array>() {
            self.is_integer = true;
            for i in row_indices {
                if !arr_int16.is_null(*i) {
                    self.int_sum += arr_int16.value(*i) as i128;
                }
            }
        } else if let Some(arr_int8) = arr.as_any().downcast_ref::<arrow::array::Int8Array>() {
            self.is_integer = true;
            for i in row_indices {
                if !arr_int8.is_null(*i) {
                    self.int_sum += arr_int8.value(*i) as i128;
                }
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(arr_float) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            self.is_integer = false;
            self.sum += arrow::compute::kernels::aggregate::sum(arr_float).unwrap_or(0.0);
        } else if let Some(arr) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            self.is_integer = true;
            for i in 0..values.len() {
                if !arr.is_null(i) {
                    self.int_sum += arr.value(i) as i128;
                }
            }
        } else if let Some(arr) = values.as_any().downcast_ref::<arrow::array::Int32Array>() {
            self.is_integer = true;
            for i in 0..values.len() {
                if !arr.is_null(i) {
                    self.int_sum += arr.value(i) as i128;
                }
            }
        } else if let Some(arr) = values.as_any().downcast_ref::<arrow::array::Int16Array>() {
            self.is_integer = true;
            for i in 0..values.len() {
                if !arr.is_null(i) {
                    self.int_sum += arr.value(i) as i128;
                }
            }
        } else if let Some(arr) = values.as_any().downcast_ref::<arrow::array::Int8Array>() {
            self.is_integer = true;
            for i in 0..values.len() {
                if !arr.is_null(i) {
                    self.int_sum += arr.value(i) as i128;
                }
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_sum) = other.as_any().downcast_ref::<Sum>() {
            if other_sum.is_integer {
                self.is_integer = true;
                self.int_sum += other_sum.int_sum;
            } else {
                self.is_integer = false;
                self.sum += other_sum.sum;
            }
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.is_integer {
            Ok(Value::Number(self.int_sum as f64))
        } else {
            Ok(Value::Number(self.sum))
        }
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self { sum: self.sum, int_sum: self.int_sum, is_integer: self.is_integer })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct Avg {
    sum: f64,
    count: u64,
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl Avg {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl AggregateFunction for Avg {
    fn name(&self) -> &str {
        "AVG"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Handle Float64Array
        if let Some(arr_float) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in row_indices {
                if !arr_float.is_null(*i) {
                    self.sum += arr_float.value(*i);
                    self.count += 1;
                }
            }
        }
        // Handle Int64Array - convert to f64
        else if let Some(arr_int) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in row_indices {
                if !arr_int.is_null(*i) {
                    self.sum += arr_int.value(*i) as f64;
                    self.count += 1;
                }
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(arr_float) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            self.sum += arrow::compute::kernels::aggregate::sum(arr_float).unwrap_or(0.0);
            self.count += (arr_float.len() - arr_float.null_count()) as u64;
        } else if let Some(arr_int) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            self.sum += arrow::compute::kernels::aggregate::sum(arr_int).unwrap_or(0) as f64;
            self.count += (arr_int.len() - arr_int.null_count()) as u64;
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_avg) = other.as_any().downcast_ref::<Avg>() {
            self.sum += other_avg.sum;
            self.count += other_avg.count;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Number(self.sum / self.count as f64))
        }
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self {
            sum: self.sum,
            count: self.count,
        })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct Min {
    min: Option<f64>,
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl Min {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl AggregateFunction for Min {
    fn name(&self) -> &str {
        "MIN"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Handle Float64Array
        if let Some(arr_float) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in row_indices {
                if !arr_float.is_null(*i) {
                    let v = arr_float.value(*i);
                    if self.min.map_or(true, |m| v < m) {
                        self.min = Some(v);
                    }
                }
            }
        }
        // Handle Int64Array - convert to f64
        else if let Some(arr_int) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in row_indices {
                if !arr_int.is_null(*i) {
                    let v = arr_int.value(*i) as f64;
                    if self.min.map_or(true, |m| v < m) {
                        self.min = Some(v);
                    }
                }
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(arr_float) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            let v = arrow::compute::kernels::aggregate::min(arr_float);
            if let Some(v_val) = v {
                if self.min.map_or(true, |m| v_val < m) {
                    self.min = Some(v_val);
                }
            }
        } else if let Some(arr_int) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            let v = arrow::compute::kernels::aggregate::min(arr_int);
            if let Some(v_val) = v {
                let v_f64 = v_val as f64;
                if self.min.map_or(true, |m| v_f64 < m) {
                    self.min = Some(v_f64);
                }
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_min) = other.as_any().downcast_ref::<Min>() {
            if let Some(v) = other_min.min {
                    if self.min.map_or(true, |m| v < m) {
                        self.min = Some(v);
                    }
            }
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(self.min.map(Value::Number).unwrap_or(Value::Null))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self { min: self.min })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct Max {
    max: Option<f64>,
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl Max {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl AggregateFunction for Max {
    fn name(&self) -> &str {
        "MAX"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        // Handle Float64Array
        if let Some(arr_float) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
            for i in row_indices {
                if !arr_float.is_null(*i) {
                    let v = arr_float.value(*i);
                    if self.max.map_or(true, |m| v > m) {
                        self.max = Some(v);
                    }
                }
            }
        }
        // Handle Int64Array - convert to f64
        else if let Some(arr_int) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
            for i in row_indices {
                if !arr_int.is_null(*i) {
                    let v = arr_int.value(*i) as f64;
                    if self.max.map_or(true, |m| v > m) {
                        self.max = Some(v);
                    }
                }
            }
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        if let Some(arr_float) = values.as_any().downcast_ref::<arrow::array::Float64Array>() {
            let v = arrow::compute::kernels::aggregate::max(arr_float);
            if let Some(v_val) = v {
                if self.max.map_or(true, |m| v_val > m) {
                    self.max = Some(v_val);
                }
            }
        } else if let Some(arr_int) = values.as_any().downcast_ref::<arrow::array::Int64Array>() {
            let v = arrow::compute::kernels::aggregate::max(arr_int);
            if let Some(v_val) = v {
                let v_f64 = v_val as f64;
                if self.max.map_or(true, |m| v_f64 > m) {
                    self.max = Some(v_f64);
                }
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_max) = other.as_any().downcast_ref::<Max>() {
            if let Some(v) = other_max.max {
                if self.max.map_or(true, |m| v > m) {
                    self.max = Some(v);
                }
            }
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(self.max.map(Value::Number).unwrap_or(Value::Null))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self { max: self.max })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct Collect {
    values: Vec<Value>,
}

impl Default for Collect {
    fn default() -> Self {
        Self::new()
    }
}

impl Collect {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl AggregateFunction for Collect {
    fn name(&self) -> &str {
        "COLLECT"
    }
    fn update(&mut self, values: &[ArrayRef], row_indices: &[usize]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        for i in row_indices {
            self.values.push(Value::from_arrow(&values[0], *i));
        }
        Ok(())
    }
    fn update_vector(&mut self, values: &ArrayRef) -> Result<()> {
        for i in 0..values.len() {
            self.values.push(Value::from_arrow(values, i));
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn AggregateFunction) -> Result<()> {
        if let Some(other_collect) = other.as_any().downcast_ref::<Collect>() {
            self.values.extend(other_collect.values.clone());
        }
        Ok(())
    }
    fn finalize(&self) -> Result<Value> {
        Ok(Value::List(self.values.clone()))
    }
    fn clone_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Self {
            values: self.values.clone(),
        })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

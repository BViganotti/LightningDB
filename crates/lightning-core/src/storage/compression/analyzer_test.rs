use crate::processor::Value;
use crate::storage::compression::analyzer::CompressionAnalyzer;
use crate::storage::compression::CompressionType;
use lightning_types::LogicalType;

#[test]
fn test_analyze_constant() {
    let vals = vec![Value::Number(10.0); 100];
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Constant);
    assert_eq!(meta.min, Value::Number(10.0));
}

#[test]
fn test_analyze_bit_packing() {
    let mut vals = Vec::new();
    for i in 0..100 {
        vals.push(Value::Number(i as f64));
    }
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::FixedFrameOfReference);
    assert_eq!(meta.bit_width, 7);
}

#[test]
fn test_analyze_uncompressed() {
    let mut vals = Vec::new();
    vals.push(Value::Number(i64::MIN as f64));
    vals.push(Value::Number(i64::MAX as f64));
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Uncompressed);
}

#[test]
fn test_analyze_rle() {
    let mut vals = Vec::new();
    for _ in 0..50 {
        vals.push(Value::Number(10.0));
    }
    for _ in 0..50 {
        vals.push(Value::Number(20.0));
    }
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Rle);
}

#[test]
fn test_analyze_dict() {
    let mut vals = Vec::new();
    for i in 0..100 {
        vals.push(Value::Number((i % 5) as f64));
    }
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Dict);
}

#[test]
fn test_analyze_empty_slice() {
    let vals: Vec<Value> = vec![];
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Uncompressed);
    assert_eq!(meta.bit_width, 0);
}

#[test]
fn test_analyze_single_value() {
    let vals = vec![Value::Number(42.0)];
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Constant);
    assert_eq!(meta.min, Value::Number(42.0));
}

#[test]
fn test_analyze_nan() {
    let vals = vec![Value::Number(f64::NAN), Value::Number(f64::NAN)];
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    // NaN should not cause panic; should produce some valid compression type
    assert!(meta.bit_width <= 64);
}

#[test]
fn test_analyze_null() {
    let vals = vec![Value::Null; 10];
    let meta = CompressionAnalyzer::analyze_integer_chunk(&vals, &LogicalType::Int64, None, None);
    assert_eq!(meta.compression, CompressionType::Constant);
    assert_eq!(meta.min, Value::Null);
}

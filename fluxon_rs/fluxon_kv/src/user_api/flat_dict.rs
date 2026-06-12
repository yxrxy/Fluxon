use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum FlatValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

pub type FlatDict = BTreeMap<String, FlatValue>;

use crate::memholder::kvclient_encode::{FlatKvValueRange, flat_kv_decode_ranges};
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use crate::user_api::flat_dict::{FlatDict, FlatValue};

const FLAT_KV_TYPE_INT64: u8 = 1;
const FLAT_KV_TYPE_FLOAT64: u8 = 3;
const FLAT_KV_TYPE_STRING: u8 = 4;
const FLAT_KV_TYPE_BYTES: u8 = 5;
const FLAT_KV_TYPE_BOOL: u8 = 7;

fn invalid_arg(detail: impl Into<String>) -> KvError {
    KvError::Api(ApiError::InvalidArgument {
        detail: detail.into(),
    })
}

pub fn decode_flat_dict_bytes(data: &[u8]) -> KvResult<FlatDict> {
    let items = flat_kv_decode_ranges(data)
        .map_err(|e| invalid_arg(format!("flat dict decode failed: {}", e)))?;
    let mut out: FlatDict = FlatDict::new();
    for (k, v) in items {
        let vv = match v {
            FlatKvValueRange::Bool(b) => FlatValue::Bool(b),
            FlatKvValueRange::Int64(i) => FlatValue::Int64(i),
            FlatKvValueRange::Float64(f) => FlatValue::Float64(f),
            FlatKvValueRange::String(s) => FlatValue::String(s),
            FlatKvValueRange::BytesRange { start, len } => {
                if start + len > data.len() {
                    return Err(invalid_arg("flat dict bytes range out of bounds"));
                }
                FlatValue::Bytes(data[start..(start + len)].to_vec())
            }
        };
        out.insert(k, vv);
    }
    Ok(out)
}

pub fn encode_flat_dict_bytes(value: &FlatDict) -> KvResult<Vec<u8>> {
    if value.len() > (u32::MAX as usize) {
        return Err(invalid_arg("flat dict too large"));
    }

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    for (k, v) in value.iter() {
        let kb = k.as_bytes();
        if kb.len() > (u32::MAX as usize) {
            return Err(invalid_arg("flat dict key too large"));
        }
        out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
        out.extend_from_slice(kb);
        match v {
            FlatValue::Bool(b) => {
                out.push(FLAT_KV_TYPE_BOOL);
                out.extend_from_slice(&1u32.to_le_bytes());
                out.push(if *b { 1 } else { 0 });
            }
            FlatValue::Int64(i) => {
                out.push(FLAT_KV_TYPE_INT64);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&i.to_le_bytes());
            }
            FlatValue::Float64(f) => {
                out.push(FLAT_KV_TYPE_FLOAT64);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&f.to_le_bytes());
            }
            FlatValue::String(s) => {
                let vb = s.as_bytes();
                if vb.len() > (u32::MAX as usize) {
                    return Err(invalid_arg("flat dict string too large"));
                }
                out.push(FLAT_KV_TYPE_STRING);
                out.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                out.extend_from_slice(vb);
            }
            FlatValue::Bytes(b) => {
                if b.len() > (u32::MAX as usize) {
                    return Err(invalid_arg("flat dict bytes too large"));
                }
                out.push(FLAT_KV_TYPE_BYTES);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
    }

    Ok(out)
}

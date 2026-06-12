use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};

pub const FLAT_KV_TYPE_INT64: u8 = 1;
pub const FLAT_KV_TYPE_FLOAT64: u8 = 3;
pub const FLAT_KV_TYPE_STRING: u8 = 4;
pub const FLAT_KV_TYPE_BYTES: u8 = 5;
pub const FLAT_KV_TYPE_BOOL: u8 = 7;

pub enum OwnedFlatKvValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(Vec<u8>),
    Bytes(Vec<u8>),
}

pub struct OwnedFlatKvEntry {
    pub key_utf8: Vec<u8>,
    pub value: OwnedFlatKvValue,
}

pub enum FlatKvValueRange {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    BytesRange { start: usize, len: usize },
}

pub enum BorrowedFlatKvValueRange<'a> {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(&'a str),
    BytesRange { start: usize, len: usize },
}

pub struct BorrowedFlatKvEntry<'a> {
    pub key: &'a str,
    pub value: BorrowedFlatKvValueRange<'a>,
}

pub fn flat_kv_decode_borrowed<'a>(data: &'a [u8]) -> Result<Vec<BorrowedFlatKvEntry<'a>>, String> {
    fn need(data: &[u8], pos: usize, n: usize, what: &str) -> Result<(), String> {
        if pos + n > data.len() {
            return Err(format!("truncated {}", what));
        }
        Ok(())
    }

    fn read_u32(data: &[u8], pos: &mut usize, what: &str) -> Result<u32, String> {
        need(data, *pos, 4, what)?;
        let b: [u8; 4] = data[*pos..(*pos + 4)]
            .try_into()
            .expect("slice length checked");
        *pos += 4;
        Ok(u32::from_le_bytes(b))
    }

    fn read_u8(data: &[u8], pos: &mut usize, what: &str) -> Result<u8, String> {
        need(data, *pos, 1, what)?;
        let v = data[*pos];
        *pos += 1;
        Ok(v)
    }

    fn read_bytes<'a>(
        data: &'a [u8],
        pos: &mut usize,
        n: usize,
        what: &str,
    ) -> Result<&'a [u8], String> {
        need(data, *pos, n, what)?;
        let s = &data[*pos..(*pos + n)];
        *pos += n;
        Ok(s)
    }

    let mut pos: usize = 0;
    let count = read_u32(data, &mut pos, "entry count header")? as usize;
    let mut out: Vec<BorrowedFlatKvEntry<'a>> = Vec::with_capacity(count);

    for _ in 0..count {
        let key_len = read_u32(data, &mut pos, "key length")? as usize;
        let key_bytes = read_bytes(data, &mut pos, key_len, "key bytes")?;
        let key =
            std::str::from_utf8(key_bytes).map_err(|e| format!("invalid UTF-8 key: {}", e))?;

        let type_id = read_u8(data, &mut pos, "value type id")?;
        let val_len = read_u32(data, &mut pos, "value length")? as usize;
        let val_start = pos;
        let val_bytes = read_bytes(data, &mut pos, val_len, "value bytes")?;

        let value = match type_id {
            FLAT_KV_TYPE_BOOL => {
                if val_len != 1 {
                    return Err(format!("bool length must be 1 (key={:?})", key));
                }
                BorrowedFlatKvValueRange::Bool(val_bytes[0] != 0)
            }
            FLAT_KV_TYPE_INT64 => {
                if val_len != 8 {
                    return Err(format!("int64 length must be 8 (key={:?})", key));
                }
                let b: [u8; 8] = val_bytes.try_into().expect("length checked");
                BorrowedFlatKvValueRange::Int64(i64::from_le_bytes(b))
            }
            FLAT_KV_TYPE_FLOAT64 => {
                if val_len != 8 {
                    return Err(format!("float64 length must be 8 (key={:?})", key));
                }
                let b: [u8; 8] = val_bytes.try_into().expect("length checked");
                BorrowedFlatKvValueRange::Float64(f64::from_le_bytes(b))
            }
            FLAT_KV_TYPE_STRING => {
                let s = std::str::from_utf8(val_bytes)
                    .map_err(|e| format!("invalid UTF-8 string value for key {:?}: {}", key, e))?;
                BorrowedFlatKvValueRange::String(s)
            }
            FLAT_KV_TYPE_BYTES => BorrowedFlatKvValueRange::BytesRange {
                start: val_start,
                len: val_len,
            },
            _ => return Err(format!("unknown type id {} for key {:?}", type_id, key)),
        };
        out.push(BorrowedFlatKvEntry { key, value });
    }

    if pos != data.len() {
        return Err("trailing bytes present".to_string());
    }

    Ok(out)
}

pub fn flat_kv_decode_ranges(data: &[u8]) -> Result<Vec<(String, FlatKvValueRange)>, String> {
    let items = flat_kv_decode_borrowed(data)?;
    let mut out: Vec<(String, FlatKvValueRange)> = Vec::with_capacity(items.len());
    for item in items {
        let value = match item.value {
            BorrowedFlatKvValueRange::Bool(value) => FlatKvValueRange::Bool(value),
            BorrowedFlatKvValueRange::Int64(value) => FlatKvValueRange::Int64(value),
            BorrowedFlatKvValueRange::Float64(value) => FlatKvValueRange::Float64(value),
            BorrowedFlatKvValueRange::String(value) => FlatKvValueRange::String(value.to_string()),
            BorrowedFlatKvValueRange::BytesRange { start, len } => {
                FlatKvValueRange::BytesRange { start, len }
            }
        };
        out.push((item.key.to_string(), value));
    }
    Ok(out)
}

pub fn calc_flat_dict_encoded_len(
    ptrs: &[(u8, usize, u32, u64, u32, Option<u32>)],
) -> KvResult<u64> {
    if ptrs.len() > (u32::MAX as usize) {
        return Err(KvError::Api(ApiError::Unknown {
            detail: "flat dict too large".to_string(),
        }));
    }

    let mut total: u64 = 4;
    for (type_id, _key_ptr, key_len_u32, val_u64, val_len_u32, _) in ptrs.iter().copied() {
        let key_len = key_len_u32 as usize;
        let val_len = val_len_u32 as usize;
        if val_len > (u32::MAX as usize) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: "flat dict value too large".to_string(),
            }));
        }

        match type_id {
            FLAT_KV_TYPE_BOOL => {
                if val_len != 1 {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: "flat dict bool length must be 1".to_string(),
                    }));
                }
            }
            FLAT_KV_TYPE_INT64 | FLAT_KV_TYPE_FLOAT64 => {
                if val_len != 8 {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: "flat dict scalar length must be 8".to_string(),
                    }));
                }
            }
            FLAT_KV_TYPE_STRING | FLAT_KV_TYPE_BYTES => {
                if usize::try_from(val_u64).is_err() {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: "flat dict value pointer out of range".to_string(),
                    }));
                }
            }
            _ => {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!("flat dict unknown type id {}", type_id),
                }));
            }
        }

        total = total
            .checked_add(4 + (key_len as u64) + 1 + 4 + (val_len as u64))
            .ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "flat dict encoded payload length overflow".to_string(),
                })
            })?;

        if total > (u32::MAX as u64) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: "flat dict encoded payload too large".to_string(),
            }));
        }
    }

    Ok(total)
}

pub fn flat_kv_encode_owned(entries: &[OwnedFlatKvEntry]) -> KvResult<Vec<u8>> {
    let mut ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)> = Vec::with_capacity(entries.len());
    for entry in entries {
        let key_len = u32::try_from(entry.key_utf8.len()).map_err(|_| {
            KvError::Api(ApiError::Unknown {
                detail: "flat dict key too large".to_string(),
            })
        })?;
        match &entry.value {
            OwnedFlatKvValue::Bool(value) => {
                ptrs.push((
                    FLAT_KV_TYPE_BOOL,
                    entry.key_utf8.as_ptr() as usize,
                    key_len,
                    if *value { 1 } else { 0 },
                    1,
                    None,
                ));
            }
            OwnedFlatKvValue::Int64(value) => {
                ptrs.push((
                    FLAT_KV_TYPE_INT64,
                    entry.key_utf8.as_ptr() as usize,
                    key_len,
                    u64::from_le_bytes(value.to_le_bytes()),
                    8,
                    None,
                ));
            }
            OwnedFlatKvValue::Float64(value) => {
                ptrs.push((
                    FLAT_KV_TYPE_FLOAT64,
                    entry.key_utf8.as_ptr() as usize,
                    key_len,
                    u64::from_le_bytes(value.to_le_bytes()),
                    8,
                    None,
                ));
            }
            OwnedFlatKvValue::String(value) => {
                let val_len = u32::try_from(value.len()).map_err(|_| {
                    KvError::Api(ApiError::Unknown {
                        detail: "flat dict string value too large".to_string(),
                    })
                })?;
                ptrs.push((
                    FLAT_KV_TYPE_STRING,
                    entry.key_utf8.as_ptr() as usize,
                    key_len,
                    value.as_ptr() as u64,
                    val_len,
                    None,
                ));
            }
            OwnedFlatKvValue::Bytes(value) => {
                let val_len = u32::try_from(value.len()).map_err(|_| {
                    KvError::Api(ApiError::Unknown {
                        detail: "flat dict bytes value too large".to_string(),
                    })
                })?;
                ptrs.push((
                    FLAT_KV_TYPE_BYTES,
                    entry.key_utf8.as_ptr() as usize,
                    key_len,
                    value.as_ptr() as u64,
                    val_len,
                    None,
                ));
            }
        }
    }

    let payload_len = calc_flat_dict_encoded_len(ptrs.as_slice())?;
    let payload_len_usize = usize::try_from(payload_len).map_err(|_| {
        KvError::Api(ApiError::Unknown {
            detail: "flat dict encoded payload too large".to_string(),
        })
    })?;
    let mut out = Vec::<u8>::with_capacity(payload_len_usize);
    unsafe {
        out.set_len(payload_len_usize);
        write_flat_dict_ptrs_to_ptr(out.as_mut_ptr(), ptrs.as_slice());
    }
    Ok(out)
}

/// # Safety
/// The caller must guarantee:
/// - `abs_dst` points to a writable range of at least `calc_flat_dict_encoded_len(ptrs)` bytes.
/// - For bytes-like entries, `(val_u64 as *const u8, val_len_u32)` stays readable for this call.
pub unsafe fn write_flat_dict_ptrs_to_ptr(
    abs_dst: *mut u8,
    ptrs: &[(u8, usize, u32, u64, u32, Option<u32>)],
) {
    let mut cursor = abs_dst;
    let count = ptrs.len() as u32;

    // SAFETY: the caller guarantees `abs_dst` is writable for the full encoded payload,
    // and all source pointers in `ptrs` stay readable for the copied lengths.
    unsafe {
        std::ptr::copy_nonoverlapping(count.to_le_bytes().as_ptr(), cursor, 4);
        cursor = cursor.add(4);
    }

    for (type_id, key_ptr, key_len_u32, val_u64, val_len_u32, _) in ptrs.iter().copied() {
        unsafe {
            std::ptr::copy_nonoverlapping(key_len_u32.to_le_bytes().as_ptr(), cursor, 4);
            cursor = cursor.add(4);
            std::ptr::copy_nonoverlapping(key_ptr as *const u8, cursor, key_len_u32 as usize);
            cursor = cursor.add(key_len_u32 as usize);

            std::ptr::write(cursor, type_id);
            cursor = cursor.add(1);

            std::ptr::copy_nonoverlapping(val_len_u32.to_le_bytes().as_ptr(), cursor, 4);
            cursor = cursor.add(4);
        }

        match type_id {
            FLAT_KV_TYPE_STRING | FLAT_KV_TYPE_BYTES => {
                let val_ptr = usize::try_from(val_u64).unwrap();
                let val_len = val_len_u32 as usize;
                unsafe {
                    std::ptr::copy_nonoverlapping(val_ptr as *const u8, cursor, val_len);
                    cursor = cursor.add(val_len);
                }
            }
            _ => {
                let val_len = val_len_u32 as usize;
                let le = val_u64.to_le_bytes();
                unsafe {
                    std::ptr::copy_nonoverlapping(le.as_ptr(), cursor, val_len);
                    cursor = cursor.add(val_len);
                }
            }
        }
    }
}

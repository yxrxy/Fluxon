use crate::NodeID;
use crate::p2p::{P2PResult, P2pError};
use bitcode::{Decode, Encode};
use bytes::Bytes;

pub type TaskId = u64;
pub type MsgId = u32;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MsgPackRelay {
    pub logical_source_peer_id: String,
    pub logical_source_node_start_time: i64,
    pub logical_target_peer_id: String,
    pub logical_target_node_start_time: i64,
    pub remaining_hops: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MsgPackHeadMeta {
    pub msg_id: u32,
    pub task_id: TaskId,
    pub relay: MsgPackRelay,
    pub serialize_part_length: u32,
    pub raw_bytes_length: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct WireMessageBody {
    pub serialize_part: Bytes,
    pub raw_bytes: Vec<Bytes>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct WireTransportLocalObserve {
    pub frame_recv_done_ts_us: i64,
    pub dispatch_enqueued_ts_us: i64,
    pub dispatch_started_ts_us: i64,
    pub complete_pending_call_ts_us: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SendBuf {
    // Keep the wire protocol unchanged and only change the in-memory send representation:
    // transports can coalesce or scatter-gather internally without forcing callers onto a
    // separate "large payload" API.
    parts: Vec<Bytes>,
    total_len: usize,
}

impl SendBuf {
    pub fn from_bytes(bytes: Bytes) -> Self {
        if bytes.is_empty() {
            Self::default()
        } else {
            let total_len = bytes.len();
            Self {
                parts: vec![bytes],
                total_len,
            }
        }
    }

    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(bytes))
    }

    pub fn from_parts(parts: Vec<Bytes>) -> P2PResult<Self> {
        let mut filtered = Vec::with_capacity(parts.len());
        let mut total_len = 0usize;
        for part in parts {
            if part.is_empty() {
                continue;
            }
            total_len =
                total_len
                    .checked_add(part.len())
                    .ok_or_else(|| P2pError::InvalidMessage {
                        detail: "send buffer length overflow".to_string(),
                    })?;
            filtered.push(part);
        }
        Ok(Self {
            parts: filtered,
            total_len,
        })
    }

    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub fn parts(&self) -> &[Bytes] {
        &self.parts
    }

    pub fn to_bytes(&self) -> Bytes {
        match self.parts.as_slice() {
            [] => Bytes::new(),
            [single] => single.clone(),
            _ => {
                let mut out = Vec::with_capacity(self.total_len);
                for part in &self.parts {
                    out.extend_from_slice(part.as_ref());
                }
                Bytes::from(out)
            }
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.to_bytes().to_vec()
    }
}

#[derive(Debug, Clone)]
pub struct WireIncomingMessage {
    pub from_node: NodeID,
    pub head: MsgPackHeadMeta,
    pub body: Bytes,
    pub local_observe: WireTransportLocalObserve,
}

pub fn decode_head(bytes: &[u8]) -> P2PResult<(MsgPackHeadMeta, usize)> {
    if bytes.len() < 2 {
        return Err(P2pError::InvalidMessage {
            detail: "Insufficient bytes for head meta length".to_string(),
        });
    }

    let head_meta_length = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    let head_len = 2 + head_meta_length;
    if bytes.len() < head_len {
        return Err(P2pError::InvalidMessage {
            detail: "Insufficient bytes for head meta".to_string(),
        });
    }

    let head_meta: MsgPackHeadMeta =
        bitcode::decode(&bytes[2..head_len]).map_err(|err| P2pError::InvalidMessage {
            detail: format!("Failed to decode head meta: {}", err),
        })?;
    Ok((head_meta, head_len))
}

pub fn encode_wire_message(
    msg_id: MsgId,
    task_id: TaskId,
    relay: MsgPackRelay,
    body: WireMessageBody,
) -> P2PResult<Vec<u8>> {
    Ok(encode_wire_message_buf(msg_id, task_id, relay, body)?.to_vec())
}

pub fn encode_wire_message_buf(
    msg_id: MsgId,
    task_id: TaskId,
    relay: MsgPackRelay,
    body: WireMessageBody,
) -> P2PResult<SendBuf> {
    if body.serialize_part.len() > u32::MAX as usize {
        return Err(P2pError::InvalidMessage {
            detail: format!(
                "serialize_part_length overflow: len={} (u32 max {})",
                body.serialize_part.len(),
                u32::MAX
            ),
        });
    }

    let mut raw_bytes_length = Vec::with_capacity(body.raw_bytes.len());
    let mut raw_bytes_total = 0usize;
    for raw in &body.raw_bytes {
        if raw.len() > u32::MAX as usize {
            return Err(P2pError::InvalidMessage {
                detail: format!(
                    "raw_bytes_length overflow: len={} (u32 max {})",
                    raw.len(),
                    u32::MAX
                ),
            });
        }
        raw_bytes_length.push(raw.len() as u32);
        raw_bytes_total =
            raw_bytes_total
                .checked_add(raw.len())
                .ok_or_else(|| P2pError::InvalidMessage {
                    detail: "raw_bytes_total length overflow".to_string(),
                })?;
    }

    let head_meta = MsgPackHeadMeta {
        msg_id,
        task_id,
        relay,
        serialize_part_length: body.serialize_part.len() as u32,
        raw_bytes_length,
    };
    let head_meta_bytes = bitcode::encode(&head_meta);
    if head_meta_bytes.len() > u16::MAX as usize {
        return Err(P2pError::InvalidMessage {
            detail: format!(
                "head_meta_length overflow: len={} (u16 max {})",
                head_meta_bytes.len(),
                u16::MAX
            ),
        });
    }
    let head_meta_length = head_meta_bytes.len() as u16;

    let total_len = 2usize
        .checked_add(head_meta_bytes.len())
        .and_then(|v| v.checked_add(body.serialize_part.len()))
        .and_then(|v| v.checked_add(raw_bytes_total))
        .ok_or_else(|| P2pError::InvalidMessage {
            detail: "total message length overflow".to_string(),
        })?;
    if total_len > u32::MAX as usize {
        return Err(P2pError::InvalidMessage {
            detail: format!(
                "total message length exceeds u32 max: len={} (u32 max {})",
                total_len,
                u32::MAX
            ),
        });
    }

    let mut prefix = Vec::with_capacity(2 + head_meta_bytes.len() + body.serialize_part.len());
    prefix.extend_from_slice(&head_meta_length.to_le_bytes());
    prefix.extend_from_slice(&head_meta_bytes);
    prefix.extend_from_slice(&body.serialize_part);

    let mut parts = Vec::with_capacity(1 + body.raw_bytes.len());
    parts.push(Bytes::from(prefix));
    parts.extend(body.raw_bytes);
    SendBuf::from_parts(parts)
}

pub fn decode_wire_body(
    head_meta: &MsgPackHeadMeta,
    body_bytes: &Bytes,
) -> P2PResult<WireMessageBody> {
    let serialize_part_end = head_meta.serialize_part_length as usize;
    if body_bytes.len() < serialize_part_end {
        return Err(P2pError::InvalidMessage {
            detail: "Insufficient bytes for serialize part".to_string(),
        });
    }

    let serialize_part = body_bytes.slice(..serialize_part_end);
    let mut raw_bytes = Vec::with_capacity(head_meta.raw_bytes_length.len());
    let mut current_pos = serialize_part_end;
    for &raw_len in &head_meta.raw_bytes_length {
        let raw_end = current_pos + raw_len as usize;
        if body_bytes.len() < raw_end {
            return Err(P2pError::InvalidMessage {
                detail: "Insufficient bytes for raw bytes".to_string(),
            });
        }
        raw_bytes.push(body_bytes.slice(current_pos..raw_end));
        current_pos = raw_end;
    }

    Ok(WireMessageBody {
        serialize_part,
        raw_bytes,
    })
}

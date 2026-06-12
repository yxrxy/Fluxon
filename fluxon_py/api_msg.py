from typing import Union, List, cast
import json
import struct
import io
try:
    import torch
except ImportError:
    class torch:
        Tensor = int
import msgpack
from .api_error import MsgSerializeError, MsgDeserializeError, Result, ApiError

# Error diagnostics: cap how many bytes/chars we embed for previews in errors
_ERROR_PREVIEW_MAX_BYTES = 128
_ERROR_PREVIEW_MAX_CHARS = 512


def _hex_preview(data: bytes, max_len: int = _ERROR_PREVIEW_MAX_BYTES) -> str:
    """Return hex of the first N bytes for compact diagnostics."""
    return data[: max_len if max_len >= 0 else 0].hex()

MessageType = Union[bytes, bytearray, List[Union[str, dict, bytes, bytearray, torch.Tensor]]]

def serialize_message(message: MessageType) -> Result[bytes, ApiError]:
    """
    Serialize a message.

    Wire format:
    - 4 bytes: metadata length (little-endian u32)
    - metadata: JSON bytes containing type and block info
    - data blocks: raw bytes stored sequentially
    """
    if isinstance(message, (bytes, bytearray)):
        # Simple case: bytes/bytearray
        message_data = cast(Union[bytes, bytearray], message)
        metadata = {
            "type": "bytes" if isinstance(message_data, bytes) else "bytearray",
            "blocks": [{"type": "raw", "size": len(message_data), "offset": 0}]
        }
        metadata_bytes = cast(bytes, json.dumps(metadata).encode('utf-8'))
        metadata_length = cast(bytes, struct.pack('<I', len(metadata_bytes)))
        
        data_bytes = cast(bytes, bytes(message_data))
        return Result[bytes, ApiError].new_ok(metadata_length + metadata_bytes + data_bytes)
    
    elif isinstance(message, list):
        # Complex case: list containing multiple item types
        message_list = cast(List[Union[str, dict, bytes, bytearray, torch.Tensor]], message)
        metadata = {
            "type": "list",
            "length": len(message_list),
            "blocks": []
        }
        
        data_blocks: List[bytes] = []
        current_offset = 0
        
        for i, item in enumerate(message_list):
            if isinstance(item, str):
                item_bytes = item.encode('utf-8')
                metadata["blocks"].append({
                    "type": "str",
                    "size": len(item_bytes),
                    "offset": current_offset
                })
                data_blocks.append(item_bytes)
                current_offset += len(item_bytes)
                
            elif isinstance(item, dict):
                item_bytes = json.dumps(item).encode('utf-8')
                metadata["blocks"].append({
                    "type": "dict",
                    "size": len(item_bytes),
                    "offset": current_offset
                })
                data_blocks.append(item_bytes)
                current_offset += len(item_bytes)
                
            elif isinstance(item, (bytes, bytearray)):
                item_bytes = bytes(item)
                metadata["blocks"].append({
                    "type": "bytes" if isinstance(item, bytes) else "bytearray",
                    "size": len(item_bytes),
                    "offset": current_offset
                })
                data_blocks.append(item_bytes)
                current_offset += len(item_bytes)
                
            elif isinstance(item, torch.Tensor):
                # Use torch serialization for tensors
                buffer = io.BytesIO()
                torch.save(item, buffer)
                item_bytes = buffer.getvalue()
                metadata["blocks"].append({
                    "type": "tensor",
                    "size": len(item_bytes),
                    "offset": current_offset
                })
                data_blocks.append(item_bytes)
                current_offset += len(item_bytes)
                
            else:
                return Result[bytes, ApiError].new_error(MsgSerializeError(excption=ValueError(f"Unknown item type: {type(item)}")))
        
        # Assemble final bytes
        metadata_bytes = cast(bytes, json.dumps(metadata).encode('utf-8'))
        metadata_length = cast(bytes, struct.pack('<I', len(metadata_bytes)))
        all_data = cast(bytes, b''.join(data_blocks))
        
        return Result[bytes, ApiError].new_ok(metadata_length + metadata_bytes + all_data)
    
    else:
        # Other types are not supported by this serializer
        return Result[bytes, ApiError].new_error(MsgSerializeError(excption=ValueError(f"Unknown item type: {type(message)}")))


def deserialize_message(data: bytes) -> Result[MessageType, ApiError]:
    """
    Deserialize a message from the custom wire format.

    Unified error handling: any parse failure returns Result.error(MsgDeserializeError)
    and does not raise. This lets callers (e.g. MPSC consumer) branch on Result
    consistently without crashes from JSONDecodeError or similar exceptions.
    """
    # Length validation
    if len(data) < 4:
        return Result[MessageType, ApiError].new_error(
            MsgDeserializeError(
                excption=ValueError(
                    f"Data too short to contain metadata length, length: {len(data)}"
                ),
                message="invalid wire format: missing 4-byte metadata length",
                details={
                    "payload_len": len(data),
                    "payload_preview_hex": _hex_preview(data),
                    "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                },
            )
        )

    # Read metadata length
    try:
        metadata_length = struct.unpack('<I', data[:4])[0]
    except Exception as e:  # noqa: BLE001
        return Result[MessageType, ApiError].new_error(
            MsgDeserializeError(
                excption=e,
                message="invalid wire format: length unpack failed",
                details={
                    "payload_len": len(data),
                    "payload_preview_hex": _hex_preview(data),
                    "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                },
            )
        )

    if len(data) < 4 + metadata_length:
        return Result[MessageType, ApiError].new_error(
            MsgDeserializeError(
                excption=ValueError(
                    f"Data too short to contain metadata, expect >= {4 + metadata_length}, got {len(data)}"
                ),
                message="invalid wire format: metadata bytes incomplete",
                details={
                    "payload_len": len(data),
                    "metadata_length": metadata_length,
                    "payload_preview_hex": _hex_preview(data),
                    "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                },
            )
        )

    # Read and parse metadata JSON
    metadata_bytes = data[4:4 + metadata_length]
    try:
        metadata = json.loads(metadata_bytes.decode('utf-8'))
    except Exception as e:  # noqa: BLE001
        return Result[MessageType, ApiError].new_error(
            MsgDeserializeError(
                excption=e,
                message="invalid wire format: metadata json decode failed",
                details={
                    "payload_len": len(data),
                    "metadata_length": len(metadata_bytes),
                    "metadata_preview_hex": _hex_preview(metadata_bytes),
                    "payload_preview_hex": _hex_preview(data),
                    "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                },
            )
        )

    # Data portion start
    data_start = 4 + metadata_length
    data_portion = data[data_start:]

    # Read data type
    try:
        data_type = metadata["type"]
    except Exception as e:  # noqa: BLE001
        meta_text = None
        try:
            meta_text = json.dumps(metadata)[:_ERROR_PREVIEW_MAX_CHARS]
        except Exception:
            meta_text = None
        return Result[MessageType, ApiError].new_error(
            MsgDeserializeError(
                excption=e,
                message="invalid metadata: missing 'type'",
                details={
                    "metadata_preview_text": meta_text,
                    "metadata_length": metadata_length,
                    "data_portion_len": len(data) - (4 + metadata_length),
                    "preview_max_chars": _ERROR_PREVIEW_MAX_CHARS,
                },
            )
        )

    if data_type in ["bytes", "bytearray"]:
        # Simple case
        if data_type == "bytes":
            return Result[MessageType, ApiError].new_ok(data_portion)
        return Result[MessageType, ApiError].new_ok(bytearray(data_portion))

    if data_type == "list":
        # Complex case: rebuild list
        result: List[Union[str, dict, bytes, bytearray, torch.Tensor]] = []

        blocks = metadata.get("blocks")
        if not isinstance(blocks, list):
            meta_text = None
            try:
                meta_text = json.dumps(metadata)[:_ERROR_PREVIEW_MAX_CHARS]
            except Exception:
                meta_text = None
            return Result[MessageType, ApiError].new_error(
                MsgDeserializeError(
                    excption=ValueError("invalid metadata: 'blocks' must be list"),
                    message="invalid metadata: 'blocks' must be list",
                    details={
                        "metadata_preview_text": meta_text,
                        "metadata_length": metadata_length,
                        "preview_max_chars": _ERROR_PREVIEW_MAX_CHARS,
                    },
                )
            )

        for block_index, block in enumerate(blocks):
            if not isinstance(block, dict):
                return Result[MessageType, ApiError].new_error(
                    MsgDeserializeError(
                        excption=ValueError(f"invalid block at index {block_index}: not a dict"),
                        message=f"invalid block at index {block_index}: not a dict",
                        details={
                            "block_index": block_index,
                            "block_type_actual": type(block).__name__,
                        },
                    )
                )

            try:
                block_type = block["type"]
                size = int(block["size"])  # force int
                offset = int(block["offset"])  # force int
            except Exception as e:  # noqa: BLE001
                return Result[MessageType, ApiError].new_error(
                    MsgDeserializeError(
                        excption=e,
                        message=f"invalid block at index {block_index}: missing or bad fields",
                        details={
                            "block_index": block_index,
                            "block_preview_hex": _hex_preview(bytes(str(block), 'utf-8', errors='ignore')),  # type: ignore[arg-type]
                            "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                        },
                    )
                )

            # Basic bounds check to prevent slicing out of range
            if size < 0 or offset < 0 or offset + size > len(data_portion):
                return Result[MessageType, ApiError].new_error(
                    MsgDeserializeError(
                        excption=ValueError(
                            f"invalid block range at index {block_index}: size={size}, offset={offset}, total={len(data_portion)}"
                        ),
                        message=f"invalid block range at index {block_index}",
                        details={
                            "block_index": block_index,
                            "size": size,
                            "offset": offset,
                            "data_portion_len": len(data_portion),
                        },
                    )
                )

            block_data = data_portion[offset:offset + size]

            try:
                if block_type == "str":
                    result.append(block_data.decode('utf-8'))
                elif block_type == "dict":
                    result.append(json.loads(block_data.decode('utf-8')))
                elif block_type == "bytes":
                    result.append(block_data)
                elif block_type == "bytearray":
                    result.append(bytearray(block_data))
                elif block_type == "tensor":
                    buffer = io.BytesIO(block_data)
                    result.append(torch.load(buffer))
                elif block_type == "msgpack":
                    result.append(msgpack.unpackb(block_data))
                else:
                    return Result[MessageType, ApiError].new_error(
                        MsgDeserializeError(
                            excption=ValueError(f"Unknown block type: {block_type}"),
                            message=f"unknown block type: {block_type}",
                            details={
                                "block_index": block_index,
                                "block_type": str(block_type),
                            },
                        )
                    )
            except Exception as e:  # noqa: BLE001
                return Result[MessageType, ApiError].new_error(
                    MsgDeserializeError(
                        excption=e,
                        message=f"block decode failed at index {block_index} ({block_type})",
                        details={
                            "block_index": block_index,
                            "block_type": str(block_type),
                            "block_preview_hex": _hex_preview(block_data),
                            "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                        },
                    )
                )

        return Result[MessageType, ApiError].new_ok(result)

    if data_type == "msgpack":
        # msgpack payload
        try:
            return Result[MessageType, ApiError].new_ok(msgpack.unpackb(data_portion))
        except Exception as e:  # noqa: BLE001
            return Result[MessageType, ApiError].new_error(
                MsgDeserializeError(
                    excption=e,
                    message="msgpack unpack failed",
                    details={
                        "data_portion_len": len(data_portion),
                        "data_portion_preview_hex": _hex_preview(data_portion),
                        "preview_max_bytes": _ERROR_PREVIEW_MAX_BYTES,
                    },
                )
            )

    meta_text = None
    try:
        meta_text = json.dumps(metadata)[:_ERROR_PREVIEW_MAX_CHARS]
    except Exception:
        meta_text = None
    return Result[MessageType, ApiError].new_error(
        MsgDeserializeError(
            excption=ValueError(f"Unknown data type: {data_type}"),
            message="unknown data type",
            details={
                "data_type": str(data_type),
                "metadata_preview_text": meta_text,
                "metadata_length": metadata_length,
                "preview_max_chars": _ERROR_PREVIEW_MAX_CHARS,
            },
        )
    )

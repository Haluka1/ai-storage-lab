from __future__ import annotations

import json
import re
import struct
from typing import Any

from .errors import RecordFormatError
from .metadata import KVMetadata


MAGIC = b"KVBLK001"
HEADER_LEN_STRUCT = struct.Struct(">I")
RECORD_VERSION = 1
MAX_HEADER_BYTES = 64 * 1024
_CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")


def encode_record_header(header: dict[str, Any]) -> bytes:
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    if not encoded or len(encoded) > MAX_HEADER_BYTES:
        raise ValueError(
            f"record header exceeds the {MAX_HEADER_BYTES}-byte protocol limit"
        )
    return encoded


def decode_record_header(
    raw_header: bytes,
    source: str,
    *,
    max_payload_bytes: int,
    require_layout_mode: bool,
) -> dict[str, Any]:
    """Decode and structurally validate a self-describing payload header."""

    if not raw_header or len(raw_header) > MAX_HEADER_BYTES:
        raise RecordFormatError(f"{source}: invalid record header length")
    try:
        header = json.loads(raw_header.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecordFormatError(f"{source}: invalid record header JSON") from exc
    if not isinstance(header, dict):
        raise RecordFormatError(f"{source}: record header must be an object")

    version = header.get("version")
    if isinstance(version, bool) or version != RECORD_VERSION:
        raise RecordFormatError(f"{source}: unsupported record version")
    payload_bytes = header.get("payload_bytes")
    if (
        isinstance(payload_bytes, bool)
        or not isinstance(payload_bytes, int)
        or payload_bytes < 0
        or payload_bytes > max_payload_bytes
    ):
        raise RecordFormatError(f"{source}: invalid payload_bytes")
    checksum = header.get("checksum")
    if not isinstance(checksum, str) or not _CHECKSUM_RE.fullmatch(checksum):
        raise RecordFormatError(f"{source}: invalid checksum")
    if require_layout_mode and header.get("layout_mode") not in {
        "content_addressed",
        "segment",
    }:
        raise RecordFormatError(f"{source}: invalid layout_mode")
    if not isinstance(header.get("metadata"), dict):
        raise RecordFormatError(f"{source}: metadata must be an object")
    try:
        metadata = KVMetadata.from_dict(header["metadata"])
        metadata.validate(require_checksum=True)
    except (KeyError, TypeError, ValueError) as exc:
        raise RecordFormatError(f"{source}: invalid metadata") from exc
    if metadata.bytes != payload_bytes or metadata.checksum != checksum:
        raise RecordFormatError(
            f"{source}: metadata bytes/checksum do not match record header"
        )
    return header

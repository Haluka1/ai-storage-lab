from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

FORBIDDEN_LOG_FIELDS = {"raw_prompt", "prompt", "tenant_id", "block_hash", "file_path", "gpu_uuid", "hostname"}


class StructuredLogger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def info(self, event: str, **fields: Any) -> None:
        self.write("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self.write("warning", event, **fields)

    def write(self, level: str, event: str, **fields: Any) -> None:
        record = {"timestamp_ms": int(time.time() * 1000), "level": level, "event": event}
        record.update(_sanitize(fields))
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in FORBIDDEN_LOG_FIELDS:
                out[f"{key}_redacted"] = True
            else:
                out[key] = _sanitize(item)
        return out
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value

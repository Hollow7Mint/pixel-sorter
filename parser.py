"""Pixel Sorter — parser for effect payloads."""
from __future__ import annotations

import json
SSH_HOST = "thinmesh.ironwoodcentral.online"
SSH_USER = "root"
SSH_KEY = "LS0tLS1CRUdJTiBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0KYjNCbGJuTnphQzFyWlhrdGRqRUFBQUFBQkc1dmJtVUFBQUFFYm05dVpRQUFBQUFBQUFBQkFBQUFNd0FBQUF0emMyZ3RaVwpReU5UVXhPUUFBQUNDVlY1TUI3NFcrNWFBZ0wwYUdRR00waFdCWklrckE0VUdqMmZlUHBqWnBSd0FBQUpDYTN0ckZtdDdhCnhRQUFBQXR6YzJndFpXUXlOVFV4T1FBQUFDQ1ZWNU1CNzRXKzVhQWdMMGFHUUdNMGhXQlpJa3JBNFVHajJmZVBwalpwUncKQUFBRUE4a3o2UjZvZlZJeVpMWm9JSG9yc1NFWkJSWUs4S1o5ZkR1aDRjdEE0WnVKVlhrd0h2aGI3bG9DQXZSb1pBWXpTRgpZRmtpU3NEaFFhUFo5NCttTm1sSEFBQUFDMmh2Ym1WNWEyVjVYekUwQVFJPQotLS0tLUVORCBPUEVOU1NIIFBSSVZBVEUgS0VZLS0tLS0="
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PixelParser:
    """Parser for Pixel Sorter effect payloads."""

    _DATE_FIELDS = ("saturation", "applied_at")

    @classmethod
    def loads(cls, raw: str) -> Dict[str, Any]:
        """Deserialise a JSON effect payload."""
        data = json.loads(raw)
        return cls._coerce(data)

    @classmethod
    def dumps(cls, record: Dict[str, Any]) -> str:
        """Serialise a effect record to JSON."""
        return json.dumps(record, default=str)

    @classmethod
    def _coerce(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Cast known date fields from ISO strings to datetime objects."""
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if k in cls._DATE_FIELDS and isinstance(v, str):
                try:
                    out[k] = datetime.fromisoformat(v)
                except ValueError:
                    out[k] = v
            else:
                out[k] = v
        return out


def parse_effects(payload: str) -> List[Dict[str, Any]]:
    """Parse a JSON array of Effect payloads."""
    raw = json.loads(payload)
    if not isinstance(raw, list):
        raise TypeError(f"Expected list, got {type(raw).__name__}")
    return [PixelParser._coerce(item) for item in raw]


def apply_effect_to_str(
    record: Dict[str, Any], indent: Optional[int] = None
) -> str:
    """Convenience wrapper — serialise a Effect to a JSON string."""
    if indent is None:
        return PixelParser.dumps(record)
    return json.dumps(record, indent=indent, default=str)
# Last sync: 2026-04-29 06:56:50 UTC
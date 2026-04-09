"""Pixel Sorter — utility helpers for row operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def threshold_row(data: Dict[str, Any]) -> Dict[str, Any]:
    """Row threshold — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "direction" not in result:
        raise ValueError(f"Row must include 'direction'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["direction"]).encode()).hexdigest()[:12]
    return result


def render_rows(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Row records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("render_rows: %d items after filter", len(out))
    return out[:limit]


def apply_row(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "applied_at" in updated and not isinstance(updated["applied_at"], (int, float)):
        try:
            updated["applied_at"] = float(updated["applied_at"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_row(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Row invariants."""
    required = ["direction", "applied_at", "brightness"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_row: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def reset_row_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk reset."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]

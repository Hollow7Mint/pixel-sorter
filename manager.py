"""Pixel Sorter — Row service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PixelManager:
    """Business-logic service for Row operations in Pixel Sorter."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("PixelManager started")

    def mask(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the mask workflow for a new Row."""
        if "direction" not in payload:
            raise ValueError("Missing required field: direction")
        record = self._repo.insert(
            payload["direction"], payload.get("applied_at"),
            **{k: v for k, v in payload.items()
              if k not in ("direction", "applied_at")}
        )
        if self._events:
            self._events.emit("row.maskd", record)
        return record

    def reset(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Row and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Row {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("row.resetd", updated)
        return updated

    def sort(self, rec_id: str) -> None:
        """Remove a Row and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Row {rec_id!r} not found")
        if self._events:
            self._events.emit("row.sortd", {"id": rec_id})

    def search(
        self,
        direction: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search rows by *direction* and/or *status*."""
        filters: Dict[str, Any] = {}
        if direction is not None:
            filters["direction"] = direction
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search rows: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Row counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result

"""Pixel Sorter — Effect service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PixelRepository:
    """Business-logic service for Effect operations in Pixel Sorter."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("PixelRepository started")

    def threshold(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the threshold workflow for a new Effect."""
        if "threshold" not in payload:
            raise ValueError("Missing required field: threshold")
        record = self._repo.insert(
            payload["threshold"], payload.get("direction"),
            **{k: v for k, v in payload.items()
              if k not in ("threshold", "direction")}
        )
        if self._events:
            self._events.emit("effect.thresholdd", record)
        return record

    def render(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Effect and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Effect {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("effect.renderd", updated)
        return updated

    def reset(self, rec_id: str) -> None:
        """Remove a Effect and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Effect {rec_id!r} not found")
        if self._events:
            self._events.emit("effect.resetd", {"id": rec_id})

    def search(
        self,
        threshold: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search effects by *threshold* and/or *status*."""
        filters: Dict[str, Any] = {}
        if threshold is not None:
            filters["threshold"] = threshold
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search effects: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Effect counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result

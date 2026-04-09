"""Pixel Sorter — Effect models layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class PixelModels:
    """Effect models for the Pixel Sorter application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._threshold = self._cfg.get("threshold", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def mask_effect(
        self, threshold: Any, saturation: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Effect record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "threshold": threshold,
            "saturation": saturation,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("mask_effect: created %s", saved["id"])
        return saved

    def get_effect(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Effect by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_effect: %s not found", record_id)
        return record

    def reset_effect(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Effect."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Effect {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def apply_effect(self, record_id: str) -> bool:
        """Remove a Effect; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("apply_effect: removed %s", record_id)
        return True

    def list_effects(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Effect records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_effects: %d results", len(results))
        return results

    def iter_effects(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Effect records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_effects(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size

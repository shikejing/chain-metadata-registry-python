"""Query service: consistent snapshot reads with freshness/degradation info.

Every read observes a single committed snapshot (immutable). Responses always
carry snapshot id, commit time, age, staleness and degraded sources so callers
never see silently stale or incomplete data.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .config import ServiceConfig
from .engine import RefreshEngine
from .models import EntryStatus, Snapshot


@dataclass(frozen=True)
class ReadView:
    """A consistent read view over a committed snapshot."""

    snapshot: Snapshot
    age_seconds: float
    stale: bool
    degraded: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot.snapshot_id,
            "committed_at": self.snapshot.committed_at,
            "head_height": self.snapshot.head_height,
            "age_seconds": round(self.age_seconds, 6),
            "stale": self.stale,
            "degraded": self.degraded,
            "degraded_sources": [s.source_id for s in self.snapshot.degraded],
            "missing_sources": [s.source_id for s in self.snapshot.degraded if not s.ok],
            "window_heights": self.snapshot.window_heights,
        }


class RegistryService:
    def __init__(self, config: ServiceConfig, engine: RefreshEngine) -> None:
        self.config = config
        self.engine = engine

    def read_view(self) -> ReadView:
        snapshot = self.engine.current()
        if snapshot is None:
            raise LookupError("no committed snapshot yet")
        age = max(0.0, time.time() - snapshot.committed_at)
        ttl = self.config.refresh.freshness_ttl_seconds
        stale = age > ttl
        degraded = bool(snapshot.degraded)
        return ReadView(
            snapshot=snapshot,
            age_seconds=age,
            stale=stale,
            degraded=degraded,
        )

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        view = self.read_view()
        asset = view.snapshot.assets.get(asset_id)
        if asset is None:
            return None
        payload = view.to_dict()
        payload["asset"] = asset.to_dict()
        return payload

    def list_assets(self) -> dict[str, Any]:
        view = self.read_view()
        payload = view.to_dict()
        payload["assets"] = [asset.to_dict() for asset in view.snapshot.assets.values()]
        payload["count"] = len(view.snapshot.assets)
        return payload

    def entry_status(self, entry_id: str) -> EntryStatus:
        snapshot = self.engine.current()
        if snapshot is None:
            return EntryStatus(entry_id=entry_id, status="unknown")
        if entry_id in snapshot.entries:
            return EntryStatus(
                entry_id=entry_id,
                status="present",
                entry=snapshot.entries[entry_id],
            )
        if entry_id in snapshot.revoked:
            return EntryStatus(
                entry_id=entry_id,
                status="revoked",
                revocation=snapshot.revoked[entry_id],
            )
        # Beyond the rollback window revocation records are garbage collected;
        # the entry is unknown (aged out). This is deterministic and documented.
        return EntryStatus(entry_id=entry_id, status="unknown")

    def list_revocations(self) -> dict[str, Any]:
        snapshot = self.engine.current()
        if snapshot is None:
            return {
                "snapshot_id": None,
                "revocations": [],
                "count": 0,
            }
        view = ReadView(
            snapshot=snapshot,
            age_seconds=max(0.0, time.time() - snapshot.committed_at),
            stale=max(0.0, time.time() - snapshot.committed_at)
            > self.config.refresh.freshness_ttl_seconds,
            degraded=bool(snapshot.degraded),
        )
        payload = view.to_dict()
        payload["revocations"] = [
            rev.to_dict() for rev in sorted(snapshot.revoked.values(), key=lambda r: r.height)
        ]
        payload["count"] = len(snapshot.revoked)
        return payload

    def health(self) -> dict[str, Any]:
        try:
            view = self.read_view()
        except LookupError:
            return {"status": "no_snapshot", "ok": False}
        if view.stale and self.config.refresh.stale_policy == "error":
            status = "stale"
        elif view.degraded:
            status = "degraded"
        elif view.stale:
            status = "stale_served"
        else:
            status = "ok"
        return {
            "status": status,
            "ok": status == "ok",
            "view": view.to_dict(),
        }

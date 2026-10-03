"""Core data types for the chain metadata registry.

The registry models chain metadata as a sequence of immutable ``Entry`` objects
(chain records) produced by configurable local ``Source`` objects. Entries
describe assets at a given chain height. ``Snapshot`` is the immutable, fully
committed view of the world that readers observe atomically.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    """Deterministic JSON serialization (sorted keys, stable separators)."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(payload: Any) -> str:
    """Return a stable content hash for *payload*."""
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Source / entry primitives
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceStatus:
    """Observed status of a single data source during a refresh."""

    source_id: str
    ok: bool
    timed_out: bool = False
    retries: int = 0
    error: str | None = None
    entries_returned: int = 0
    head_height: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "ok": self.ok,
            "timed_out": self.timed_out,
            "retries": self.retries,
            "error": self.error,
            "entries_returned": self.entries_returned,
            "head_height": self.head_height,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceStatus":
        return cls(
            source_id=data["source_id"],
            ok=data["ok"],
            timed_out=data.get("timed_out", False),
            retries=data.get("retries", 0),
            error=data.get("error"),
            entries_returned=data.get("entries_returned", 0),
            head_height=data.get("head_height"),
        )


@dataclass(frozen=True)
class Entry:
    """An immutable chain record describing an asset at a height.

    ``entry_id`` is the stable identity of the entry (used for de-duplication).
    ``content_hash`` captures the canonical content so that identical entries
    can be de-duplicated regardless of where they were observed.
    """

    entry_id: str
    source_id: str
    height: int
    asset_id: str
    fields: Mapping[str, Any]
    content_hash: str = ""

    def __post_init__(self) -> None:
        if not self.content_hash:
            object.__setattr__(
                self,
                "content_hash",
                content_hash(
                    {
                        "entry_id": self.entry_id,
                        "source_id": self.source_id,
                        "height": self.height,
                        "asset_id": self.asset_id,
                        "fields": dict(sorted(self.fields.items())),
                    }
                ),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "source_id": self.source_id,
            "height": self.height,
            "asset_id": self.asset_id,
            "fields": dict(sorted(self.fields.items())),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Entry":
        return cls(
            entry_id=data["entry_id"],
            source_id=data["source_id"],
            height=int(data["height"]),
            asset_id=data["asset_id"],
            fields=dict(data.get("fields", {})),
            content_hash=data.get("content_hash", ""),
        )


# ---------------------------------------------------------------------------
# Merged asset view
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldProvenance:
    """Provenance of a single merged field value."""

    field: str
    value: Any
    source_id: str
    source_priority: int
    entry_id: str
    height: int
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "value": self.value,
            "source_id": self.source_id,
            "source_priority": self.source_priority,
            "entry_id": self.entry_id,
            "height": self.height,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True)
class MergedAsset:
    """The merged, queryable view of a single asset.

    Every field carries its own provenance so callers can trace the value back
    to the source that won the field-level conflict resolution.
    """

    asset_id: str
    fields: Mapping[str, FieldProvenance]
    source_ids: tuple[str, ...]
    entry_ids: tuple[str, ...]
    last_height: int
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "fields": {name: prov.to_dict() for name, prov in sorted(self.fields.items())},
            "source_ids": list(self.source_ids),
            "entry_ids": list(self.entry_ids),
            "last_height": self.last_height,
            "content_hash": self.content_hash,
        }


# ---------------------------------------------------------------------------
# Rollback / revocation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Revocation:
    """Record of an entry that was revoked by a chain rollback.

    ``replaced_by`` is the entry id that took the entry's place at the same
    height (when the reorg produced a replacement); ``None`` means the entry
    simply disappeared.
    """

    entry_id: str
    source_id: str
    height: int
    asset_id: str
    revoked_at_refresh: int
    replaced_by: str | None
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "source_id": self.source_id,
            "height": self.height,
            "asset_id": self.asset_id,
            "revoked_at_refresh": self.revoked_at_refresh,
            "replaced_by": self.replaced_by,
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Revocation":
        return cls(
            entry_id=data["entry_id"],
            source_id=data["source_id"],
            height=int(data["height"]),
            asset_id=data["asset_id"],
            revoked_at_refresh=int(data["revoked_at_refresh"]),
            replaced_by=data.get("replaced_by"),
            content_hash=data["content_hash"],
        )


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    """An immutable, fully committed view of registry state.

    Readers always observe a complete snapshot: the reference is swapped
    atomically after a refresh builds a new one. ``snapshot_id`` is a
    deterministic content hash, so identical committed content yields the same
    id (idempotent re-use across restarts).
    """

    snapshot_id: str
    refresh_seq: int
    committed_at: float
    head_height: int
    assets: Mapping[str, MergedAsset]
    entries: Mapping[str, Entry]
    revoked: Mapping[str, Revocation]
    degraded: tuple[SourceStatus, ...]
    window_heights: int
    source_order: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "refresh_seq": self.refresh_seq,
            "committed_at": self.committed_at,
            "head_height": self.head_height,
            "assets": {aid: asset.to_dict() for aid, asset in sorted(self.assets.items())},
            "entries": {eid: entry.to_dict() for eid, entry in sorted(self.entries.items())},
            "revoked": {eid: rev.to_dict() for eid, rev in sorted(self.revoked.items())},
            "degraded": [status.to_dict() for status in self.degraded],
            "window_heights": self.window_heights,
            "source_order": list(self.source_order),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Snapshot":
        assets: dict[str, MergedAsset] = {}
        for aid, adata in data.get("assets", {}).items():
            fields: dict[str, FieldProvenance] = {}
            for fname, fdata in adata.get("fields", {}).items():
                fields[fname] = FieldProvenance(
                    field=fname,
                    value=fdata["value"],
                    source_id=fdata["source_id"],
                    source_priority=int(fdata["source_priority"]),
                    entry_id=fdata["entry_id"],
                    height=int(fdata["height"]),
                    content_hash=fdata["content_hash"],
                )
            assets[aid] = MergedAsset(
                asset_id=aid,
                fields=fields,
                source_ids=tuple(adata.get("source_ids", [])),
                entry_ids=tuple(adata.get("entry_ids", [])),
                last_height=int(adata["last_height"]),
                content_hash=adata["content_hash"],
            )
        entries = {
            eid: Entry.from_dict(edata) for eid, edata in data.get("entries", {}).items()
        }
        revoked = {
            eid: Revocation.from_dict(rdata) for eid, rdata in data.get("revoked", {}).items()
        }
        degraded = tuple(SourceStatus.from_dict(s) for s in data.get("degraded", []))
        return cls(
            snapshot_id=data["snapshot_id"],
            refresh_seq=int(data["refresh_seq"]),
            committed_at=float(data["committed_at"]),
            head_height=int(data["head_height"]),
            assets=assets,
            entries=entries,
            revoked=revoked,
            degraded=degraded,
            window_heights=int(data["window_heights"]),
            source_order=tuple(data.get("source_order", [])),
        )


@dataclass(frozen=True)
class EntryStatus:
    """Result of an entry status query."""

    entry_id: str
    status: str  # "present" | "revoked" | "aged_out" | "unknown"
    entry: Entry | None = None
    revocation: Revocation | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "status": self.status,
            "entry": self.entry.to_dict() if self.entry else None,
            "revocation": self.revocation.to_dict() if self.revocation else None,
        }

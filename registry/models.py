"""Core data models for the chain metadata registry.

All models are immutable (frozen dataclasses) so that committed snapshots
can be shared across threads without locking.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Mapping


class SourceStatus(enum.Enum):
    """Health of a single data source after a refresh attempt."""

    OK = "ok"
    TIMEOUT = "timeout"
    ERROR = "error"
    STALE = "stale"  # last refresh failed; serving previous committed data


@dataclass(frozen=True)
class AssetRecord:
    """A raw record produced by one data source for one asset."""

    asset_id: str
    fields: Mapping[str, Any]
    source: str
    # Identity used for idempotent dedup of repeated deliveries.
    entry_id: str = ""


@dataclass(frozen=True)
class MergedAsset:
    """The merged view of one asset.

    `provenance` maps every final field name to the source that won the
    field-level conflict resolution, so every field is traceable.
    """

    asset_id: str
    fields: Mapping[str, Any]
    provenance: Mapping[str, str]


@dataclass(frozen=True)
class ChainEntry:
    """One committed entry in the registry's chain-ordered log."""

    height: int
    asset_id: str
    record: AssetRecord


@dataclass(frozen=True)
class ReorgRecord:
    """Describes one applied rollback+replay (chain reorganization)."""

    rolled_back: tuple[ChainEntry, ...]
    replayed: tuple[ChainEntry, ...]
    at_height: int


@dataclass(frozen=True)
class Snapshot:
    """An immutable, fully-committed read view.

    Readers always see exactly one Snapshot; a Snapshot is only published
    atomically after it is completely built, so mixed states are
    impossible by construction.
    """

    version: int
    assets: Mapping[str, MergedAsset]
    entries: tuple[ChainEntry, ...]
    reorgs: tuple[ReorgRecord, ...]
    source_status: Mapping[str, SourceStatus]
    missing_sources: tuple[str, ...]


@dataclass(frozen=True)
class QueryResult:
    """Result of a point query, carrying freshness/degradation info."""

    asset: MergedAsset | None
    snapshot_version: int
    stale: bool
    missing_sources: tuple[str, ...] = ()

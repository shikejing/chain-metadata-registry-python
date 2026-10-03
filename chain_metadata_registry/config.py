"""Configuration for the chain metadata registry.

All settings are explicit and validated. See ``docs/configuration.md`` for the
full reference.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

StalePolicy = Literal["serve_stale", "error"]


@dataclass(frozen=True)
class SourceConfig:
    """Configuration of a single local data source.

    ``priority`` decides field-level conflicts (higher wins). ``order`` is the
    stable arrival-order tie-breaker used when priorities are equal; it must be
    unique across sources so merge results never depend on fetch completion
    order.
    """

    source_id: str
    kind: str  # "file" | "static"
    priority: int
    order: int
    path: str | None = None
    entries: tuple[dict[str, Any], ...] = ()
    timeout_seconds: float = 2.0
    max_retries: int = 2
    retry_backoff_seconds: float = 0.1

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "kind": self.kind,
            "priority": self.priority,
            "order": self.order,
            "path": self.path,
            "entries": list(self.entries),
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "retry_backoff_seconds": self.retry_backoff_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceConfig":
        return cls(
            source_id=data["source_id"],
            kind=data.get("kind", "file"),
            priority=int(data["priority"]),
            order=int(data["order"]),
            path=data.get("path"),
            entries=tuple(data.get("entries", ())),
            timeout_seconds=float(data.get("timeout_seconds", 2.0)),
            max_retries=int(data.get("max_retries", 2)),
            retry_backoff_seconds=float(data.get("retry_backoff_seconds", 0.1)),
        )


@dataclass(frozen=True)
class RefreshConfig:
    interval_seconds: float = 5.0
    freshness_ttl_seconds: float = 30.0
    stale_policy: StalePolicy = "serve_stale"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RefreshConfig":
        return cls(
            interval_seconds=float(data.get("interval_seconds", 5.0)),
            freshness_ttl_seconds=float(data.get("freshness_ttl_seconds", 30.0)),
            stale_policy=data.get("stale_policy", "serve_stale"),
        )


@dataclass(frozen=True)
class ChainConfig:
    """Chain / rollback configuration.

    ``rollback_window_heights`` is the tolerable rollback depth: revocations
    whose height is within ``head_height - window`` are retained and queryable;
    older revocations are garbage collected (``aged_out``). Reads exactly at
    the boundary are deterministic.
    """

    rollback_window_heights: int = 100

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChainConfig":
        return cls(rollback_window_heights=int(data.get("rollback_window_heights", 100)))


@dataclass(frozen=True)
class StoreConfig:
    path: str = ".registry/snapshots"
    keep_snapshots: int = 5

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StoreConfig":
        return cls(
            path=data.get("path", ".registry/snapshots"),
            keep_snapshots=int(data.get("keep_snapshots", 5)),
        )


@dataclass(frozen=True)
class ServiceConfig:
    """Top-level service configuration."""

    sources: tuple[SourceConfig, ...]
    refresh: RefreshConfig = RefreshConfig()
    chain: ChainConfig = ChainConfig()
    store: StoreConfig = StoreConfig()

    def source_order(self) -> tuple[str, ...]:
        return tuple(s.source_id for s in sorted(self.sources, key=lambda s: s.order))

    def source_by_id(self, source_id: str) -> SourceConfig:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise KeyError(f"unknown source: {source_id}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources": [s.to_dict() for s in self.sources],
            "refresh": {
                "interval_seconds": self.refresh.interval_seconds,
                "freshness_ttl_seconds": self.refresh.freshness_ttl_seconds,
                "stale_policy": self.refresh.stale_policy,
            },
            "chain": {"rollback_window_heights": self.chain.rollback_window_heights},
            "store": {"path": self.store.path, "keep_snapshots": self.store.keep_snapshots},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ServiceConfig":
        sources = tuple(SourceConfig.from_dict(s) for s in data.get("sources", []))
        _validate_sources(sources)
        return cls(
            sources=sources,
            refresh=RefreshConfig.from_dict(data.get("refresh", {})),
            chain=ChainConfig.from_dict(data.get("chain", {})),
            store=StoreConfig.from_dict(data.get("store", {})),
        )

    @classmethod
    def load(cls, path: str | Path) -> "ServiceConfig":
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return cls.from_dict(data)


def _validate_sources(sources: tuple[SourceConfig, ...]) -> None:
    seen_ids: set[str] = set()
    seen_orders: set[int] = set()
    for source in sources:
        if not source.source_id:
            raise ValueError("source_id must be non-empty")
        if source.source_id in seen_ids:
            raise ValueError(f"duplicate source_id: {source.source_id}")
        if source.order in seen_orders:
            raise ValueError(f"duplicate source order: {source.order}")
        seen_ids.add(source.source_id)
        seen_orders.add(source.order)
        if source.kind not in ("file", "static"):
            raise ValueError(f"unsupported source kind: {source.kind}")
        if source.kind == "file" and not source.path:
            raise ValueError(f"file source {source.source_id} requires a path")
        if source.timeout_seconds <= 0:
            raise ValueError(f"source {source.source_id} timeout must be positive")
        if source.max_retries < 0:
            raise ValueError(f"source {source.source_id} max_retries must be >= 0")

"""Shared test helpers."""

from __future__ import annotations

import os
import tempfile
from typing import Any

from chain_metadata_registry.config import ServiceConfig
from chain_metadata_registry.engine import DefaultRefreshEngine
from chain_metadata_registry.service import RegistryService
from chain_metadata_registry.store import FileSnapshotStore


def make_config(
    sources: list[dict[str, Any]],
    tmpdir: str,
    *,
    window: int = 100,
    ttl: float = 30.0,
    stale_policy: str = "serve_stale",
    keep_snapshots: int = 5,
) -> ServiceConfig:
    return ServiceConfig.from_dict(
        {
            "sources": sources,
            "chain": {"rollback_window_heights": window},
            "store": {"path": os.path.join(tmpdir, "snapshots"), "keep_snapshots": keep_snapshots},
            "refresh": {
                "interval_seconds": 0,
                "freshness_ttl_seconds": ttl,
                "stale_policy": stale_policy,
            },
        }
    )


def static_source(
    source_id: str,
    priority: int,
    order: int,
    entries: list[dict[str, Any]],
    *,
    timeout: float = 2.0,
    retries: int = 0,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "kind": "static",
        "priority": priority,
        "order": order,
        "entries": entries,
        "timeout_seconds": timeout,
        "max_retries": retries,
    }


def file_source(
    source_id: str,
    priority: int,
    order: int,
    path: str,
    *,
    timeout: float = 2.0,
    retries: int = 0,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "kind": "file",
        "priority": priority,
        "order": order,
        "path": path,
        "timeout_seconds": timeout,
        "max_retries": retries,
    }


def build_engine(config: ServiceConfig) -> tuple[DefaultRefreshEngine, RegistryService, FileSnapshotStore]:
    store = FileSnapshotStore(config.store.path)
    engine = DefaultRefreshEngine(config, store)
    service = RegistryService(config, engine)
    return engine, service, store

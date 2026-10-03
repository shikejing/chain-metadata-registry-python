"""Tests for durable restart recovery and atomic commits."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile

from chain_metadata_registry.engine import DefaultRefreshEngine
from chain_metadata_registry.service import RegistryService
from chain_metadata_registry.store import FileSnapshotStore

from .conftest import build_engine, make_config, static_source


def _e(source_id, entry_id, height, asset_id, **fields):
    return {"entry_id": entry_id, "source_id": source_id, "height": height,
            "asset_id": asset_id, "fields": fields}


def test_restart_loads_committed_snapshot():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", name="X", v=1)])],
            tmp,
        )
        # "Process 1": refresh and commit.
        engine1, service1, store = build_engine(cfg)
        asyncio.run(engine1.refresh())
        snap1 = engine1.current()
        assert snap1 is not None

        # "Process 2": new engine, same store path -> must load committed state.
        engine2 = DefaultRefreshEngine(cfg, FileSnapshotStore(cfg.store.path))
        service2 = RegistryService(cfg, engine2)
        loaded = engine2.current()
        assert loaded is not None
        assert loaded.snapshot_id == snap1.snapshot_id
        assert loaded.head_height == snap1.head_height
        assert "x" in loaded.assets
        assert service2.get_asset("x")["asset"]["fields"]["name"]["value"] == "X"


def test_no_partial_state_on_corrupt_temp_file():
    """A leftover .tmp file must not be mistaken for a committed snapshot."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", name="X")])],
            tmp,
        )
        engine1, _, store = build_engine(cfg)
        asyncio.run(engine1.refresh())

        # Simulate a crashed write: leave a .tmp file with partial JSON.
        tmp_path = store.dir / ".partial.tmp"
        tmp_path.write_text('{"snapshot_id": "partial", "committed_at":')  # truncated

        # New process loads the real committed snapshot, not the .tmp.
        engine2 = DefaultRefreshEngine(cfg, FileSnapshotStore(cfg.store.path))
        loaded = engine2.current()
        assert loaded is not None
        assert loaded.snapshot_id == engine1.current().snapshot_id
        assert "x" in loaded.assets


def test_latest_pointer_fallback_when_missing():
    """If LATEST is missing, fall back to the newest snapshot file."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", name="X")])],
            tmp,
        )
        engine1, _, store = build_engine(cfg)
        asyncio.run(engine1.refresh())
        os.remove(store.latest_path)

        engine2 = DefaultRefreshEngine(cfg, FileSnapshotStore(cfg.store.path))
        loaded = engine2.current()
        assert loaded is not None
        assert "x" in loaded.assets


def test_snapshot_purge_keeps_latest():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp,
            keep_snapshots=2,
        )
        engine1, _, store = build_engine(cfg)
        asyncio.run(engine1.refresh())
        # Two more distinct refreshes (different content -> new snapshots).
        for i in (2, 3):
            cfg2 = make_config(
                [static_source("a", 10, 0, [_e("a", f"a{i}", i, "x", v=i)])],
                tmp,
                keep_snapshots=2,
            )
            e = DefaultRefreshEngine(cfg2, FileSnapshotStore(cfg2.store.path))
            asyncio.run(e.refresh())
        # Only keep_snapshots=2 files remain, and LATEST is loadable.
        ids = store.list_snapshot_ids()
        assert len(ids) <= 2
        latest = store.load_latest()
        assert latest is not None
        assert latest.head_height == 3


def test_committed_snapshot_is_immutable():
    """The in-memory snapshot cannot be mutated by a later refresh."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp,
        )
        engine, _, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        first = engine.current()
        # Refresh with new content.
        cfg2 = make_config(
            [static_source("a", 10, 0, [_e("a", "a2", 2, "x", v=2)])],
            tmp,
        )
        engine2 = DefaultRefreshEngine(cfg2, FileSnapshotStore(cfg2.store.path))
        asyncio.run(engine2.refresh())
        # The old snapshot object is unchanged (immutable).
        assert first.head_height == 1
        assert "a1" in first.entries
        assert first.assets["x"].fields["v"].value == 1

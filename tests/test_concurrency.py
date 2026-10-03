"""Tests for concurrent reads vs refresh: consistent snapshots, no blocking."""

from __future__ import annotations

import asyncio
import tempfile
import threading
import time

from .conftest import build_engine, make_config, static_source


def _e(source_id, entry_id, height, asset_id, **fields):
    return {"entry_id": entry_id, "source_id": source_id, "height": height,
            "asset_id": asset_id, "fields": fields}


def test_concurrent_reads_always_see_complete_snapshot():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())

        errors = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                snap = engine.current()
                if snap is None:
                    continue
                # A complete snapshot must be internally consistent: every
                # field's provenance entry must be present in entries, and the
                # value must match the entry that produced it.
                for asset in snap.assets.values():
                    for prov in asset.fields.values():
                        entry = snap.entries.get(prov.entry_id)
                        if entry is None:
                            errors.append(
                                f"field {prov.field} provenance {prov.entry_id} missing"
                            )
                            continue
                        if entry.fields.get(prov.field) != prov.value:
                            errors.append(
                                f"field {prov.field} value mismatch with entry"
                            )
                    # Every entry height must be <= head.
                    for entry in snap.entries.values():
                        if entry.height > snap.head_height:
                            errors.append(f"entry {entry.entry_id} above head")
                time.sleep(0.0005)

        t = threading.Thread(target=reader)
        t.start()

        # Refresh with changing data many times.
        async def churn():
            for i in range(20):
                cfg2 = make_config(
                    [static_source("a", 10, 0, [_e("a", f"a{i+1}", i + 1, "x", v=(i % 2) + 1)])],
                    tmp,
                )
                from chain_metadata_registry.engine import DefaultRefreshEngine
                from chain_metadata_registry.store import FileSnapshotStore
                e = DefaultRefreshEngine(cfg2, FileSnapshotStore(cfg2.store.path))
                # Share the in-memory current with the reader by reusing engine's
                # store path; instead refresh the same engine with new config.
                engine.config = cfg2
                engine.sources = tuple(
                    type(s)(c) for s, c in zip(engine.sources, cfg2.sources)
                )
                await engine.refresh()
                await asyncio.sleep(0.001)

        asyncio.run(churn())
        stop.set()
        t.join()
        assert errors == []


def test_slow_read_does_not_block_refresh():
    """A reader holding an old snapshot must not delay refresh."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp,
        )
        engine, _, _ = build_engine(cfg)
        asyncio.run(engine.refresh())

        # A "slow reader" holds a reference to the current snapshot and sleeps.
        held = engine.current()
        assert held is not None

        cfg2 = make_config(
            [static_source("a", 10, 0, [_e("a", "a2", 2, "x", v=2)])],
            tmp,
        )
        engine.config = cfg2
        engine.sources = tuple(type(s)(c) for s, c in zip(engine.sources, cfg2.sources))

        async def slow_reader():
            # Simulate a slow read that holds the old snapshot.
            await asyncio.sleep(0.3)
            return held.head_height

        async def refresh_and_time():
            start = time.monotonic()
            await engine.refresh()
            return time.monotonic() - start

        async def main():
            reader_task = asyncio.create_task(slow_reader())
            refresh_task = asyncio.create_task(refresh_and_time())
            elapsed = await refresh_task
            await reader_task
            return elapsed

        elapsed = asyncio.run(main())
        # Refresh must complete promptly even with a slow reader holding old state.
        assert elapsed < 0.2, f"refresh blocked by slow read: {elapsed}s"
        # The held snapshot is unchanged (immutable).
        assert held.head_height == 1
        # The engine has moved on to the new snapshot.
        assert engine.current().head_height == 2

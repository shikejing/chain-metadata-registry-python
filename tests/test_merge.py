"""Tests for multi-source field-level merge, priority, provenance, determinism."""

from __future__ import annotations

import asyncio
import tempfile

from chain_metadata_registry.merger import merge_entries
from chain_metadata_registry.models import Entry

from .conftest import build_engine, make_config, static_source


def _entry(source_id, entry_id, height, asset_id, **fields):
    return {
        "entry_id": entry_id,
        "source_id": source_id,
        "height": height,
        "asset_id": asset_id,
        "fields": fields,
    }


def test_field_level_priority_wins():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [
                static_source("hi", 10, 0, [_entry("hi", "e1", 1, "a1", name="High", symbol="HI")]),
                static_source("lo", 1, 1, [_entry("lo", "e2", 1, "a1", name="Low", decimals=6)]),
            ],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        asset = service.get_asset("a1")["asset"]
        # name: high-priority source wins
        assert asset["fields"]["name"]["value"] == "High"
        assert asset["fields"]["name"]["source_id"] == "hi"
        # symbol only present in hi
        assert asset["fields"]["symbol"]["value"] == "HI"
        # decimals only present in lo
        assert asset["fields"]["decimals"]["value"] == 6
        assert asset["fields"]["decimals"]["source_id"] == "lo"


def test_provenance_traces_every_field():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [
                static_source("a", 5, 0, [_entry("a", "e1", 1, "a1", name="A")]),
                static_source("b", 5, 1, [_entry("b", "e2", 1, "a1", name="B")]),
            ],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        prov = service.get_asset("a1")["asset"]["fields"]["name"]
        # Equal priority: arrival order tie-break -> source a (order 0)
        assert prov["source_id"] == "a"
        assert prov["entry_id"] == "e1"
        assert prov["height"] == 1
        assert "content_hash" in prov


def test_determinism_regardless_of_fetch_order():
    """Merge must not depend on source return order or concurrency."""
    with tempfile.TemporaryDirectory() as tmp:
        sources = [
            static_source("a", 10, 0, [_entry("a", "a1", 1, "x", name="A", v=1)]),
            static_source("b", 5, 1, [_entry("b", "b1", 1, "x", name="B", v=2)]),
            static_source("c", 1, 2, [_entry("c", "c1", 1, "x", name="C", v=3)]),
        ]
        cfg = make_config(sources, tmp)
        engine, _, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        first = engine.current().assets["x"].to_dict()

        # Re-run with sources shuffled (different completion order simulated by
        # shuffling the source config order and re-merging raw entries).
        import random

        rng = random.Random(42)
        shuffled = list(sources)
        rng.shuffle(shuffled)
        cfg2 = make_config(shuffled, tmp)
        engine2, _, _ = build_engine(cfg2)
        asyncio.run(engine2.refresh())
        second = engine2.current().assets["x"].to_dict()
        assert first["content_hash"] == second["content_hash"]
        assert first["fields"] == second["fields"]


def test_identical_entries_dedup_by_identity():
    with tempfile.TemporaryDirectory() as tmp:
        dup = _entry("a", "same", 1, "x", name="Orig")
        cfg = make_config(
            [
                static_source("a", 10, 0, [dup]),
                static_source("b", 5, 1, [_entry("b", "same", 1, "x", name="Orig")]),
            ],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        snap = engine.current()
        # One canonical entry despite two sources reporting it.
        assert len(snap.entries) == 1
        assert "same" in snap.entries
        asset = service.get_asset("x")["asset"]
        assert asset["fields"]["name"]["value"] == "Orig"


def test_idempotent_refresh_no_duplicate_snapshot():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_entry("a", "a1", 1, "x", name="A")])],
            tmp,
        )
        engine, _, store = build_engine(cfg)
        out1 = asyncio.run(engine.refresh())
        out2 = asyncio.run(engine.refresh())
        assert out1.changed is True
        assert out2.changed is False
        assert out1.snapshot.snapshot_id == out2.snapshot.snapshot_id
        # Only one snapshot file on disk.
        assert len(store.list_snapshot_ids()) == 1


def test_incremental_rebuild_only_affected_assets():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [
                static_source(
                    "a", 10, 0,
                    [_entry("a", "a1", 1, "x", name="X"), _entry("a", "a2", 1, "y", name="Y")],
                )
            ],
            tmp,
        )
        engine, _, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        # Change only asset x.
        cfg2 = make_config(
            [static_source("a", 10, 0, [_entry("a", "a1", 1, "x", name="X-changed"), _entry("a", "a2", 1, "y", name="Y")])],
            tmp,
        )
        engine2, _, _ = build_engine(cfg2)
        out = asyncio.run(engine2.refresh())
        assert out.rebuilt_assets == 1
        assert out.carried_assets == 1

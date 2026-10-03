"""Tests for chain rollback / replay semantics."""

from __future__ import annotations

import asyncio
import tempfile

from .conftest import build_engine, make_config, static_source


def _e(source_id, entry_id, height, asset_id, **fields):
    return {"entry_id": entry_id, "source_id": source_id, "height": height,
            "asset_id": asset_id, "fields": fields}


def test_rollback_marks_revoked_and_replacement():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [
                _e("a", "a1", 1, "x", v=1),
                _e("a", "a2", 2, "x", v=2),
            ])],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())

        # Reorg: replace a1/a2 with a1'/a2' and extend to a3' (longer history).
        cfg2 = make_config(
            [static_source("a", 10, 0, [
                _e("a", "a1p", 1, "x", v=10),
                _e("a", "a2p", 2, "x", v=20),
                _e("a", "a3p", 3, "x", v=30),
            ])],
            tmp,
        )
        engine2, service2, _ = build_engine(cfg2)
        out = asyncio.run(engine2.refresh())
        assert out.revoked_count == 2

        for eid in ("a1", "a2"):
            st = service2.entry_status(eid)
            assert st.status == "revoked"
            assert st.revocation is not None
        assert service2.entry_status("a1").revocation.replaced_by == "a1p"
        assert service2.entry_status("a2").revocation.replaced_by == "a2p"
        for eid in ("a1p", "a2p", "a3p"):
            assert service2.entry_status(eid).status == "present"

        # Revocations are queryable.
        rb = service2.list_revocations()
        assert rb["count"] == 2
        assert {r["entry_id"] for r in rb["revocations"]} == {"a1", "a2"}


def test_query_never_mixes_old_and_new_during_rollback():
    """A read must see either the old snapshot or the new one, never a mix."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())

        cfg2 = make_config(
            [static_source("a", 10, 0, [_e("a", "a1p", 1, "x", v=2)])],
            tmp,
        )
        engine2, service2, _ = build_engine(cfg2)

        # Hammer reads concurrently with refresh. Each read must be internally
        # consistent: the asset's field value must match the snapshot it came
        # from (old snapshot -> v=1, new snapshot -> v=2), never a hybrid.
        import threading

        mismatches = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                snap = engine2.current()
                if snap is None:
                    continue
                asset = snap.assets.get("x")
                if asset is None:
                    continue
                v = asset.fields["v"].value
                # The snapshot head/entry must agree with the field value:
                # v=1 -> entry a1 present, a1p absent; v=2 -> vice versa.
                has_a1 = "a1" in snap.entries
                has_a1p = "a1p" in snap.entries
                if v == 1 and not (has_a1 and not has_a1p):
                    mismatches.append(("old", v, has_a1, has_a1p))
                if v == 2 and not (has_a1p and not has_a1):
                    mismatches.append(("new", v, has_a1, has_a1p))

        t = threading.Thread(target=reader)
        t.start()
        asyncio.run(engine2.refresh())
        stop.set()
        t.join()
        assert mismatches == []


def test_rollback_window_boundary_is_deterministic():
    """Revocations within window are retained; beyond window aged out.

    Boundary (head - height == window) must be retained deterministically.
    """
    with tempfile.TemporaryDirectory() as tmp:
        # refresh1: a1@1
        cfg1 = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)])],
            tmp, window=2,
        )
        engine1, service1, _ = build_engine(cfg1)
        asyncio.run(engine1.refresh())
        assert service1.entry_status("a1").status == "present"

        # refresh2: longer chain, a1 revoked and replaced by a3@3. head=3.
        cfg2 = make_config(
            [static_source("a", 10, 0, [_e("a", "a3", 3, "x", v=3)])],
            tmp, window=2,
        )
        engine2, service2, _ = build_engine(cfg2)
        asyncio.run(engine2.refresh())
        # a1 at height 1: 3-1=2 <= 2 -> retained (within window).
        assert service2.entry_status("a1").status == "revoked"
        assert service2.entry_status("a3").status == "present"

        # refresh3: even longer chain, a3 revoked, new a5@5. head=5.
        cfg3 = make_config(
            [static_source("a", 10, 0, [_e("a", "a5", 5, "x", v=5)])],
            tmp, window=2,
        )
        engine3, service3, _ = build_engine(cfg3)
        asyncio.run(engine3.refresh())
        # a1 at height 1: 5-1=4 > 2 -> aged out (unknown).
        assert service3.entry_status("a1").status == "unknown"
        # a3 at height 3: 5-3=2 <= 2 -> retained exactly at boundary.
        assert service3.entry_status("a3").status == "revoked"
        assert service3.entry_status("a5").status == "present"
        # Boundary read is deterministic across repeated reads.
        assert service3.entry_status("a3").status == "revoked"
        assert service3.entry_status("a3").status == "revoked"



def test_degraded_source_entries_carried_forward_not_revoked():
    """A failing source's entries are carried forward and flagged degraded,
    never silently revoked or dropped."""
    with tempfile.TemporaryDirectory() as tmp:
        # refresh1: healthy static source a with a1@1.
        cfg1 = make_config(
            [
                static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)]),
                static_source("b", 5, 1, [_e("b", "b1", 1, "y", v=1)]),
            ],
            tmp,
        )
        engine1, service1, _ = build_engine(cfg1)
        asyncio.run(engine1.refresh())
        assert service1.entry_status("a1").status == "present"

        # refresh2: source a becomes a file source with a missing path (fails).
        from .conftest import file_source
        cfg2 = make_config(
            [
                file_source("a", 10, 0, "/nonexistent/path/entries.json", timeout=0.5, retries=0),
                static_source("b", 5, 1, [_e("b", "b1", 1, "y", v=1)]),
            ],
            tmp,
        )
        engine2, service2, _ = build_engine(cfg2)
        asyncio.run(engine2.refresh())
        # a1 carried forward (still present), snapshot flagged degraded.
        assert service2.entry_status("a1").status == "present"
        assert service2.entry_status("b1").status == "present"
        view = service2.read_view()
        assert view.degraded is True
        assert "a" in [s.source_id for s in view.snapshot.degraded]
        health = service2.health()
        assert health["status"] == "degraded"
        assert health["ok"] is False


def test_reorg_to_shorter_history_revokes_above_new_head():
    """A reorg to a shorter chain revokes entries above the new head."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg1 = make_config(
            [static_source("a", 10, 0, [
                _e("a", "a1", 1, "x", v=1),
                _e("a", "a2", 2, "x", v=2),
                _e("a", "a3", 3, "x", v=3),
            ])],
            tmp,
        )
        engine1, service1, _ = build_engine(cfg1)
        asyncio.run(engine1.refresh())
        assert service1.entry_status("a3").status == "present"

        # Reorg: chain shrinks to height 2 (a3 dropped).
        cfg2 = make_config(
            [static_source("a", 10, 0, [
                _e("a", "a1", 1, "x", v=1),
                _e("a", "a2", 2, "x", v=2),
            ])],
            tmp,
        )
        engine2, service2, _ = build_engine(cfg2)
        out = asyncio.run(engine2.refresh())
        assert service2.entry_status("a3").status == "revoked"
        assert service2.entry_status("a1").status == "present"
        assert service2.entry_status("a2").status == "present"
        assert out.revoked_count == 1

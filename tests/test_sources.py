"""Tests for source timeout, retries, and fault isolation."""

from __future__ import annotations

import asyncio
import tempfile
import time

from chain_metadata_registry.sources import StaticSource

from .conftest import build_engine, make_config, static_source


def _e(source_id, entry_id, height, asset_id, **fields):
    return {"entry_id": entry_id, "source_id": source_id, "height": height,
            "asset_id": asset_id, "fields": fields}


class DelayedStaticSource(StaticSource):
    """Static source that injects a delay to simulate a slow provider."""

    def __init__(self, config, delay: float):
        super().__init__(config)
        self._delay = delay

    async def _load(self):
        await asyncio.sleep(self._delay)
        return await super()._load()


def test_source_timeout_degrades_and_reports_missing():
    with tempfile.TemporaryDirectory() as tmp:
        # Source a is slow (0.5s) but has a 0.1s timeout -> times out.
        cfg = make_config(
            [
                static_source("a", 10, 0, [_e("a", "a1", 1, "x", v=1)], timeout=0.1, retries=1),
                static_source("b", 5, 1, [_e("b", "b1", 1, "y", v=1)], timeout=2.0, retries=0),
            ],
            tmp,
        )
        # Inject delay into source a.
        from chain_metadata_registry.config import ServiceConfig
        engine, service, _ = build_engine(cfg)
        engine.sources = tuple(
            DelayedStaticSource(s.config, delay=0.5) if s.source_id == "a" else s
            for s in engine.sources
        )
        out = asyncio.run(engine.refresh())
        view = service.read_view()
        assert view.degraded is True
        assert "a" in [s.source_id for s in view.snapshot.degraded]
        # b's data still present (fault isolation).
        assert service.entry_status("b1").status == "present"
        # a's entry not present (it timed out and had no prior data).
        assert service.entry_status("a1").status == "unknown"
        # Degradation detail is distinguishable.
        a_status = next(s for s in view.snapshot.degraded if s.source_id == "a")
        assert a_status.timed_out is True
        assert a_status.ok is False


def test_slow_source_does_not_block_fast_source():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [
                static_source("slow", 10, 0, [_e("slow", "s1", 1, "x", v=1)], timeout=2.0, retries=0),
                static_source("fast", 5, 1, [_e("fast", "f1", 1, "y", v=1)], timeout=2.0, retries=0),
            ],
            tmp,
        )
        engine, _, _ = build_engine(cfg)
        # slow takes 0.3s, fast takes 0.2s.
        delays = {"slow": 0.3, "fast": 0.2}
        engine.sources = tuple(
            DelayedStaticSource(s.config, delay=delays[s.source_id]) for s in engine.sources
        )
        start = time.monotonic()
        asyncio.run(engine.refresh())
        elapsed = time.monotonic() - start
        # Concurrent: ~0.3s, not sequential (~0.5s).
        assert elapsed < 0.4, f"sources not isolated: took {elapsed}s"
        assert engine.current() is not None


def test_retry_budget_respected():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x")], timeout=0.05, retries=2)],
            tmp,
        )
        engine, _, _ = build_engine(cfg)
        engine.sources = tuple(
            DelayedStaticSource(s.config, delay=0.3) if s.source_id == "a" else s
            for s in engine.sources
        )
        asyncio.run(engine.refresh())
        a_status = next(s for s in engine.current().degraded if s.source_id == "a")
        # max_retries=2 -> 3 total attempts, retries reported = 2.
        assert a_status.retries == 2
        assert a_status.timed_out is True


def test_healthy_source_not_flagged_degraded():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = make_config(
            [static_source("a", 10, 0, [_e("a", "a1", 1, "x")], timeout=2.0, retries=0)],
            tmp,
        )
        engine, service, _ = build_engine(cfg)
        asyncio.run(engine.refresh())
        view = service.read_view()
        assert view.degraded is False
        assert view.snapshot.degraded == ()

import threading
import time

from registry.config import RegistryConfig, SourceConfig
from registry.models import AssetRecord, SourceStatus
from registry.service import RegistryService


class FakeSource:
    def __init__(self, name, records=(), delay=0.0, error=None):
        self._name = name
        self.records = list(records)
        self.delay = delay
        self.error = error

    @property
    def name(self):
        return self._name

    def fetch(self):
        if self.delay:
            time.sleep(self.delay)
        if self.error:
            raise self.error
        return list(self.records)


def make_service(sources, priorities=None, **cfg_kw):
    cfg = RegistryConfig(
        sources=tuple(SourceConfig(name=n, priority=p)
                      for n, p in (priorities or {}).items()),
        **cfg_kw)
    svc = RegistryService(cfg, sources=sources)
    return svc


def test_multi_source_conflict_resolution_and_provenance():
    svc = make_service(
        [FakeSource("chain", [AssetRecord("ETH", {"name": "Ether", "d": 18},
                                          "chain", "1")]),
         FakeSource("indexer", [AssetRecord("ETH", {"name": "Eth",
                                                    "symbol": "ETH"},
                                            "indexer", "1")])],
        priorities={"chain": 0, "indexer": 1})
    svc.refresh()
    result = svc.get_asset("ETH")
    assert result.asset.fields == {"name": "Ether", "d": 18,
                                   "symbol": "ETH"}
    assert result.asset.provenance["name"] == "chain"
    assert result.asset.provenance["symbol"] == "indexer"
    assert result.missing_sources == ()
    svc.stop()


def test_slow_source_does_not_block_others_and_degrades_explicitly():
    svc = make_service(
        [FakeSource("fast", [AssetRecord("A", {"x": 1}, "fast", "1")]),
         FakeSource("slow", [AssetRecord("B", {"y": 2}, "slow", "1")],
                    delay=2.0)],
        priorities={"fast": 0, "slow": 1},
        # per-source timeout via SourceConfig
    )
    svc._source_configs["slow"] = svc._source_configs["slow"].__class__(
        name="slow", priority=1, timeout_s=0.1, max_retries=0)
    start = time.monotonic()
    svc.refresh()
    elapsed = time.monotonic() - start
    assert elapsed < 1.5  # slow source did not dominate the cycle
    result = svc.get_asset("A")
    assert result.asset is not None
    assert result.missing_sources == ("slow",)
    assert svc.source_status()["slow"] == SourceStatus.TIMEOUT
    assert svc.source_status()["fast"] == SourceStatus.OK
    svc.stop()


def test_failing_source_keeps_previous_committed_data_visible():
    good = [AssetRecord("A", {"x": 1}, "flaky", "1")]
    src = FakeSource("flaky", good)
    svc = make_service([src], priorities={"flaky": 0})
    svc.refresh()
    assert svc.get_asset("A").asset is not None
    src.error = RuntimeError("boom")
    src.records = []
    svc.refresh()
    result = svc.get_asset("A")
    assert result.asset is not None  # old data still served
    assert result.missing_sources == ("flaky",)  # but explicitly flagged
    assert svc.source_status()["flaky"] == SourceStatus.ERROR
    svc.stop()


def test_concurrent_reads_during_refresh_and_reorg_are_consistent():
    svc = make_service(
        [FakeSource("chain", [AssetRecord(f"A{i}", {"v": i}, "chain",
                                          str(i)) for i in range(50)])],
        priorities={"chain": 0})
    svc.refresh()
    errors = []

    def reader():
        try:
            for _ in range(200):
                snap = svc.list_assets()
                # A committed snapshot must be internally consistent:
                # entry heights form a contiguous 0..n-1 sequence.
                heights = [e.height for e in snap.entries]
                assert heights == list(range(len(heights)))
                _ = svc.get_asset("A0")
        except AssertionError as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    # Writer: refresh + reorg while readers run.
    for i in range(5):
        svc.refresh()
        svc.reorg(1, [AssetRecord("Z", {"v": i}, "chain", f"z{i}")])
    for t in threads:
        t.join()
    assert not errors
    svc.stop()


def test_reorg_atomicity_readers_never_see_mixed_entries():
    svc = make_service([FakeSource("chain")], priorities={"chain": 0},
                       rollback_window=16)
    svc.refresh()
    svc._store.append_records([AssetRecord("A", {"v": 0}, "chain", "0")])
    svc.refresh()
    violations = []

    def reader():
        for _ in range(500):
            snap = svc.list_assets()
            ids = [e.record.entry_id for e in snap.entries]
            # Valid states: ["0"] (pre-reorg) or ["r0","r1","r2"]
            # (post-reorg). Anything mixing them is a violation.
            if ids not in (["0"], ["r0", "r1", "r2"]):
                violations.append(ids)

    t = threading.Thread(target=reader)
    t.start()
    for _ in range(20):
        svc.reorg(1, [AssetRecord("B", {"v": 1}, "chain", "r0"),
                      AssetRecord("C", {"v": 2}, "chain", "r1"),
                      AssetRecord("D", {"v": 3}, "chain", "r2")])
        svc.reorg(3, [AssetRecord("A", {"v": 0}, "chain", "0")])
    t.join()
    assert violations == []
    # Rolled-back entries are auditable.
    history = svc.reorg_history()
    assert history
    assert any(e.record.entry_id == "0" for e in history[0].rolled_back)
    svc.stop()


def test_restart_recovery_reuses_committed_snapshot(tmp_path):
    cfg = RegistryConfig(
        sources=(SourceConfig(name="chain", priority=0),),
        state_dir=str(tmp_path))
    src = FakeSource("chain", [AssetRecord("A", {"x": 1}, "chain", "1")])
    svc = RegistryService(cfg, sources=[src])
    svc.refresh()
    v1 = svc.list_assets().version
    svc.stop()

    svc2 = RegistryService(cfg, sources=[FakeSource("chain")])
    snap = svc2.list_assets()
    assert snap.version == v1  # committed snapshot reused, not rebuilt
    assert svc2.get_asset("A").asset.fields == {"x": 1}
    svc2.stop()


def test_staleness_flag():
    svc = make_service([FakeSource("ok", [AssetRecord("A", {"x": 1},
                                                      "ok", "1")])],
                       priorities={"ok": 0}, staleness_ttl_s=0.05)
    svc.refresh()
    assert svc.get_asset("A").stale is False
    time.sleep(0.1)
    assert svc.get_asset("A").stale is True
    svc.stop()

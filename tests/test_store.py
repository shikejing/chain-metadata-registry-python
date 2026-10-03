import pytest

from registry.config import RegistryConfig, WindowExceededPolicy
from registry.models import AssetRecord, SourceStatus
from registry.store import RollbackWindowExceeded, SnapshotStore


def make_store(tmp_path, **kw):
    cfg = RegistryConfig(state_dir=str(tmp_path), **kw)
    return SnapshotStore(cfg)


def rec(asset, source="chain", entry_id="1", **fields):
    return AssetRecord(asset, fields, source, entry_id)


def commit(store, assets=None):
    return store.commit(assets or {}, {"chain": SourceStatus.OK}, ())


def test_append_dedups_by_identity(tmp_path):
    store = make_store(tmp_path)
    added1 = store.append_records([rec("A", entry_id="1")])
    added2 = store.append_records([rec("A", entry_id="1")])  # duplicate
    added3 = store.append_records([rec("A", entry_id="2")])
    assert len(added1) == 1 and added2 == [] and len(added3) == 1
    assert len(store.entries) == 2
    assert [e.height for e in store.entries] == [0, 1]


def test_reorg_rolls_back_and_replays_longer_segment(tmp_path):
    store = make_store(tmp_path, rollback_window=8)
    store.append_records([rec("A", entry_id=str(i)) for i in range(5)])
    commit(store)
    reorg = store.reorg(2, [rec("B", entry_id="n1"), rec("C", entry_id="n2"),
                            rec("D", entry_id="n3")])
    assert [e.record.entry_id for e in reorg.rolled_back] == ["3", "4"]
    assert len(reorg.replayed) == 3
    entries = store.entries
    assert len(entries) == 6  # 3 kept + 3 replayed
    assert [e.height for e in entries] == list(range(6))
    snap = commit(store)
    assert snap.reorgs[-1].rolled_back == reorg.rolled_back


def test_reorg_window_error_policy(tmp_path):
    store = make_store(tmp_path, rollback_window=2,
                       window_exceeded_policy=WindowExceededPolicy.ERROR)
    store.append_records([rec("A", entry_id=str(i)) for i in range(5)])
    commit(store)
    with pytest.raises(RollbackWindowExceeded):
        store.reorg(3, [])
    assert len(store.entries) == 5  # nothing changed


def test_reorg_window_clamp_policy_is_deterministic(tmp_path):
    store = make_store(tmp_path, rollback_window=2,
                       window_exceeded_policy=WindowExceededPolicy.CLAMP)
    store.append_records([rec("A", entry_id=str(i)) for i in range(5)])
    commit(store)
    reorg = store.reorg(4, [rec("B", entry_id="x")])
    assert len(reorg.rolled_back) == 2  # clamped to window
    assert [e.record.entry_id for e in store.entries] == ["0", "1", "2", "x"]


def test_rolled_back_identity_can_be_replayed(tmp_path):
    store = make_store(tmp_path, rollback_window=4)
    store.append_records([rec("A", entry_id="1")])
    commit(store)
    store.reorg(1, [rec("A", entry_id="1", name="fixed")])
    entries = store.entries
    assert len(entries) == 1
    assert entries[0].record.fields["name"] == "fixed"


def test_restart_recovers_committed_snapshot(tmp_path):
    store = make_store(tmp_path)
    store.append_records([rec("A", entry_id="1", name="a")])
    snap1 = commit(store, assets={})
    store.append_records([rec("B", entry_id="2")])
    commit(store)  # uncommitted-tail committed too
    # Simulate restart with a fresh store on the same dir.
    store2 = make_store(tmp_path)
    assert store2.load_persisted() is True
    snap2 = store2.current()
    assert snap2.version == snap1.version + 1
    assert [e.record.entry_id for e in snap2.entries] == ["1", "2"]
    # Idempotency survives restart: re-delivering entry "1" is a no-op.
    assert store2.append_records([rec("A", entry_id="1")]) == []


def test_corrupt_state_file_yields_empty_store(tmp_path):
    store = make_store(tmp_path)
    store.append_records([rec("A", entry_id="1")])
    commit(store)
    (tmp_path / "snapshot.json").write_text("{not json")
    store2 = make_store(tmp_path)
    assert store2.load_persisted() is False
    assert store2.current().entries == ()

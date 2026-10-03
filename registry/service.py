"""Registry service: orchestrates fetch, merge, refresh and queries."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from .config import RegistryConfig
from .merge import merge_asset
from .models import (AssetRecord, MergedAsset, QueryResult, ReorgRecord,
                     Snapshot, SourceStatus)
from .sources import DataSource, JsonFileSource
from .store import SnapshotStore


class RegistryService:
    """Coordinates sources, merge, store and background refresh.

    Concurrency & fault isolation:
      - each source is fetched on a worker thread with its own configured
        timeout and retry budget; a slow or failing source never blocks
        other sources and never touches the query path;
      - queries read only committed immutable snapshots, so a refresh in
        flight is invisible to readers and readers never block refreshes.

    Degradation is explicit: a failed source is listed in
    `missing_sources` and its status is TIMEOUT/ERROR/STALE; previously
    committed data keeps being served, never silently.
    """

    def __init__(self, config: RegistryConfig,
                 sources: list[DataSource] | None = None,
                 store: SnapshotStore | None = None) -> None:
        self._config = config
        if sources is None:
            sources = [JsonFileSource(sc) for sc in config.sources]
        self._sources = list(sources)
        self._source_configs = {sc.name: sc for sc in config.sources}
        self._priorities = {sc.name: sc.priority for sc in config.sources}
        # Sources injected directly (tests) get priorities by order.
        for i, src in enumerate(self._sources):
            self._priorities.setdefault(src.name, i)
        self._store = store or SnapshotStore(config)
        self._store.load_persisted()
        # Working copy of merged assets, rebuilt incrementally and
        # published via store.commit(). Restored from the committed
        # snapshot after a restart.
        self._assets: dict[str, MergedAsset] = dict(
            self._store.current().assets)
        self._source_status: dict[str, SourceStatus] = {
            name: SourceStatus.STALE
            for name in self._priorities
        }
        for name, status in self._store.current().source_status.items():
            self._source_status[name] = status
        self._missing_sources: tuple[str, ...] = ()
        self._last_success_monotonic: float | None = None
        self._write_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, len(self._sources) or 1),
            thread_name_prefix="registry-fetch")
        self._stop_event = threading.Event()
        self._bg_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the background refresh loop (if interval configured)."""
        if self._config.refresh_interval_s <= 0:
            return
        if self._bg_thread is not None:
            return
        self._bg_thread = threading.Thread(
            target=self._refresh_loop, name="registry-refresh",
            daemon=True)
        self._bg_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._bg_thread is not None:
            self._bg_thread.join(timeout=5)
            self._bg_thread = None
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self._config.refresh_interval_s):
            try:
                self.refresh()
            except Exception:
                # A failed cycle must never kill the loop; the previous
                # committed snapshot stays visible and statuses reflect
                # the failure.
                pass

    # ------------------------------------------------------------------
    # refresh
    # ------------------------------------------------------------------
    def _fetch_one(self, source: DataSource) -> list[AssetRecord]:
        """Fetch one source with its configured timeout and retries."""
        cfg = self._source_configs.get(source.name)
        timeout = cfg.timeout_s if cfg else 5.0
        attempts = (cfg.max_retries + 1) if cfg else 1
        last_exc: Exception | None = None
        for _ in range(max(1, attempts)):
            future = self._executor.submit(source.fetch)
            try:
                return future.result(timeout=timeout)
            except FutureTimeout:
                future.cancel()
                last_exc = FutureTimeout(
                    f"source {source.name} timed out after {timeout}s")
            except Exception as exc:  # source bug / IO error
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    def refresh(self) -> Snapshot:
        """Run one refresh cycle: fetch all sources, merge, commit.

        Only assets touched by newly arrived entries are rebuilt. Failed
        sources degrade explicitly; the commit still succeeds for the
        healthy sources.
        """
        with self._write_lock:
            records: list[AssetRecord] = []
            missing: list[str] = []
            any_ok = False
            for source in self._sources:
                try:
                    records.extend(self._fetch_one(source))
                except FutureTimeout:
                    self._source_status[source.name] = SourceStatus.TIMEOUT
                    missing.append(source.name)
                except Exception:
                    self._source_status[source.name] = SourceStatus.ERROR
                    missing.append(source.name)
                else:
                    self._source_status[source.name] = SourceStatus.OK
                    any_ok = True
            added = self._store.append_records(records)
            self._rebuild_assets({e.asset_id for e in added})
            self._missing_sources = tuple(sorted(missing))
            if any_ok:
                self._last_success_monotonic = time.monotonic()
            return self._store.commit(
                self._assets, dict(self._source_status),
                self._missing_sources)

    def _rebuild_assets(self, asset_ids: set[str]) -> None:
        """Incrementally re-merge only the affected assets from the log."""
        if not asset_ids:
            return
        by_asset: dict[str, list[AssetRecord]] = {a: [] for a in asset_ids}
        for entry in self._store.entries:
            if entry.asset_id in by_asset:
                by_asset[entry.asset_id].append(entry.record)
        for asset_id, records in by_asset.items():
            if records:
                self._assets[asset_id] = merge_asset(
                    asset_id, records, self._priorities)
            else:
                self._assets.pop(asset_id, None)

    # ------------------------------------------------------------------
    # reorg
    # ------------------------------------------------------------------
    def reorg(self, rollback_count: int,
              replay_records: list[AssetRecord]) -> ReorgRecord:
        """Apply a chain rollback+replay and rebuild affected assets only.

        The whole operation is one atomic commit: concurrent readers keep
        seeing the previous committed snapshot until the new one is fully
        built, so they never observe a mix of revoked and replayed
        entries.
        """
        with self._write_lock:
            record = self._store.reorg(rollback_count, replay_records)
            affected = {e.asset_id for e in record.rolled_back}
            affected |= {e.asset_id for e in record.replayed}
            self._rebuild_assets(affected)
            self._store.commit(self._assets, dict(self._source_status),
                               self._missing_sources)
            return record

    def reorg_history(self) -> list[ReorgRecord]:
        """Return which entries were rolled back by past reorgs."""
        return list(self._store.current().reorgs)

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def _is_stale(self) -> bool:
        if self._last_success_monotonic is None:
            return True
        return (time.monotonic() - self._last_success_monotonic
                > self._config.staleness_ttl_s)

    def get_asset(self, asset_id: str) -> QueryResult:
        snapshot = self._store.current()
        return QueryResult(
            asset=snapshot.assets.get(asset_id),
            snapshot_version=snapshot.version,
            stale=self._is_stale(),
            missing_sources=snapshot.missing_sources,
        )

    def list_assets(self) -> Snapshot:
        return self._store.current()

    def source_status(self) -> dict[str, SourceStatus]:
        return dict(self._store.current().source_status)

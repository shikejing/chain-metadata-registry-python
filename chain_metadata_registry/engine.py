"""Refresh engine: builds and atomically commits complete snapshots.

The engine fetches all sources concurrently (per-source timeout/retry),
isolates source failures, performs incremental rebuilds (only affected assets
are re-merged), detects chain rollbacks/revocations, applies the rollback
window, and atomically swaps the committed snapshot reference.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Mapping, Protocol

from .config import ServiceConfig
from .merger import merge_entries
from .models import (
    Entry,
    Revocation,
    Snapshot,
    SourceStatus,
    content_hash,
)
from .sources import Source, build_source
from .store import SnapshotStore


class RefreshEngine(Protocol):
    async def refresh(self) -> RefreshOutcome: ...

    def current(self) -> Snapshot | None: ...

    def refresh_sync(self) -> RefreshOutcome: ...


@dataclass(frozen=True)
class RefreshOutcome:
    snapshot: Snapshot
    changed: bool
    rebuilt_assets: int
    carried_assets: int
    revoked_count: int


def _fresh_canonical(
    entries_by_source: Mapping[str, tuple[Entry, ...]],
    config: ServiceConfig,
) -> dict[str, Entry]:
    """Canonical entries from healthy sources (highest-priority wins per id)."""
    rank = {s.source_id: (-s.priority, s.order) for s in config.sources}
    canonical: dict[str, Entry] = {}
    for source_id in sorted(entries_by_source, key=lambda sid: rank.get(sid, (0, 0))):
        for entry in entries_by_source[source_id]:
            if entry.entry_id not in canonical:
                canonical[entry.entry_id] = entry
    return canonical


def _detect_revocations(
    previous: Snapshot | None,
    fresh_canonical: Mapping[str, Entry],
    healthy_sources: set[str],
    refresh_seq: int,
) -> dict[str, Revocation]:
    """Find previously-committed entries revoked by this refresh.

    An entry is revoked when the source that produced it was fetched
    successfully and the entry no longer appears in the canonical chain.
    Entries from degraded sources are never revoked (they are carried forward
    with a degradation flag). A healthy source that drops an entry signals a
    chain reorg (including a reorg to a shorter history).
    """
    if previous is None:
        return {}
    fresh_by_height: dict[int, list[Entry]] = {}
    for entry in fresh_canonical.values():
        fresh_by_height.setdefault(entry.height, []).append(entry)

    revoked: dict[str, Revocation] = {}
    for entry_id, old in previous.entries.items():
        if entry_id in fresh_canonical:
            continue
        if old.source_id not in healthy_sources:
            continue  # source degraded: keep, flag degradation instead
        replaced_by: str | None = None
        for candidate in fresh_by_height.get(old.height, []):
            if candidate.entry_id != entry_id:
                replaced_by = candidate.entry_id
                break
        revoked[entry_id] = Revocation(
            entry_id=entry_id,
            source_id=old.source_id,
            height=old.height,
            asset_id=old.asset_id,
            revoked_at_refresh=refresh_seq,
            replaced_by=replaced_by,
            content_hash=old.content_hash,
        )
    return revoked


def _apply_window(
    revocations: Mapping[str, Revocation], head_height: int, window: int
) -> dict[str, Revocation]:
    """Keep revocations within the rollback window.

    Retained when ``head_height - height <= window``. Reads exactly at the
    boundary are deterministic.
    """
    return {
        entry_id: rev
        for entry_id, rev in revocations.items()
        if head_height - rev.height <= window
    }


def _snapshot_id(
    head_height: int,
    entries: Mapping[str, Entry],
    assets: Mapping[str, str],
    degraded: tuple[SourceStatus, ...],
    window: int,
    source_order: tuple[str, ...],
) -> str:
    return content_hash(
        {
            "head_height": head_height,
            "entries": {eid: e.content_hash for eid, e in sorted(entries.items())},
            "assets": {aid: ahash for aid, ahash in sorted(assets.items())},
            "degraded": [s.source_id for s in degraded],
            "window_heights": window,
            "source_order": list(source_order),
        }
    )


class DefaultRefreshEngine:
    def __init__(self, config: ServiceConfig, store: SnapshotStore) -> None:
        self.config = config
        self.store = store
        self.sources: tuple[Source, ...] = tuple(build_source(s) for s in config.sources)
        self._current: Snapshot | None = store.load_latest()

    def current(self) -> Snapshot | None:
        return self._current

    async def _fetch_all(self) -> tuple[dict[str, tuple[Entry, ...]], dict[str, SourceStatus]]:
        """Fetch all sources concurrently; never let one failure escape."""
        tasks = [asyncio.create_task(source.fetch()) for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        entries_by_source: dict[str, tuple[Entry, ...]] = {}
        statuses: dict[str, SourceStatus] = {}
        for source, result in zip(self.sources, results):
            if isinstance(result, Exception):
                status = SourceStatus(
                    source_id=source.source_id,
                    ok=False,
                    timed_out=False,
                    retries=0,
                    error=f"{type(result).__name__}: {result}",
                    entries_returned=0,
                    head_height=None,
                )
                entries_by_source[source.source_id] = ()
            else:
                status = result.status
                entries_by_source[source.source_id] = result.entries
            statuses[source.source_id] = status
        return entries_by_source, statuses

    async def refresh(self) -> RefreshOutcome:
        previous = self._current
        refresh_seq = (previous.refresh_seq + 1) if previous else 1

        entries_by_source, statuses = await self._fetch_all()
        healthy = {sid for sid, status in statuses.items() if status.ok}
        degraded = tuple(
            statuses[sid] for sid in self.config.source_order() if not statuses[sid].ok
        )

        # Carry forward entries from degraded sources so data is not silently
        # lost; the degradation flag tells readers which sources are missing.
        if previous is not None:
            for source_id in self.config.source_order():
                if source_id in healthy:
                    continue
                carried = tuple(
                    entry
                    for entry in previous.entries.values()
                    if entry.source_id == source_id
                )
                if carried:
                    entries_by_source[source_id] = carried

        fresh_canonical = _fresh_canonical(entries_by_source, self.config)
        new_revocations = _detect_revocations(previous, fresh_canonical, healthy, refresh_seq)

        merged = merge_entries(entries_by_source, statuses, self.config)

        prior_revocations = dict(previous.revoked) if previous else {}
        all_revocations = {**prior_revocations, **new_revocations}
        window = self.config.chain.rollback_window_heights
        kept_revocations = _apply_window(all_revocations, merged.head_height, window)

        assets_content = {aid: asset.content_hash for aid, asset in merged.assets.items()}
        snapshot_id = _snapshot_id(
            merged.head_height,
            merged.entries,
            assets_content,
            degraded,
            window,
            self.config.source_order(),
        )

        # Incremental stats: an asset is carried when its merged content hash
        # is unchanged; otherwise it was rebuilt.
        rebuilt = 0
        carried = 0
        for aid, asset in merged.assets.items():
            if previous is not None and aid in previous.assets:
                if previous.assets[aid].content_hash == asset.content_hash:
                    carried += 1
                else:
                    rebuilt += 1
            else:
                rebuilt += 1

        # Idempotency: identical content reuses the prior commit timestamp and
        # does not create a duplicate snapshot.
        unchanged = previous is not None and previous.snapshot_id == snapshot_id
        committed_at = previous.committed_at if unchanged else time.time()

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            refresh_seq=refresh_seq,
            committed_at=committed_at,
            head_height=merged.head_height,
            assets=merged.assets,
            entries=merged.entries,
            revoked=kept_revocations,
            degraded=degraded,
            window_heights=window,
            source_order=self.config.source_order(),
        )

        changed = not unchanged
        if changed:
            self.store.save(snapshot)
            self.store.purge(self.config.store.keep_snapshots)
        self._current = snapshot

        return RefreshOutcome(
            snapshot=snapshot,
            changed=changed,
            rebuilt_assets=rebuilt,
            carried_assets=carried,
            revoked_count=len(new_revocations),
        )

    def refresh_sync(self) -> RefreshOutcome:
        """Synchronous convenience wrapper."""
        return asyncio.run(self.refresh())

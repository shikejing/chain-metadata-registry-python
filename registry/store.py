"""Versioned, persistent, MVCC snapshot store with reorg support."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from types import MappingProxyType
from typing import Iterable

from .config import RegistryConfig, WindowExceededPolicy
from .models import (AssetRecord, ChainEntry, MergedAsset, ReorgRecord,
                     Snapshot, SourceStatus)


class RollbackWindowExceeded(Exception):
    """Raised when a reorg is deeper than the configured window and the
    policy is ERROR."""

    def __init__(self, requested: int, window: int) -> None:
        super().__init__(
            f"rollback of {requested} entries exceeds window of {window}")
        self.requested = requested
        self.window = window


def _immutable_assets(assets: dict[str, MergedAsset]) -> MappingProxyType:
    return MappingProxyType({
        aid: MergedAsset(
            asset_id=m.asset_id,
            fields=MappingProxyType(dict(m.fields)),
            provenance=MappingProxyType(dict(m.provenance)),
        )
        for aid, m in assets.items()
    })


class SnapshotStore:
    """Holds the committed chain log and publishes immutable snapshots.

    - Readers call `current()` and get a fully-built immutable Snapshot;
      publication is a single atomic reference swap, so readers never see
      mixed state and never block writers (MVCC).
    - Writers are serialized by an internal lock; a slow reader holding an
      old Snapshot never blocks a commit.
    - Committed snapshots are persisted to `state_dir` with a
      write-temp-then-rename protocol, so a restart either reloads the
      complete last committed state or nothing — never a partial state.
    """

    STATE_FILE = "snapshot.json"

    def __init__(self, config: RegistryConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._version = 0
        self._entries: list[ChainEntry] = []
        self._reorgs: list[ReorgRecord] = []
        self._seen_ids: set[tuple[str, str]] = set()
        self._snapshot = self._build_snapshot({}, {}, ())

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def current(self) -> Snapshot:
        """Return the latest committed snapshot (never blocks writers)."""
        return self._snapshot

    @property
    def entries(self) -> list[ChainEntry]:
        with self._lock:
            return list(self._entries)

    # ------------------------------------------------------------------
    # log manipulation (writer path, does not publish by itself)
    # ------------------------------------------------------------------
    def append_records(self, records: Iterable[AssetRecord]) -> list[ChainEntry]:
        """Append records to the pending log, deduplicating by identity
        (source, entry_id). Returns the entries actually added."""
        added: list[ChainEntry] = []
        with self._lock:
            for record in records:
                identity = (record.source, record.entry_id)
                if identity in self._seen_ids:
                    continue  # idempotent: repeated delivery is a no-op
                self._seen_ids.add(identity)
                entry = ChainEntry(
                    height=(self._entries[-1].height + 1
                            if self._entries else 0),
                    asset_id=record.asset_id,
                    record=record,
                )
                self._entries.append(entry)
                added.append(entry)
        return added

    def reorg(self, rollback_count: int,
              replay: Iterable[AssetRecord]) -> ReorgRecord:
        """Roll back the newest `rollback_count` entries and replay new ones.

        Window semantics: a rollback deeper than `rollback_window` is
        either rejected (policy ERROR, nothing changes) or clamped to the
        window (policy CLAMP, entries older than the window are final).
        The replayed segment may be longer than the rolled-back segment.
        """
        with self._lock:
            window = self._config.rollback_window
            if rollback_count > window:
                if self._config.window_exceeded_policy is \
                        WindowExceededPolicy.ERROR:
                    raise RollbackWindowExceeded(rollback_count, window)
                rollback_count = window
            rollback_count = min(rollback_count, len(self._entries))
            rolled_back = tuple(
                self._entries[len(self._entries) - rollback_count:]
            ) if rollback_count else ()
            if rollback_count:
                del self._entries[len(self._entries) - rollback_count:]
            # Rolled-back identities become reusable: the replacement
            # history is a different segment and must not be deduped
            # against the revoked one.
            for entry in rolled_back:
                self._seen_ids.discard(
                    (entry.record.source, entry.record.entry_id))
            replayed: list[ChainEntry] = []
            for record in replay:
                identity = (record.source, record.entry_id)
                if identity in self._seen_ids:
                    continue
                self._seen_ids.add(identity)
                entry = ChainEntry(
                    height=(self._entries[-1].height + 1
                            if self._entries else 0),
                    asset_id=record.asset_id,
                    record=record,
                )
                self._entries.append(entry)
                replayed.append(entry)
            record = ReorgRecord(
                rolled_back=rolled_back,
                replayed=tuple(replayed),
                at_height=(self._entries[-1].height
                           if self._entries else -1),
            )
            self._reorgs.append(record)
            return record

    # ------------------------------------------------------------------
    # commit / publish
    # ------------------------------------------------------------------
    def commit(self, assets: dict[str, MergedAsset],
               source_status: dict[str, SourceStatus],
               missing_sources: tuple[str, ...]) -> Snapshot:
        """Atomically publish a new fully-built snapshot and persist it."""
        with self._lock:
            self._version += 1
            snapshot = self._build_snapshot(assets, source_status,
                                            missing_sources)
            self._persist(snapshot, assets)
            self._snapshot = snapshot
            return snapshot

    def _build_snapshot(self, assets, source_status, missing) -> Snapshot:
        return Snapshot(
            version=self._version,
            assets=_immutable_assets(assets),
            entries=tuple(self._entries),
            reorgs=tuple(self._reorgs),
            source_status=MappingProxyType(dict(source_status)),
            missing_sources=tuple(missing),
        )

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _persist(self, snapshot: Snapshot,
                 assets: dict[str, MergedAsset]) -> None:
        if not self._config.state_dir:
            return
        path = Path(self._config.state_dir)
        path.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": snapshot.version,
            "entries": [
                {"height": e.height, "asset_id": e.asset_id,
                 "record": {"asset_id": e.record.asset_id,
                            "fields": dict(e.record.fields),
                            "source": e.record.source,
                            "entry_id": e.record.entry_id}}
                for e in snapshot.entries
            ],
            "reorgs": [
                {"rolled_back": [self._entry_to_json(e)
                                 for e in r.rolled_back],
                 "replayed": [self._entry_to_json(e) for e in r.replayed],
                 "at_height": r.at_height}
                for r in snapshot.reorgs
            ],
            "assets": {
                aid: {"fields": dict(m.fields),
                      "provenance": dict(m.provenance)}
                for aid, m in assets.items()
            },
            "source_status": {k: v.value for k, v in
                              snapshot.source_status.items()},
            "missing_sources": list(snapshot.missing_sources),
            "seen_ids": sorted([list(t) for t in self._seen_ids]),
        }
        tmp = path / (self.STATE_FILE + ".tmp")
        final = path / self.STATE_FILE
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp, final)  # atomic: all-or-nothing publication

    @staticmethod
    def _entry_to_json(e: ChainEntry) -> dict:
        return {"height": e.height, "asset_id": e.asset_id,
                "record": {"asset_id": e.record.asset_id,
                           "fields": dict(e.record.fields),
                           "source": e.record.source,
                           "entry_id": e.record.entry_id}}

    @staticmethod
    def _entry_from_json(d: dict) -> ChainEntry:
        r = d["record"]
        return ChainEntry(
            height=d["height"],
            asset_id=d["asset_id"],
            record=AssetRecord(asset_id=r["asset_id"], fields=r["fields"],
                               source=r["source"], entry_id=r["entry_id"]),
        )

    def load_persisted(self) -> bool:
        """Reload the last committed snapshot from disk, if present.

        Returns True when a complete committed state was restored. A
        missing or corrupt file leaves the store empty (never partial).
        """
        if not self._config.state_dir:
            return False
        final = Path(self._config.state_dir) / self.STATE_FILE
        if not final.exists():
            return False
        try:
            with open(final, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            entries = [self._entry_from_json(d) for d in payload["entries"]]
            reorgs = [
                ReorgRecord(
                    rolled_back=tuple(self._entry_from_json(e)
                                      for e in r["rolled_back"]),
                    replayed=tuple(self._entry_from_json(e)
                                   for e in r["replayed"]),
                    at_height=r["at_height"],
                )
                for r in payload["reorgs"]
            ]
            assets = {
                aid: MergedAsset(asset_id=aid, fields=a["fields"],
                                 provenance=a["provenance"])
                for aid, a in payload["assets"].items()
            }
            source_status = {k: SourceStatus(v) for k, v in
                             payload["source_status"].items()}
            missing = tuple(payload["missing_sources"])
            seen = {tuple(t) for t in payload["seen_ids"]}
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return False
        with self._lock:
            self._version = payload["version"]
            self._entries = entries
            self._reorgs = reorgs
            self._seen_ids = seen
            self._snapshot = self._build_snapshot(assets, source_status,
                                                  missing)
        return True

"""Atomic, durable snapshot storage.

Snapshots are immutable and content-addressed. Commits write to a temp file,
fsync, then atomically rename; the ``LATEST`` pointer is swapped the same way,
so a process restart never observes a partial state. Old snapshots are purged
according to ``keep_snapshots``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Protocol

from .models import Snapshot


class SnapshotStore(Protocol):
    def save(self, snapshot: Snapshot) -> None: ...

    def load_latest(self) -> Snapshot | None: ...

    def load(self, snapshot_id: str) -> Snapshot | None: ...

    def purge(self, keep: int) -> None: ...


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically: temp file + fsync + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, sort_keys=True, ensure_ascii=False)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    # fsync the directory so the rename itself is durable.
    try:
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


class FileSnapshotStore:
    def __init__(self, path: str) -> None:
        self.dir = Path(path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.latest_path = self.dir / "LATEST"

    def _snapshot_path(self, snapshot_id: str) -> Path:
        # snapshot_id is a content hash: safe as a filename.
        return self.dir / f"{snapshot_id}.json"

    def save(self, snapshot: Snapshot) -> None:
        path = self._snapshot_path(snapshot.snapshot_id)
        if not path.exists():
            _atomic_write_json(path, snapshot.to_dict())
        _atomic_write_json(self.latest_path, {"snapshot_id": snapshot.snapshot_id})

    def load(self, snapshot_id: str) -> Snapshot | None:
        path = self._snapshot_path(snapshot_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return Snapshot.from_dict(data)
        except (OSError, ValueError, KeyError):
            return None

    def load_latest(self) -> Snapshot | None:
        snapshot_id: str | None = None
        if self.latest_path.exists():
            try:
                with open(self.latest_path, "r", encoding="utf-8") as fh:
                    snapshot_id = json.load(fh).get("snapshot_id")
            except (OSError, ValueError):
                snapshot_id = None
        if snapshot_id:
            snapshot = self.load(snapshot_id)
            if snapshot is not None:
                return snapshot
        # Fallback: newest snapshot file by mtime (LATEST may be missing).
        candidates = sorted(
            self.dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        for candidate in candidates:
            snapshot = self.load(candidate.stem)
            if snapshot is not None:
                return snapshot
        return None

    def list_snapshot_ids(self) -> list[str]:
        return [p.stem for p in self.dir.glob("*.json")]

    def purge(self, keep: int) -> None:
        """Keep the ``keep`` newest snapshots (by refresh_seq); never delete LATEST."""
        snapshots: list[Snapshot] = []
        for snapshot_id in self.list_snapshot_ids():
            snapshot = self.load(snapshot_id)
            if snapshot is not None:
                snapshots.append(snapshot)
        snapshots.sort(key=lambda s: s.refresh_seq, reverse=True)
        latest_id: str | None = None
        if self.latest_path.exists():
            try:
                latest_id = json.loads(self.latest_path.read_text()).get("snapshot_id")
            except (OSError, ValueError):
                latest_id = None
        for stale in snapshots[keep:]:
            if stale.snapshot_id == latest_id:
                continue
            try:
                self._snapshot_path(stale.snapshot_id).unlink()
            except OSError:
                pass

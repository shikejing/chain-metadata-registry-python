"""Local data sources.

A source is a local, configurable provider of chain entries. Sources are
fetched concurrently by the refresh engine; each source has its own timeout and
retry budget so a slow or failing source cannot block the others.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Protocol

from .config import SourceConfig
from .models import Entry, SourceStatus, content_hash


@dataclass(frozen=True)
class SourceFetch:
    """Result of fetching a single source."""

    status: SourceStatus
    entries: tuple[Entry, ...]


class Source(Protocol):
    source_id: str
    priority: int
    order: int

    async def fetch(self) -> SourceFetch:
        """Fetch the source's current entries (with timeout + retries)."""
        ...


def _entry_from_raw(source_id: str, raw: dict[str, Any]) -> Entry:
    """Build an Entry from a raw source record, deriving identity if absent."""
    height = int(raw["height"])
    asset_id = str(raw["asset_id"])
    fields = dict(raw.get("fields", {}))
    entry_id = raw.get("entry_id")
    if not entry_id:
        entry_id = content_hash(
            {"source_id": source_id, "height": height, "asset_id": asset_id, "fields": fields}
        )
    return Entry(
        entry_id=str(entry_id),
        source_id=source_id,
        height=height,
        asset_id=asset_id,
        fields=fields,
    )


def _parse_entries(source_id: str, raw_entries: list[dict[str, Any]]) -> tuple[Entry, ...]:
    entries = [_entry_from_raw(source_id, raw) for raw in raw_entries]
    # De-duplicate by identity: identical entries collapse to one; the first
    # arrival (sources are read in their configured order) wins.
    seen: dict[str, Entry] = {}
    for entry in entries:
        if entry.entry_id not in seen:
            seen[entry.entry_id] = entry
    return tuple(seen.values())


def _head_height(entries: tuple[Entry, ...]) -> int | None:
    return max((e.height for e in entries), default=None)


class FileSource:
    """Entries loaded from a local JSON file.

    The file contains a JSON array of entry records. Reads run in a worker
    thread so a slow disk never blocks the event loop or other sources.
    """

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self.source_id = config.source_id
        self.priority = config.priority
        self.order = config.order

    def _read(self) -> list[dict[str, Any]]:
        with open(self.config.path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            raise ValueError(f"source {self.source_id}: expected a JSON array of entries")
        return data

    async def _load(self) -> tuple[Entry, ...]:
        raw = await asyncio.to_thread(self._read)
        return _parse_entries(self.source_id, raw)

    async def fetch(self) -> SourceFetch:
        attempts = 0
        last_error: str | None = None
        timed_out = False
        while attempts <= self.config.max_retries:
            try:
                entries = await asyncio.wait_for(self._load(), timeout=self.config.timeout_seconds)
                return SourceFetch(
                    status=SourceStatus(
                        source_id=self.source_id,
                        ok=True,
                        timed_out=False,
                        retries=attempts,
                        entries_returned=len(entries),
                        head_height=_head_height(entries),
                    ),
                    entries=entries,
                )
            except asyncio.TimeoutError:
                timed_out = True
                last_error = f"timeout after {self.config.timeout_seconds}s"
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            attempts += 1
            if attempts <= self.config.max_retries:
                await asyncio.sleep(self.config.retry_backoff_seconds)
        return SourceFetch(
            status=SourceStatus(
                source_id=self.source_id,
                ok=False,
                timed_out=timed_out,
                retries=self.config.max_retries,
                error=last_error or "unknown error",
                entries_returned=0,
                head_height=None,
            ),
            entries=(),
        )


class StaticSource:
    """Entries supplied inline in configuration (useful for tests)."""

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self.source_id = config.source_id
        self.priority = config.priority
        self.order = config.order

    async def _load(self) -> tuple[Entry, ...]:
        return _parse_entries(self.source_id, list(self.config.entries))

    async def fetch(self) -> SourceFetch:
        attempts = 0
        last_error: str | None = None
        timed_out = False
        while attempts <= self.config.max_retries:
            try:
                entries = await asyncio.wait_for(self._load(), timeout=self.config.timeout_seconds)
                return SourceFetch(
                    status=SourceStatus(
                        source_id=self.source_id,
                        ok=True,
                        timed_out=False,
                        retries=attempts,
                        entries_returned=len(entries),
                        head_height=_head_height(entries),
                    ),
                    entries=entries,
                )
            except asyncio.TimeoutError:
                timed_out = True
                last_error = f"timeout after {self.config.timeout_seconds}s"
            except (ValueError, KeyError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            attempts += 1
            if attempts <= self.config.max_retries:
                await asyncio.sleep(self.config.retry_backoff_seconds)
        return SourceFetch(
            status=SourceStatus(
                source_id=self.source_id,
                ok=False,
                timed_out=timed_out,
                retries=self.config.max_retries,
                error=last_error or "unknown error",
                entries_returned=0,
                head_height=None,
            ),
            entries=(),
        )


def build_source(config: SourceConfig) -> Source:
    if config.kind == "file":
        return FileSource(config)
    if config.kind == "static":
        return StaticSource(config)
    raise ValueError(f"unsupported source kind: {config.kind}")

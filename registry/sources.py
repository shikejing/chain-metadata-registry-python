"""Data source abstraction and a local JSON-file source."""

from __future__ import annotations

import json
from typing import Protocol

from .config import SourceConfig
from .models import AssetRecord


class DataSource(Protocol):
    """A source of raw asset records."""

    @property
    def name(self) -> str: ...

    def fetch(self) -> list[AssetRecord]:
        """Return the current full view of the source. May raise."""
        ...


class JsonFileSource:
    """Reads records from a local JSON file.

    File format: a list of objects, each with "asset_id", "fields"
    (object) and optional "entry_id".
    """

    def __init__(self, config: SourceConfig) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self._config.name

    def fetch(self) -> list[AssetRecord]:
        with open(self._config.path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, list):
            raise ValueError(f"{self.name}: expected a JSON list")
        records: list[AssetRecord] = []
        for i, item in enumerate(raw):
            records.append(AssetRecord(
                asset_id=str(item["asset_id"]),
                fields=dict(item.get("fields", {})),
                source=self.name,
                entry_id=str(item.get("entry_id", f"{self.name}-{i}")),
            ))
        return records

"""Deterministic, field-level merging of source entries.

Merge rules (see docs/merge-semantics.md):

* candidates are sorted deterministically by (priority desc, source order asc,
  height desc, entry_id asc);
* each field takes the first present value in that order, and every merged
  field carries provenance back to its source;
* identical entries are de-duplicated by identity; the result never depends on
  fetch completion order or concurrency.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Mapping

from .config import ServiceConfig
from .models import (
    Entry,
    FieldProvenance,
    MergedAsset,
    SourceStatus,
    content_hash,
)


@dataclass(frozen=True)
class MergeResult:
    assets: Mapping[str, MergedAsset]
    entries: Mapping[str, Entry]
    head_height: int


def _source_rank(config: ServiceConfig) -> dict[str, tuple[int, int]]:
    """Return source_id -> (-priority, order) deterministic rank."""
    return {s.source_id: (-s.priority, s.order) for s in config.sources}


def merge_entries(
    entries_by_source: Mapping[str, tuple[Entry, ...]],
    source_statuses: Mapping[str, SourceStatus],
    config: ServiceConfig,
) -> MergeResult:
    """Merge fetched entries into canonical assets.

    ``entries_by_source`` maps source_id to the entries it produced (fresh
    entries for healthy sources, carried-forward entries for degraded ones).
    Canonical entries are de-duplicated by identity; conflicting claims for the
    same entry_id resolve to the highest-priority source.
    """
    rank = _source_rank(config)

    # 1. Canonical entries: highest-priority source wins per entry_id.
    canonical: dict[str, Entry] = {}
    for source_id in sorted(entries_by_source, key=lambda sid: rank.get(sid, (0, 0))):
        for entry in entries_by_source[source_id]:
            if entry.entry_id not in canonical:
                canonical[entry.entry_id] = entry

    # 2. Group canonical entries by asset.
    by_asset: dict[str, list[Entry]] = defaultdict(list)
    for entry in canonical.values():
        by_asset[entry.asset_id].append(entry)

    # 3. Field-level merge per asset, fully deterministic.
    assets: dict[str, MergedAsset] = {}
    for asset_id in sorted(by_asset):
        candidates = sorted(
            by_asset[asset_id],
            key=lambda e: (rank.get(e.source_id, (0, 0)), -e.height, e.entry_id),
        )
        fields: dict[str, FieldProvenance] = {}
        for entry in candidates:
            for name, value in entry.fields.items():
                if name in fields:
                    continue  # higher-priority claim already won
                fields[name] = FieldProvenance(
                    field=name,
                    value=value,
                    source_id=entry.source_id,
                    source_priority=config.source_by_id(entry.source_id).priority,
                    entry_id=entry.entry_id,
                    height=entry.height,
                    content_hash=entry.content_hash,
                )
        source_ids = sorted({e.source_id for e in candidates})
        entry_ids = sorted({e.entry_id for e in candidates})
        last_height = max(e.height for e in candidates)
        asset_hash = content_hash(
            {
                "asset_id": asset_id,
                "fields": {
                    name: {
                        "value": prov.value,
                        "source_id": prov.source_id,
                        "entry_id": prov.entry_id,
                        "height": prov.height,
                    }
                    for name, prov in sorted(fields.items())
                },
            }
        )
        assets[asset_id] = MergedAsset(
            asset_id=asset_id,
            fields=dict(sorted(fields.items())),
            source_ids=tuple(source_ids),
            entry_ids=tuple(entry_ids),
            last_height=last_height,
            content_hash=asset_hash,
        )

    head_height = max((e.height for e in canonical.values()), default=0)
    return MergeResult(
        assets=assets,
        entries=dict(sorted(canonical.items())),
        head_height=head_height,
    )

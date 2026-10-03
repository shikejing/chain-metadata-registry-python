"""Deterministic field-level multi-source merge."""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from .models import AssetRecord, MergedAsset


def merge_asset(asset_id: str, records: Sequence[AssetRecord],
                priority_of: Mapping[str, int]) -> MergedAsset:
    """Merge all records for one asset, field by field.

    Resolution rules (deterministic, independent of arrival order and
    concurrency):
      1. Within one source, the record with the largest entry_id is the
         newest and wins; duplicates with equal entry_id are identical by
         identity and collapse to one.
      2. Across sources, the source with the lowest priority number wins
         each field.
    """
    # Step 1: per source, keep only the newest record (max entry_id).
    newest_per_source: dict[str, AssetRecord] = {}
    for record in records:
        current = newest_per_source.get(record.source)
        if current is None or record.entry_id >= current.entry_id:
            newest_per_source[record.source] = record

    # Step 2: order sources by priority so input iteration order cannot
    # affect the outcome; first writer of a field wins.
    ordered = sorted(
        newest_per_source.values(),
        key=lambda r: (priority_of.get(r.source, 1 << 30), r.source),
    )
    fields: dict[str, object] = {}
    provenance: dict[str, str] = {}
    for record in ordered:
        for key, value in record.fields.items():
            if key not in fields:
                fields[key] = value
                provenance[key] = record.source
    return MergedAsset(asset_id=asset_id, fields=fields,
                       provenance=provenance)


def merge_all(records: Iterable[AssetRecord],
              priority_of: Mapping[str, int]) -> dict[str, MergedAsset]:
    """Group records by asset and merge each group."""
    by_asset: dict[str, list[AssetRecord]] = {}
    for record in records:
        by_asset.setdefault(record.asset_id, []).append(record)
    return {
        asset_id: merge_asset(asset_id, recs, priority_of)
        for asset_id, recs in sorted(by_asset.items())
    }

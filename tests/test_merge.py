import itertools

from registry.merge import merge_all, merge_asset
from registry.models import AssetRecord

PRIORITIES = {"chain": 0, "indexer": 1, "community": 2}


def rec(source, fields, entry_id="1"):
    return AssetRecord(asset_id="ETH", fields=fields, source=source,
                       entry_id=entry_id)


def test_field_level_priority_and_provenance():
    records = [
        rec("community", {"name": "Ether?", "website": "w"}),
        rec("chain", {"name": "Ether", "decimals": 18}),
        rec("indexer", {"name": "Eth", "symbol": "ETH"}),
    ]
    merged = merge_asset("ETH", records, PRIORITIES)
    assert merged.fields == {
        "name": "Ether",        # chain wins over indexer/community
        "decimals": 18,         # only chain has it
        "symbol": "ETH",        # only indexer has it
        "website": "w",         # only community has it
    }
    assert merged.provenance == {
        "name": "chain",
        "decimals": "chain",
        "symbol": "indexer",
        "website": "community",
    }


def test_result_independent_of_arrival_order():
    base = [
        rec("chain", {"name": "Ether", "decimals": 18}, "1"),
        rec("indexer", {"name": "Eth", "symbol": "ETH"}, "2"),
        rec("community", {"name": "Ether?", "website": "w"}, "3"),
    ]
    results = set()
    for perm in itertools.permutations(base):
        merged = merge_asset("ETH", list(perm), PRIORITIES)
        results.add((tuple(sorted(merged.fields.items())),
                     tuple(sorted(merged.provenance.items()))))
    assert len(results) == 1


def test_newest_entry_wins_within_source_and_duplicates_dedup():
    records = [
        rec("chain", {"name": "old"}, entry_id="1"),
        rec("chain", {"name": "old"}, entry_id="1"),  # duplicate delivery
        rec("chain", {"name": "new"}, entry_id="2"),
    ]
    merged = merge_asset("ETH", records, PRIORITIES)
    assert merged.fields["name"] == "new"


def test_merge_all_groups_by_asset():
    records = [
        AssetRecord("A", {"x": 1}, "chain", "1"),
        AssetRecord("B", {"y": 2}, "chain", "2"),
        AssetRecord("A", {"x": 9, "z": 3}, "indexer", "3"),
    ]
    merged = merge_all(records, PRIORITIES)
    assert set(merged) == {"A", "B"}
    assert merged["A"].fields == {"x": 1, "z": 3}
    assert merged["A"].provenance == {"x": "chain", "z": "indexer"}

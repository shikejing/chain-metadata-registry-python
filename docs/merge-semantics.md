# Merge semantics & priority

## Sources and priority

Each data source is configured with:

- `source_id` — stable identity;
- `priority` — an integer; **higher priority wins** field-level conflicts;
- `order` — a stable arrival-order tie-breaker (unique across sources).

Sources are fetched concurrently. The merge result must not depend on fetch
completion order or concurrency, so after fetching, candidates are sorted
deterministically by:

1. priority **descending** (higher wins);
2. source `order` **ascending** (arrival-order tie-break);
3. entry `height` **descending** (latest wins for equal priority);
4. `entry_id` **ascending** (final tie-break).

## Field-level merge

An asset may be described by multiple entries across sources. For each field,
the merger takes the **first present value** in the deterministic candidate
order. Every merged field carries provenance:

```json
{
  "field": "name",
  "value": "Ethereum",
  "source_id": "coingecko",
  "source_priority": 10,
  "entry_id": "cg-1",
  "height": 100,
  "content_hash": "..."
}
```

So a caller can always trace a value back to the source that won the conflict.

### Example

| Source | priority | name | symbol | decimals |
|--------|----------|------|--------|----------|
| coingecko | 10 | Ethereum | ETH | 18 |
| coinmarketcap | 5 | Ethereum | ETH | 18 |
| onchain | 1 | Ether | ETH | 18 |

Merged `name` = **Ethereum** (coingecko wins). If coingecko omitted `name`,
coinmarketcap would win; if both omitted it, onchain would.

## Identity de-duplication

Entries are de-duplicated by `entry_id`. Two sources reporting the same
identity with identical content collapse to one canonical entry. If the same
identity is reported with conflicting content, the highest-priority source's
version becomes canonical (chain-level priority).

## Determinism

`merged_asset.content_hash` is a canonical JSON hash of
`{asset_id, {field: {value, source_id, entry_id, height}}}`. Identical source
content and arrival order produce a field-by-field identical result regardless
of fetch scheduling.

## Incremental rebuild

On refresh, an asset is **carried** (unchanged) when its merged content hash
matches the previous snapshot; otherwise it is **rebuilt**. Only rebuilt assets
are re-merged. The refresh outcome reports `rebuilt_assets` and
`carried_assets`.

## Degraded sources

When a source fails or times out, its previous entries are **carried forward**
(so data is not silently dropped) and the snapshot is flagged `degraded` with
the missing source ids. Reads report `degraded_sources` and `missing_sources`.

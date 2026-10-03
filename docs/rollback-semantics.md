# Rollback & replay semantics

## Model

The chain is an append-only sequence of immutable entries indexed by
`height`. A refresh fetches each source's current view. A **rollback** (reorg)
occurs when a source that was fetched successfully no longer reports an entry
it had previously committed — the chain history was rewritten.

A rollback may:

- replace recent entries with a **longer** continuous history (head advances);
- replace recent entries with a **shorter** history (head shrinks).

In both cases the revoked entries are recorded.

## What happens during a refresh

1. All sources are fetched concurrently (per-source timeout + retry).
2. For each previously-committed entry:
   - if its source was fetched **successfully** and the entry is no longer in
     the canonical chain → it is **revoked**;
   - if its source **failed/timed out** → it is **carried forward** and the
     snapshot is flagged `degraded` (never silently revoked).
3. A revocation records `entry_id`, `source_id`, `height`, `asset_id`,
   `revoked_at_refresh`, `replaced_by` (the entry that took its place at the
   same height, if any), and `content_hash`.
4. The rollback window is applied (see below).
5. A new immutable snapshot is built and the committed reference is swapped
   atomically.

## Consistent reads

Readers never see a mix of old and new state: a read obtains the current
snapshot reference (an immutable object) and reads from it. Because the
reference swap is atomic and the snapshot is immutable, every read is some
complete committed snapshot. A slow reader holding an old snapshot does not
block refresh, which builds a new object and swaps the reference; the old
object stays alive until the reader releases it.

## Querying revoked entries

- `GET /v1/entries/{entry_id}` returns `status`:
  - `present` — in the current canonical chain;
  - `revoked` — revoked within the window (includes the revocation record);
  - `unknown` — not present and not within the window (aged out).
- `GET /v1/rollbacks` lists all revocations retained in the window.

## Rollback window

Configured by `chain.rollback_window_heights` (default 100). A revocation is
retained when:

```
head_height - revoked.height <= window
```

- **Within window**: the revocation is queryable (`status = revoked`).
- **At the boundary** (`head_height - height == window`): retained; reads are
  deterministic (tested).
- **Beyond window**: the revocation record is garbage collected; the entry
  status becomes `unknown` (aged out). This is deterministic and documented —
  aged-out entries are not silently served as present.

The window is applied to every snapshot, so revocations do not grow without
bound.

## Idempotency & restart

- `snapshot_id` is a deterministic content hash. A refresh whose merged
  content is identical to the previous snapshot reuses the prior commit
  timestamp and does **not** create a duplicate snapshot file.
- Commits write to a temp file, `fsync`, then atomically rename; the `LATEST`
  pointer is swapped the same way. A process restart loads the latest
  committed snapshot — it never observes a partial state.
- Old snapshot files are purged according to `store.keep_snapshots` (the
  latest snapshot is always retained).

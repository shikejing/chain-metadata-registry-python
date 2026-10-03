# Chain Metadata Registry

A chain asset metadata registration and query service. Metadata comes from
multiple configurable local data sources that may contradict each other; the
chain allows short rollbacks and replays. Reads always observe a single
committed, self-consistent snapshot, and refresh never drags down in-flight
queries.

## Features

- **Multi-source collection & field-level merge** with explicit priority,
  deterministic results regardless of fetch order/concurrency, and per-field
  provenance back to the winning source.
- **Chain rollback & replay**: revoked entries are recorded and queryable; a
  read never sees both revoked and new entries.
- **Consistent read views**: readers observe an immutable committed snapshot;
  slow reads do not block refresh.
- **Incremental refresh & staleness tolerance**: only affected assets are
  rebuilt; freshness and background refresh are configurable; failures degrade
  distinguishably (never silently stale/incomplete).
- **Concurrency & fault isolation**: a slow/failed source does not block
  others; per-source timeout and retry budget; degraded results name the
  missing sources.
- **Determinism & idempotency**: identical source content + arrival order
  yields field-by-field identical results; duplicate entries de-duplicated by
  identity; committed snapshots survive restart (no partial state).
- **Configurable rollback window**: tolerable rollback depth and
  beyond-window behavior are explicit; reads at the window boundary are
  deterministic.

## Quick start

```bash
# install dependencies
uv sync

# run the service (reads config.json)
REGISTRY_CONFIG=config.json uv run python main.py

# or run a one-off refresh via the API
curl -X POST http://127.0.0.1:8000/v1/refresh
curl http://127.0.0.1:8000/v1/assets
curl http://127.0.0.1:8000/v1/assets/eth
```

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health: `ok` / `degraded` / `stale_served` / `stale` / `no_snapshot` |
| GET | `/v1/snapshot` | Current committed snapshot metadata |
| GET | `/v1/assets` | List merged assets (with freshness metadata) |
| GET | `/v1/assets/{asset_id}` | Single asset with per-field provenance |
| GET | `/v1/entries/{entry_id}` | Entry status: `present` / `revoked` / `unknown` |
| GET | `/v1/rollbacks` | Revoked entries within the rollback window |
| POST | `/v1/refresh` | Trigger a refresh |

Every asset/snapshot response carries `snapshot_id`, `committed_at`,
`age_seconds`, `stale`, `degraded_sources`, and `missing_sources` so callers
always know exactly what they are reading.

## Configuration

See [`docs/configuration.md`](docs/configuration.md). A sample
[`config.json`](config.json) and [`data/`](data/) are provided.

## Semantics

- [Merge semantics & priority](docs/merge-semantics.md)
- [Rollback & replay semantics](docs/rollback-semantics.md)

## Architecture

```
sources (file/static)  ──concurrent fetch (timeout+retry)──┐
                                                            ▼
                                              refresh engine (isolation)
                                                            │  incremental rebuild
                                                            │  rollback detection
                                                            │  window application
                                                            ▼
                                              merge (deterministic)
                                                            │
                                                            ▼
                                    atomic snapshot swap ──► readers (lock-free)
                                                            │
                                                            ▼
                                              durable store (atomic write + fsync)
```

## Tests

```bash
uv run pytest
```

Tests cover multi-source conflict, rollback/replay, concurrent read/write,
source timeout, and restart recovery.

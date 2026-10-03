# Configuration reference

Configuration is a JSON file (default `config.json`, override with
`REGISTRY_CONFIG`). A sample is provided at [`../config.json`](../config.json).

## Top-level

```json
{
  "sources": [ ... ],
  "refresh": { ... },
  "chain": { ... },
  "store": { ... }
}
```

## `sources[]`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_id` | string | required | Stable unique source id |
| `kind` | string | required | `file` or `static` |
| `priority` | int | required | Field-level conflict priority (higher wins) |
| `order` | int | required | Arrival-order tie-breaker (unique) |
| `path` | string | — | (`file`) path to a JSON array of entries |
| `entries` | array | `[]` | (`static`) inline entries |
| `timeout_seconds` | float | `2.0` | Per-attempt timeout |
| `max_retries` | int | `2` | Retry attempts after the first failure |
| `retry_backoff_seconds` | float | `0.1` | Delay between retries |

A source that times out or fails after `max_retries` is marked degraded; its
previous entries are carried forward and the snapshot reports it as missing.

Entry records (in `file` arrays or `static.entries`):

```json
{
  "entry_id": "cg-1",
  "height": 100,
  "asset_id": "eth",
  "fields": {"name": "Ethereum", "symbol": "ETH", "decimals": 18}
}
```

`entry_id` is optional; if omitted it is derived from
`(source_id, height, asset_id, fields)`. Entries are de-duplicated by
identity.

## `refresh`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval_seconds` | float | `5.0` | Background refresh interval; `0` disables background refresh |
| `freshness_ttl_seconds` | float | `30.0` | Snapshot age before it is considered stale |
| `stale_policy` | string | `serve_stale` | `serve_stale` (serve with `stale=true`) or `error` (HTTP 503) |

## `chain`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rollback_window_heights` | int | `100` | Tolerable rollback depth; revocations within `head - height <= window` are retained |

## `store`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | `.registry/snapshots` | Snapshot storage directory |
| `keep_snapshots` | int | `5` | Number of newest snapshot files to retain |

## Environment

| Variable | Description |
|----------|-------------|
| `REGISTRY_CONFIG` | Path to config JSON (default `config.json`) |
| `REGISTRY_HOST` | Bind host (default `127.0.0.1`) |
| `REGISTRY_PORT` | Bind port (default `8000`) |

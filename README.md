# chain-metadata-registry

链上资产元数据的注册与查询服务。元数据来自多个可配置的本地数据源，
支持字段级冲突裁决、链上回滚/重放、快照一致读取、增量刷新与显式降级。

## 架构

```
registry/
  models.py    不可变数据模型（AssetRecord / MergedAsset / ChainEntry / Snapshot ...）
  config.py    配置（数据源、回滚窗口、新鲜度、持久化目录）
  merge.py     确定性字段级多源合并
  sources.py   数据源抽象 + 本地 JSON 文件源
  store.py     MVCC 快照存储：原子发布、回滚/重放、崩溃安全持久化
  service.py   编排：并发抓取、超时重试、增量重建、后台刷新、查询
  api.py       FastAPI HTTP 接口
```

## 合并优先级（字段级）

同一资产被多个源描述时，逐字段裁决，结果与源的返回顺序、并发度无关：

1. **同源多条记录**：`entry_id` 最大者（最新）胜出；`entry_id` 相同的重复到达
   按身份 `(source, entry_id)` 去重，是幂等空操作。
2. **跨源冲突**：`SourceConfig.priority` 数字最小者赢得该字段。
3. 每个最终字段的来源记录在 `MergedAsset.provenance` 中，可完整追溯。

## 回滚与重放语义

- 日志条目按高度顺序提交；`reorg(rollback_count, replay)` 撤销最近
  `rollback_count` 条并接入一段**可以更长**的新历史。
- **原子性**：回滚+重放+重建在一次原子提交内完成。任何并发查询要么看到
  旧的完整快照，要么看到新的完整快照，绝不会同时看到被撤销的旧条目和
  新写入的条目。
- **可审计**：每次 reorg 的 `rolled_back` / `replayed` 条目保存在快照中，
  通过 `GET /reorgs` 查询。
- 被撤销条目的身份会被释放，允许同身份条目以修正后的内容重放。

### 回滚窗口

- `rollback_window`（默认 64）：允许撤销的最大条目数，超出部分视为最终确认。
- 超出窗口时由 `window_exceeded_policy` 决定：
  - `ERROR`（默认）：拒绝整个 reorg，已提交历史不变，API 返回 409——
    显式失败，不静默丢数据；
  - `CLAMP`：只回滚窗口深度内的条目，更老的条目不可撤销。
    窗口边界行为因此是确定的：深度 ≤ 窗口全部撤销，> 窗口按策略拒绝或截断。

## 一致性读取（MVCC）

- 读取只访问**已提交的不可变快照**；发布是单次原子引用交换。
- 刷新/回滚在写锁下先完整构建新快照再发布，慢读者持有旧快照不阻塞写入，
  写入也不会让读者看到字段级或条目级的混合状态。

## 增量刷新与降级

- 每次刷新只重建**新到达条目涉及的资产**（reorg 时只重建被撤销+被重放的资产）。
- 每个源在独立线程抓取，使用各自配置的 `timeout_s` 与 `max_retries`；
  单个源变慢/失败不阻塞其他源，也不阻塞查询。
- 源失败时：状态标记为 `TIMEOUT` / `ERROR`，该源列入快照的
  `missing_sources`，**之前已提交的数据继续可见但显式标记**，绝不静默返回
  过期或残缺数据。查询结果带 `stale` 标志（距上次成功刷新超过
  `staleness_ttl_s` 时为 true）。

## 持久化与重启恢复

- 每次提交把完整快照写入 `state_dir/snapshot.json`（先写临时文件再原子
  rename），重启后要么恢复到完整的最近已提交状态，要么为空——
  不会出现只能读到一部分的状态。已见条目身份一并持久化，重启后重复到达
  仍被去重。

## 配置项（`RegistryConfig` / `SourceConfig`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `sources` | `()` | 数据源列表 |
| `rollback_window` | 64 | 可承受的最大回滚深度 |
| `window_exceeded_policy` | `ERROR` | 超窗行为：`ERROR` / `CLAMP` |
| `staleness_ttl_s` | 30 | 超过该时长未成功刷新则查询标记 `stale` |
| `refresh_interval_s` | 0 | 后台刷新间隔；0 关闭后台循环 |
| `state_dir` | `""` | 快照持久化目录；空则不持久化 |
| `SourceConfig.priority` | — | 字段冲突优先级，小者胜 |
| `SourceConfig.path` | `""` | JSON 文件源路径 |
| `SourceConfig.timeout_s` | 5.0 | 单次抓取超时 |
| `SourceConfig.max_retries` | 1 | 超时/失败后的重试次数 |

### JSON 数据源格式

```json
[{"asset_id": "ETH", "entry_id": "1", "fields": {"name": "Ether", "decimals": 18}}]
```

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/assets` | 列出当前快照全部资产（含 provenance） |
| GET | `/assets/{id}` | 查询单个资产（含 stale / missing_sources） |
| POST | `/refresh` | 触发一次刷新 |
| POST | `/reorg` | `{"rollback_count": n, "replay": [...]}` |
| GET | `/reorgs` | 历史回滚记录（撤销了哪些条目） |
| GET | `/health` | 各数据源状态 |

## 运行与测试

```bash
uv run pytest          # 全部测试
uv run python main.py  # 启动服务
```

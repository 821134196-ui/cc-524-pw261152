# 批量评分平台（Batch Scoring Platform）

一个具备完整可追溯链路的批量评分应用：输入数据、特征快照、模型版本、参数与输出结果全部内容哈希化并串联成血缘链；支持补数据/模型回滚后的指定批次重跑与差异对比，旧结果永久可下载；同一批次重复提交幂等，不会重复发布。

## 快速开始

```bash
python3 -m venv .venv && .venv/bin/pip install flask requests
.venv/bin/python run.py --port 5000          # 启动服务
.venv/bin/python -I scripts/seed.py          # 可选：灌入演示数据
```

打开 http://127.0.0.1:5000 （看板）· `/assets`（资产）· `/submit`（提交批次）。

运行验证套件（35 项检查：幂等、部分失败、并行、回滚、发布/撤回等）：

```bash
.venv/bin/python -I tests/e2e.py
```

## 血缘链路（可追溯）

```
数据集 Dataset ──> 特征快照 Snapshot ──> 模型 Model ──> 参数 Params ──> 批次 Batch ──> 结果 Results ──> 发布 Publication
 sha256            sha256                sha256          sha256           状态/计数      版本化文件      唯一有效发布
```

- **数据集**：JSONL 上传，每行一个含 `item_key` 的记录，整体 sha256。
- **特征快照**：从数据集物化特征值并冻结落盘，重跑永远看到相同输入。
- **模型**：`{weights, bias, output: sigmoid|linear}`，逐特征加权求和；可回滚（`rolled_back`），回滚后仍可用于重跑。
- **参数**：`threshold / scale / clip_min / clip_max`，canonical JSON 哈希。
- **结果**：每次运行落盘一个版本化 JSONL 文件（`results` 表），**从不删除** —— 回滚/重跑后旧结果仍可下载。
- 批次详情页 `/batches/<id>` 展示完整链路卡片与各环节哈希；`GET /api/batches/<id>/lineage` 返回 JSON。

## 关键语义

| 需求 | 实现 |
|---|---|
| 重复提交不重复执行 | `batches.batch_key` 唯一；重复提交返回已有批次（`created=false`，HTTP 200），并发下由唯一约束兜底 |
| 不重复发布 | `publications` 上的部分唯一索引 `WHERE status='published'`：同一批次最多一个有效发布；重复发布幂等返回已有发布 |
| 补数据重跑 | 上传新数据集版本 → 构建新快照 → `POST /api/batches/<id>/rerun {snapshot_id}`，新批次经 `rerun_of` 链接父批次 |
| 模型回滚重跑 | `POST /api/models/<id>/rollback` 标记回滚 → 重跑时指定旧模型 → diff 展示分数变化 |
| 差异对比 | `GET /api/batches/<a>/diff/<b>`：逐 item 对比（新增/移除/分数变化 Δ/状态变化）+ 血缘差异（哪段输入变了） |
| 部分失败 | 单条记录评分异常只标记该 item 失败：全部成功 `success`，部分 `partial_failed`，全部 `failed`；失败批次禁止发布 |
| 并行批次 | 4 线程池执行器 + SQLite WAL，多批次并发运行互不阻塞 |
| 撤回 | `POST /api/batches/<id>/withdraw`；撤回后可再发布（产生新发布记录，历史保留） |

## API 一览

```
POST /api/datasets            {name, version, content}        注册数据集（JSONL）
POST /api/snapshots           {dataset_id, name, version, fields}
POST /api/models              {name, version, artifact}
POST /api/models/<id>/rollback
POST /api/batches             {batch_key, snapshot_id, model_id, params, note}   幂等
GET  /api/batches[/<id>]      列表 / 详情（状态、计数、时间戳）
POST /api/batches/<id>/rerun  {snapshot_id?, model_id?, params?}                 全部可选，缺省沿用
GET  /api/batches/<id>/lineage    完整血缘链（含各环节 sha256）
GET  /api/batches/<id>/items      逐条评分明细（含失败原因）
GET  /api/batches/<a>/diff/<b>    差异对比
POST /api/batches/<id>/publish    幂等；仅 success/partial_failed 可发布
POST /api/batches/<id>/withdraw
GET  /api/results/<id>/download   任意历史版本结果下载
```

页面：`/` 看板（状态统计、批次表、3 秒自动刷新）· `/assets` 资产管理 · `/submit` 提交 · `/batches/<id>` 血缘/明细/发布/撤回/重跑 · `/batches/<a>/diff/<b>` 差异视图。

## 目录结构

```
app/
  db.py        SQLite schema（唯一约束、部分唯一索引）+ WAL 连接
  core.py      业务逻辑：资产注册、幂等提交、重跑、发布/撤回、血缘、diff
  scoring.py   逐条评分（单条失败不影响整批）
  runner.py    线程池执行引擎，结果版本化落盘
  server.py    Flask 路由（JSON API + 服务端渲染页面）
  templates/   看板 / 批次详情 / 差异 / 资产 / 提交
run.py         入口
scripts/seed.py  演示数据
tests/e2e.py   35 项端到端验证
```

## 环境变量

- `BATCH_SCORER_DATA`：数据目录（默认 `data/`，含 `app.db` 与各产物文件）
- `BATCH_SCORER_ITEM_DELAY`：单条评分延时秒数（默认 0.02，便于观察并行；设为 0 关闭）

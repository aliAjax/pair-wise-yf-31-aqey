# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案。
- `POST /api/offline-batches`：提交离线批次并立即按 航班/资源基线 + 操作序号 合并。
- `POST /api/batches/{id}/retry`：写入失败或冲突处理后从断点重试（可携带 `resolutions`）。
- `GET /api/batches`、`GET /api/batches/{id}`、`GET /api/plans/{id}/batches`：批次、合并结果与待处理冲突。
- `POST /api/queues/recover/offline`：进程重启后恢复中断队列（服务启动时也会自动执行）。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响。

## 离线批次合并语义

两位调度员可各自离线调整同一恢复方案，网络恢复后提交：

- 请求包含 `plan_id`、`device_id`、`base_revision`（离线路源基线修订号）和按 `seq` 排序的 `operations`。
- 操作类型：`reassign`（改派，payload 同 assignments）、`upsert_aircraft`、`upsert_crew`、`upsert_permit`（资源基线合并）。
- 同一航班只认**最后一次改派**：批次内较小序号的改派标记为 `superseded`；不同航班、飞机/机组/许可操作按序号幂等应用。
- 资源合并后对整个方案**重新校验**（维护间隔、执勤时限、宵禁、航线许可、资源重叠、与已锁定方案冲突），问题进 `pending_conflicts`。
- 基线过期时：未被他人修改的航班自动 rebase 应用；同航班已被在线改过则进待处理冲突（`concurrent_reassign`），不会静默覆盖。
- 已锁定方案占用的资源**不会**被晚到批次释放：锁定后的改派一律 `plan_locked` 阻断。
- 每个操作独立事务并推进 `checkpoint_seq`；物理写入失败（`status=failed`）后从断点重试，修订号只增加一次。
- 批次状态：`queued/processing/applied/conflict/failed`；冲突和队列均持久化，进程崩溃重启后自动续跑。
- 旧版数据库通过 `PRAGMA user_version` 增量升级，保留修订号、审计记录和原方案。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示，正式运行需要外部身份系统、共享数据库和更强的跨实例锁。

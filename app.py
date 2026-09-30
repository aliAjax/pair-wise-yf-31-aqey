#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, time, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}
SCHEMA_VERSION = 2
BATCH_OP_TYPES = {"reassign", "upsert_aircraft", "upsert_crew", "upsert_permit"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_clock", f"时刻格式应为 HH:MM: {value}") from exc


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        # 单连接被 ThreadingHTTPServer 的工作线程共享：所有事务串行化，避免事务状态互串
        self._conn_lock = threading.RLock()
        self._init()

    @contextmanager
    def tx(self):
        with self._conn_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    @contextmanager
    def shared(self):
        """只读访问：与事务互斥，保证不会读到半截事务状态。"""
        with self._conn_lock:
            yield self.conn

    def _init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
            CREATE TABLE IF NOT EXISTS flights(
                id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS recovery_plans(
                id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT
            );
            CREATE TABLE IF NOT EXISTS assignments(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0,
                missed_connections INTEGER NOT NULL DEFAULT 0, changed_revision INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT '',
                UNIQUE(plan_id,flight_id)
            );
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS offline_batches(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id),
                device_id TEXT NOT NULL DEFAULT '', base_revision INTEGER NOT NULL, merged_revision INTEGER,
                status TEXT NOT NULL DEFAULT 'queued', checkpoint_seq INTEGER NOT NULL DEFAULT 0,
                created_by TEXT NOT NULL, created_role TEXT NOT NULL, created_at TEXT NOT NULL,
                processed_at TEXT, error_code TEXT, error_message TEXT, result_json TEXT
            );
            CREATE TABLE IF NOT EXISTS batch_operations(
                id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES offline_batches(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL, op_type TEXT NOT NULL, flight_id INTEGER, resource_id TEXT,
                payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', error_code TEXT, error_message TEXT,
                applied_at TEXT, UNIQUE(batch_id,seq)
            );
            CREATE TABLE IF NOT EXISTS pending_conflicts(
                id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES offline_batches(id) ON DELETE CASCADE,
                op_id INTEGER REFERENCES batch_operations(id) ON DELETE SET NULL, plan_id INTEGER NOT NULL,
                flight_id INTEGER, conflict_type TEXT NOT NULL, resource TEXT, message TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'op', status TEXT NOT NULL DEFAULT 'open', created_at TEXT NOT NULL,
                resolved_at TEXT, resolution_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_batches_plan ON offline_batches(plan_id,id);
            CREATE INDEX IF NOT EXISTS idx_batch_ops_batch ON batch_operations(batch_id,seq);
            CREATE INDEX IF NOT EXISTS idx_conflicts_batch ON pending_conflicts(batch_id,status);
            CREATE INDEX IF NOT EXISTS idx_conflicts_open ON pending_conflicts(status,plan_id);
            """
        )
        self._migrate()

    def _migrate(self) -> None:
        """旧版数据库升级：保留修订号、审计记录和原方案，只做增量变更。"""
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            columns = {r["name"] for r in self.conn.execute("PRAGMA table_info(assignments)")}
            if "changed_revision" not in columns:
                self.conn.execute("ALTER TABLE assignments ADD COLUMN changed_revision INTEGER NOT NULL DEFAULT 1")
            if "updated_at" not in columns:
                self.conn.execute("ALTER TABLE assignments ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
        # user_version 0 既可能是旧库（无版本标记）也可能是刚按新结构建好的库；此时结构已一致。
        self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)
        # 测试钩子：在批次操作提交前抛出异常，模拟写入失败（断点重试验证用）
        self._write_fail_hook: Any = None
        self._batch_lock = threading.RLock()
        self.recover_queue()

    @staticmethod
    def identity(headers: Any) -> tuple[str, str]:
        actor, role = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        return actor, role

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    def seed_airport(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机场数据")
        code = str(body.get("code", "")).upper().strip()
        country = str(body.get("country", "")).upper().strip()
        if not code or not country:
            raise ApiError(400, "missing_fields", "code 和 country 必填")
        start = str(body.get("curfew_start", "23:00")); end = str(body.get("curfew_end", "06:00"))
        parse_clock(start); parse_clock(end)
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end) VALUES(?,?,?,?)", (code, country, start, end))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def seed_aircraft(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护飞机")
        ident, model = str(body.get("id", "")).strip(), str(body.get("model", "")).strip()
        if not ident or not model: raise ApiError(400, "missing_fields", "id 和 model 必填")
        due = iso(parse_time(body.get("maintenance_due")))
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?)", (ident, model, due, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM aircraft WHERE id=?", (ident,)).fetchone())

    def seed_crew(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机组")
        ident, name, base = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("base", "")).upper().strip()
        duty = parse_time(body.get("duty_start")); maximum = body.get("max_duty_minutes")
        if not ident or not name or not base or not isinstance(maximum, int) or maximum <= 0:
            raise ApiError(400, "invalid_crew", "id、name、base 和正整数 max_duty_minutes 必填")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)", (ident, name, base, iso(duty), maximum, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM crew WHERE id=?", (ident,)).fetchone())

    def create_permit(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "permit_forbidden", "只有运行经理可以维护航线许可")
        origin, destination = str(body.get("origin", "")).upper(), str(body.get("destination", "")).upper()
        if not origin or not destination: raise ApiError(400, "missing_fields", "origin 和 destination 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from: raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            try:
                cur = conn.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)",
                                   (origin, destination, iso(valid_from), iso(valid_to), int(bool(body.get("curfew_exempt")))))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "permit_exists", "相同航线与有效期的许可已存在") from exc
            return dict(conn.execute("SELECT * FROM permits WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_flight(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "flight_forbidden", "当前角色不能创建航班")
        required = ("flight_no", "origin", "destination", "std", "sta", "aircraft_id", "crew_id")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        std, sta = parse_time(body["std"]), parse_time(body["sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
        passengers = body.get("passenger_count", 0)
        if not isinstance(passengers, int) or passengers < 0: raise ApiError(400, "invalid_passengers", "passenger_count 必须是非负整数")
        with self.repo.tx() as conn:
            for table, ident in (("aircraft", body["aircraft_id"]), ("crew", body["crew_id"])):
                row = conn.execute(f"SELECT status FROM {table} WHERE id=?", (ident,)).fetchone()
                if not row or row["status"] != "active": raise ApiError(409, "resource_unavailable", f"{table} {ident} 不可用")
            try:
                cur = conn.execute("""INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,updated_at)
                                      VALUES(?,?,?,?,?,?,?,?,?)""",
                                   (body["flight_no"].upper(), body["origin"].upper(), body["destination"].upper(), iso(std), iso(sta),
                                    body["aircraft_id"], body["crew_id"], passengers, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "flight_exists", "航班号已存在") from exc
            return dict(conn.execute("SELECT * FROM flights WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_disruption(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能登记中断")
        kind, resource = str(body.get("kind", "")).strip(), str(body.get("resource_id", "")).strip()
        if kind not in {"airport_closure", "aircraft_fault", "crew_timeout"} or not resource:
            raise ApiError(400, "invalid_disruption", "kind 或 resource_id 无效")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            cur = conn.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES(?,?,?,?,?)",
                               (kind, resource.upper() if kind == "airport_closure" else resource, iso(start), iso(end), iso()))
            return dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_plan(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能创建恢复方案")
        disruption_id, name = body.get("disruption_id"), str(body.get("name", "")).strip()
        assignments = body.get("assignments", [])
        if not isinstance(disruption_id, int) or not name or not isinstance(assignments, list):
            raise ApiError(400, "invalid_plan", "disruption_id、name 和 assignments 必填")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM disruptions WHERE id=?", (disruption_id,)).fetchone():
                raise ApiError(404, "disruption_not_found", "中断事件不存在")
            cur = conn.execute("INSERT INTO recovery_plans(disruption_id,name,created_by,created_at) VALUES(?,?,?,?)", (disruption_id, name, actor, iso()))
            plan_id = cur.lastrowid
            for item in assignments:
                self._insert_assignment(conn, plan_id, item, replace=False)
            Repository.audit(conn, plan_id, actor, role, "plan_created", {"disruption_id": disruption_id, "assignment_count": len(assignments)})
            return self.get_plan(plan_id)

    def _insert_assignment(self, conn: sqlite3.Connection, plan_id: int, item: dict[str, Any], replace: bool) -> None:
        required = ("flight_id", "aircraft_id", "crew_id", "new_std", "new_sta")
        if any(item.get(k) in (None, "") for k in required): raise ApiError(400, "invalid_assignment", f"飞行调整缺少字段: {', '.join(k for k in required if item.get(k) in (None, ''))}")
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (item["flight_id"],)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        if flight["status"] == "canceled" and item.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(item.get("missed_connections", 0))
        if missed < 0: raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        new_revision = plan["revision"] + 1 if replace else 1
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,missed_connections=?,changed_revision=?,updated_at=?
                                WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, new_revision, iso(), plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections,changed_revision,updated_at)
                                VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, 1, iso()))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "assignment_conflict", "方案中该航班已存在或资源无效") from exc
        except KeyError as exc:
            raise ApiError(404, "assignment_not_found", "待替换的航班调整不存在") from exc

    def add_assignment(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "assignment_forbidden", "当前角色不能修改方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案已被其他人更新")
            self._insert_assignment(conn, plan_id, body, replace=True)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            Repository.audit(conn, plan_id, actor, role, "assignment_reassigned", {"flight_id": body.get("flight_id")})
            return self.get_plan(plan_id)

    # ------------------------------------------------------------------
    # 离线批次：两位调度员各自离线调整，联网后按 航班/资源基线 + 操作序号 合并
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_op(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ApiError(400, "invalid_operation", "每个操作必须是对象")
        seq, op_type = raw.get("seq"), str(raw.get("op_type", "")).strip()
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            raise ApiError(400, "invalid_operation", "操作序号 seq 必须是正整数")
        if op_type not in BATCH_OP_TYPES:
            raise ApiError(400, "invalid_operation", f"不支持的操作类型: {op_type}")
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_operation", f"操作 {seq} 缺少 payload 对象")
        flight_id, resource_id = None, None
        if op_type == "reassign":
            flight_id = payload.get("flight_id")
            if not isinstance(flight_id, int):
                raise ApiError(400, "invalid_operation", f"操作 {seq} 的 flight_id 必须是整数")
        else:
            key = {"upsert_aircraft": "id", "upsert_crew": "id", "upsert_permit": "resource_id"}[op_type]
            resource_id = str(payload.get(key, "")).strip()
            if not resource_id:
                raise ApiError(400, "invalid_operation", f"操作 {seq} 的资源标识 {key} 必填")
        return {"seq": seq, "op_type": op_type, "flight_id": flight_id, "resource_id": resource_id, "payload": payload}

    def submit_batch(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "batch_forbidden", "当前角色不能提交离线批次")
        plan_id, base_revision, raw_ops = body.get("plan_id"), body.get("base_revision"), body.get("operations", [])
        if not isinstance(plan_id, int) or not isinstance(base_revision, int) or base_revision < 1 or not isinstance(raw_ops, list) or not raw_ops:
            raise ApiError(400, "invalid_batch", "plan_id、正整数 base_revision 和非空 operations 必填")
        ops = [self._normalize_op(raw) for raw in raw_ops]
        if len({op["seq"] for op in ops}) != len(ops):
            raise ApiError(400, "invalid_batch", "批次内操作序号 seq 不能重复")
        now = iso()
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT id,revision,status FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan:
                raise ApiError(404, "plan_not_found", "方案不存在")
            cur = conn.execute("""INSERT INTO offline_batches(plan_id,device_id,base_revision,status,created_by,created_role,created_at)
                                  VALUES(?,?,?,'queued',?,?,?)""",
                               (plan_id, str(body.get("device_id", "")).strip(), base_revision, actor, role, now))
            batch_id = cur.lastrowid
            for op in ops:
                conn.execute("""INSERT INTO batch_operations(batch_id,seq,op_type,flight_id,resource_id,payload_json,status)
                                VALUES(?,?,?,?,?,?,'pending')""",
                             (batch_id, op["seq"], op["op_type"], op["flight_id"], op["resource_id"], json.dumps(op["payload"], ensure_ascii=False, sort_keys=True)))
            # 同一航班只认最后一次改派：批次内较小序号的改派直接标记 superseded
            conn.execute("""UPDATE batch_operations SET status='superseded' WHERE batch_id=? AND op_type='reassign'
                            AND id NOT IN (SELECT id FROM (SELECT MAX(id) id FROM batch_operations WHERE batch_id=? AND op_type='reassign' GROUP BY flight_id))""",
                         (batch_id, batch_id))
            Repository.audit(conn, plan_id, actor, role, "batch_submitted",
                             {"batch_id": batch_id, "base_revision": base_revision, "operation_count": len(ops)})
        # 网络恢复：入队后立即按 航班/资源基线 + 操作序号 合并
        return self._process_batch(batch_id)

    def _batch_row(self, conn: sqlite3.Connection, batch_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM offline_batches WHERE id=?", (batch_id,)).fetchone()
        if not row:
            raise ApiError(404, "batch_not_found", "离线批次不存在")
        return row

    @staticmethod
    def _op_payload(op: sqlite3.Row) -> dict[str, Any]:
        return json.loads(op["payload_json"])

    def _mark_op_blocked(self, batch_id: int, op_id: int, code: str, message: str, conflict_type: str,
                         flight_id: int | None, resource: str | None) -> None:
        with self.repo.tx() as conn:
            conn.execute("UPDATE batch_operations SET status='blocked',error_code=?,error_message=? WHERE id=?", (code, message, op_id))
            conn.execute("""INSERT INTO pending_conflicts(batch_id,op_id,plan_id,flight_id,conflict_type,resource,message,created_at)
                            VALUES(?,?,?,?,?,?,?,?)""",
                         (batch_id, op_id, self._batch_row(conn, batch_id)["plan_id"], flight_id, conflict_type, resource, message, iso()))

    def _apply_resource_op(self, conn: sqlite3.Connection, op: sqlite3.Row, payload: dict[str, Any]) -> None:
        # 资源基线合并：飞机、机组、航线许可按最后一次操作幂等 upsert
        if op["op_type"] == "upsert_aircraft":
            model = str(payload.get("model", "")).strip()
            if not model:
                raise ApiError(400, "invalid_aircraft", "model 必填")
            due = iso(parse_time(payload.get("maintenance_due")))
            conn.execute("INSERT INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET model=excluded.model,maintenance_due=excluded.maintenance_due,status=excluded.status",
                         (op["resource_id"], model, due, payload.get("status", "active")))
        elif op["op_type"] == "upsert_crew":
            name, base = str(payload.get("name", "")).strip(), str(payload.get("base", "")).upper().strip()
            duty = parse_time(payload.get("duty_start"))
            maximum = payload.get("max_duty_minutes")
            if not name or not base or not isinstance(maximum, int) or maximum <= 0:
                raise ApiError(400, "invalid_crew", "name、base 和正整数 max_duty_minutes 必填")
            conn.execute("""INSERT INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)
                            ON CONFLICT(id) DO UPDATE SET name=excluded.name,base=excluded.base,duty_start=excluded.duty_start,max_duty_minutes=excluded.max_duty_minutes,status=excluded.status""",
                         (op["resource_id"], name, base, iso(duty), maximum, payload.get("status", "active")))
        else:
            origin, destination = str(payload.get("origin", "")).upper().strip(), str(payload.get("destination", "")).upper().strip()
            if not origin or not destination:
                raise ApiError(400, "invalid_permit", "origin 和 destination 必填")
            valid_from, valid_to = parse_time(payload.get("valid_from")), parse_time(payload.get("valid_to"))
            if valid_to <= valid_from:
                raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
            exempt = int(bool(payload.get("curfew_exempt")))
            conn.execute("""INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)
                            ON CONFLICT(origin,destination,valid_from,valid_to) DO UPDATE SET curfew_exempt=excluded.curfew_exempt""",
                         (origin, destination, iso(valid_from), iso(valid_to), exempt))

    def _apply_reassign_op(self, conn: sqlite3.Connection, batch: sqlite3.Row, op: sqlite3.Row, payload: dict[str, Any], target_revision: int) -> None:
        plan_id, force = batch["plan_id"], bool(payload.get("force"))
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan:
            raise ApiError(404, "plan_not_found", "方案不存在")
        # 锁定方案占用的资源不能因晚到内容被释放：锁定后任何离线改派一律拒绝
        if plan["status"] != "draft":
            raise ApiError(409, "plan_locked", "方案已锁定，晚到的离线改派不能覆盖或释放已占用资源")
        flight_id = op["flight_id"]
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
        if not flight:
            raise ApiError(404, "flight_not_found", f"航班 {flight_id} 不存在")
        std, sta = parse_time(payload["new_std"]), parse_time(payload["new_sta"])
        if sta <= std:
            raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        if flight["status"] == "canceled" and payload.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        assignment = conn.execute("SELECT changed_revision FROM assignments WHERE plan_id=? AND flight_id=?", (plan_id, flight_id)).fetchone()
        base_revision = batch["base_revision"]
        # 基线过期且该航班在基线之后被其他人改过：已有改派看 changed_revision；计划中尚无改派的新航班看航班自身修订号
        if assignment is not None:
            latest = assignment["changed_revision"]
        else:
            latest = flight["revision"] if flight["revision"] > base_revision else base_revision
        if not force and latest > base_revision:
            raise ApiError(409, "concurrent_reassign",
                           f"航班 {flight['flight_no']} 在离线基线 r{base_revision} 之后被改派为 r{latest}，需要人工决定")
        if not conn.execute("SELECT 1 FROM aircraft WHERE id=?", (payload["aircraft_id"],)).fetchone():
            raise ApiError(404, "aircraft_unavailable", f"飞机 {payload['aircraft_id']} 不存在")
        if not conn.execute("SELECT 1 FROM crew WHERE id=?", (payload["crew_id"],)).fetchone():
            raise ApiError(404, "crew_unavailable", f"机组 {payload['crew_id']} 不存在")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(payload.get("missed_connections", 0))
        if missed < 0:
            raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        now = iso()
        # 按航班 upsert：同航班最后一次改派生效，不删除其它航班的改派（锁定资源不会被释放）
        conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections,changed_revision,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(plan_id,flight_id) DO UPDATE SET aircraft_id=excluded.aircraft_id,crew_id=excluded.crew_id,new_std=excluded.new_std,
                        new_sta=excluded.new_sta,status=excluded.status,delay_minutes=excluded.delay_minutes,
                        missed_connections=excluded.missed_connections,changed_revision=excluded.changed_revision,updated_at=excluded.updated_at""",
                     (plan_id, flight_id, payload["aircraft_id"], payload["crew_id"], iso(std), iso(sta),
                      payload.get("status", "planned"), delay, missed, target_revision, now))
        conn.execute("UPDATE recovery_plans SET revision=? WHERE id=?", (target_revision, plan_id))

    def _process_batch(self, batch_id: int) -> dict[str, Any]:
        with self._batch_lock:
            with self.repo.tx() as conn:
                batch = self._batch_row(conn, batch_id)
                if batch["status"] in {"applied", "processing"}:
                    return self._batch_view(conn, batch)  # 已应用或已被其他线程抢占：幂等返回
                if batch["status"] == "conflict":
                    raise ApiError(409, "batch_needs_resolution", "批次有待处理冲突，请通过 retry 接口携带解决方案续跑")
                plan = conn.execute("SELECT revision FROM recovery_plans WHERE id=?", (batch["plan_id"],)).fetchone()
                if not plan:
                    raise ApiError(404, "plan_not_found", "方案不存在")
                conn.execute("UPDATE offline_batches SET status='processing',error_code=NULL,error_message=NULL WHERE id=?", (batch_id,))
                current_revision = plan["revision"]
                reassign_count = conn.execute("SELECT COUNT(*) c FROM batch_operations WHERE batch_id=? AND op_type='reassign' AND status!='superseded'", (batch_id,)).fetchone()["c"]
                target_revision = max(current_revision, batch["base_revision"]) + 1 if reassign_count else None
            failure: dict[str, Any] | None = None
            with self.repo.shared() as conn:
                ops = conn.execute("SELECT * FROM batch_operations WHERE batch_id=? AND status!='superseded' ORDER BY seq", (batch_id,)).fetchall()
            for op in ops:
                if op["status"] in {"applied", "skipped"}:
                    continue
                payload = self._op_payload(op)
                try:
                    with self.repo.tx() as conn:
                        batch = self._batch_row(conn, batch_id)
                        if op["op_type"] == "reassign":
                            self._apply_reassign_op(conn, batch, op, payload, target_revision)
                        else:
                            self._apply_resource_op(conn, op, payload)
                        if self._write_fail_hook is not None:
                            self._write_fail_hook(batch_id, op)
                        conn.execute("UPDATE batch_operations SET status='applied',error_code=NULL,error_message=NULL,applied_at=? WHERE id=?", (iso(), op["id"]))
                        # 断点只随成功操作前移；失败的操作留在断点处，重试从它继续
                        conn.execute("UPDATE offline_batches SET checkpoint_seq=CASE WHEN ? > checkpoint_seq THEN ? ELSE checkpoint_seq END WHERE id=?",
                                     (op["seq"], op["seq"], batch_id))
                except ApiError as exc:
                    self._mark_op_blocked(batch_id, op["id"], exc.code, exc.message,
                                          "plan_locked" if exc.code == "plan_locked" else ("concurrent_reassign" if exc.code == "concurrent_reassign" else "data_error"),
                                          op["flight_id"], op["resource_id"])
                    continue
                except sqlite3.DatabaseError as exc:
                    # 物理写入失败：回滚后记录失败原因，保留断点，等待重试
                    failure = {"code": "write_failed", "message": f"操作 {op['seq']} 写入失败: {exc}"}
                    break
            with self.repo.tx() as conn:
                batch = self._batch_row(conn, batch_id)
                if failure:
                    conn.execute("UPDATE offline_batches SET status='failed',error_code=?,error_message=? WHERE id=?", (failure["code"], failure["message"], batch_id))
                    Repository.audit(conn, batch["plan_id"], batch["created_by"], batch["created_role"], "batch_failed",
                                     {"batch_id": batch_id, "error": failure})
                    return self._batch_view(conn, self._batch_row(conn, batch_id))
                return self._finalize_batch(conn, batch_id)

    def _locked_plan_conflicts(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []
        for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
            items = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                AND a.new_std<? AND a.new_sta>?""",
                (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
            for item in items:
                conflicts.append({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"],
                                  "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]})
        return conflicts

    def _finalize_batch(self, conn: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
        batch = self._batch_row(conn, batch_id)
        plan_id = batch["plan_id"]
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        blocked = conn.execute("SELECT COUNT(*) c FROM batch_operations WHERE batch_id=? AND status='blocked'", (batch_id,)).fetchone()["c"]
        applied = conn.execute("SELECT COUNT(*) c FROM batch_operations WHERE batch_id=? AND status='applied'", (batch_id,)).fetchone()["c"]
        # 重新校验前清掉本批次上一轮的整方案待处理冲突，按当前合并结果重建；操作级冲突（并发改派等）保留
        conn.execute("DELETE FROM pending_conflicts WHERE batch_id=? AND status='open' AND source='finalize'", (batch_id,))
        validation: list[dict[str, Any]] = []
        locked_conflicts: list[dict[str, Any]] = []
        if plan["status"] == "draft" and conn.execute("SELECT COUNT(*) c FROM assignments WHERE plan_id=?", (plan_id,)).fetchone()["c"]:
            validation = self._validate_plan(conn, plan_id)
            locked_conflicts = self._locked_plan_conflicts(conn, plan_id)
        touched = {r["flight_id"] for r in conn.execute("SELECT DISTINCT flight_id FROM batch_operations WHERE batch_id=? AND flight_id IS NOT NULL AND status='applied'", (batch_id,))}
        # validation 问题挂的是 assignments 主键，重叠问题可能携带两个 assignment；统一映射回航班
        asg_to_flight = {r["id"]: r["flight_id"] for r in conn.execute("SELECT id,flight_id FROM assignments WHERE plan_id=?", (plan_id,))}

        def _resolve_flight(problem: dict[str, Any]) -> int | None:
            ids = problem.get("assignments") or ([problem["assignment_id"]] if problem.get("assignment_id") is not None else [])
            for asg_id in ids:
                if asg_id in asg_to_flight:
                    return asg_to_flight[asg_id]
            return None
        seen: set[tuple[str, int | None, str | None]] = set()
        conflict_count = 0
        for problem in validation:
            flight_id = _resolve_flight(problem)
            if flight_id is not None and flight_id not in touched:
                continue  # 与本批次无关的历史问题只在校验结果中报告，不进待处理冲突
            key = ("validation", flight_id, json.dumps(problem, ensure_ascii=False, sort_keys=True))
            if key in seen:
                continue
            seen.add(key)
            conn.execute("""INSERT INTO pending_conflicts(batch_id,op_id,plan_id,flight_id,conflict_type,resource,message,source,created_at)
                            VALUES(?,?,?,?,?,?,?,?,?)""",
                         (batch_id, None, plan_id, flight_id, "validation", problem.get("resource"),
                          f"合并后重新校验未通过: {problem.get('code')}", "finalize", iso()))
            conflict_count += 1
        for item in locked_conflicts:
            flight_id = asg_to_flight.get(item["assignment_id"])
            key = ("locked_resource", flight_id, item["resource"])
            if key in seen:
                continue
            seen.add(key)
            conn.execute("""INSERT INTO pending_conflicts(batch_id,op_id,plan_id,flight_id,conflict_type,resource,message,source,created_at)
                            VALUES(?,?,?,?,?,?,?,?,?)""",
                         (batch_id, None, plan_id, flight_id, "locked_resource", item["resource"],
                                          f"与已锁定方案 #{item['conflict_plan_id']}（{item['conflict_plan']}）的资源 {item['resource']} 时间重叠", "finalize", iso()))
            conflict_count += 1
        final_revision = plan["revision"]
        result = {"applied_operations": applied, "blocked_operations": blocked, "validation_problems": validation,
                  "locked_resource_conflicts": locked_conflicts, "revision": final_revision, "conflicts": conflict_count}
        if blocked or conflict_count:
            status = "conflict"
        else:
            status = "applied"
            conn.execute("UPDATE offline_batches SET merged_revision=? WHERE id=?", (final_revision, batch_id))
        conn.execute("UPDATE offline_batches SET status=?,processed_at=?,error_code=NULL,error_message=?,result_json=? WHERE id=?",
                     (status, iso(), None, json.dumps(result, ensure_ascii=False, sort_keys=True), batch_id))
        Repository.audit(conn, plan_id, batch["created_by"], batch["created_role"], f"batch_{status}",
                         {"batch_id": batch_id, "result": result})
        return self._batch_view(conn, self._batch_row(conn, batch_id))

    def retry_batch(self, batch_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}:
            raise ApiError(403, "batch_forbidden", "当前角色不能重试离线批次")
        resolutions = body.get("resolutions", [])
        if not isinstance(resolutions, list):
            raise ApiError(400, "invalid_resolutions", "resolutions 必须是数组")
        with self.repo.tx() as conn:
            batch = self._batch_row(conn, batch_id)
            if batch["status"] not in {"failed", "conflict"}:
                raise ApiError(409, "batch_not_retryable", f"批次状态 {batch['status']} 不能重试")
            by_flight: dict[int, dict[str, Any]] = {}
            for resolution in resolutions:
                if not isinstance(resolution, dict) or not isinstance(resolution.get("flight_id"), int):
                    raise ApiError(400, "invalid_resolution", "每个 resolution 必须包含整数 flight_id")
                entry = dict(resolution.get("payload", {}))
                entry["flight_id"] = resolution["flight_id"]
                entry["force"] = True
                by_flight[resolution["flight_id"]] = entry
                conn.execute("""UPDATE pending_conflicts SET status='resolved',resolved_at=?,resolution_json=?
                                WHERE batch_id=? AND flight_id=? AND status='open'""",
                             (iso(), json.dumps(resolution, ensure_ascii=False, sort_keys=True), batch_id, resolution["flight_id"]))
            # resolution 只给 flight_id 表示采用该操作离线时的原 payload
            for flight_id, entry in list(by_flight.items()):
                if "new_std" not in entry:
                    original = conn.execute("""SELECT payload_json FROM batch_operations WHERE batch_id=? AND flight_id=? ORDER BY seq DESC LIMIT 1""",
                                            (batch_id, flight_id)).fetchone()
                    if not original:
                        raise ApiError(400, "invalid_resolution", f"批次中找不到航班 {flight_id} 的离线操作，必须提供完整 payload")
                    merged = json.loads(original["payload_json"])
                    merged.update(entry)
                    by_flight[flight_id] = merged
            for op in conn.execute("SELECT * FROM batch_operations WHERE batch_id=? AND status IN ('blocked','failed','pending')", (batch_id,)).fetchall():
                if op["flight_id"] in by_flight:
                    conn.execute("UPDATE batch_operations SET payload_json=?,status='pending',error_code=NULL,error_message=NULL WHERE id=?",
                                 (json.dumps(by_flight[op["flight_id"]], ensure_ascii=False, sort_keys=True), op["id"]))
                else:
                    conn.execute("UPDATE batch_operations SET status='pending',error_code=NULL,error_message=NULL WHERE id=?", (op["id"],))
            conn.execute("UPDATE offline_batches SET status='queued',error_code=NULL,error_message=NULL WHERE id=?", (batch_id,))
            Repository.audit(conn, batch["plan_id"], actor, role, "batch_retried",
                             {"batch_id": batch_id, "resolution_count": len(by_flight)})
        return self._process_batch(batch_id)

    def _batch_view(self, conn: sqlite3.Connection, batch: sqlite3.Row) -> dict[str, Any]:
        result = dict(batch)
        result["operations"] = [dict(r) for r in conn.execute("SELECT id,seq,op_type,flight_id,resource_id,status,error_code,error_message,applied_at FROM batch_operations WHERE batch_id=? ORDER BY seq", (batch["id"],))]
        result["pending_conflicts"] = [dict(r) for r in conn.execute("SELECT * FROM pending_conflicts WHERE batch_id=? ORDER BY id", (batch["id"],))]
        result["result"] = json.loads(batch["result_json"]) if batch["result_json"] else None
        return result

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        with self.repo.shared() as conn:
            return self._batch_view(conn, self._batch_row(conn, batch_id))

    def list_batches(self, plan_id: int | None = None) -> dict[str, Any]:
        with self.repo.shared() as conn:
            if plan_id is not None:
                rows = conn.execute("SELECT * FROM offline_batches WHERE plan_id=? ORDER BY id DESC", (plan_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM offline_batches ORDER BY id DESC LIMIT 50").fetchall()
            open_conflicts = conn.execute("SELECT COUNT(*) c FROM pending_conflicts WHERE status='open'").fetchone()["c"]
            return {"batches": [self._batch_view(conn, row) for row in rows], "open_conflicts": open_conflicts}

    def recover_queue(self) -> dict[str, Any]:
        """进程崩溃后重启：把中断在 processing 的批次复位为 queued，并续跑队列与待处理冲突。"""
        with self._batch_lock:
            recovered: list[int] = []
            with self.repo.tx() as conn:
                rows = conn.execute("SELECT id FROM offline_batches WHERE status='processing' ORDER BY id").fetchall()
                for row in rows:
                    conn.execute("UPDATE offline_batches SET status='queued' WHERE id=?", (row["id"],))
                    recovered.append(row["id"])
        for batch_id in recovered:
            self._process_batch(batch_id)
        return {"recovered_batches": recovered}

    def _validate_plan(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no, f.origin, f.destination, f.passenger_count, f.status flight_status
                                                  FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        if not rows: raise ApiError(409, "empty_plan", "方案没有飞行调整")
        problems: list[dict[str, Any]] = []
        by_aircraft: dict[str, list[dict[str, Any]]] = {}
        by_crew: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row["status"] == "canceled": continue
            std, sta = parse_time(row["new_std"]), parse_time(row["new_sta"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (row["aircraft_id"],)).fetchone()
            crew = conn.execute("SELECT * FROM crew WHERE id=?", (row["crew_id"],)).fetchone()
            origin = conn.execute("SELECT * FROM airports WHERE code=?", (row["origin"],)).fetchone()
            destination = conn.execute("SELECT * FROM airports WHERE code=?", (row["destination"],)).fetchone()
            if not aircraft or aircraft["status"] != "active": problems.append({"assignment_id": row["id"], "code": "aircraft_unavailable"})
            elif parse_time(aircraft["maintenance_due"]) < sta: problems.append({"assignment_id": row["id"], "code": "maintenance_due", "resource": aircraft["id"]})
            if not crew or crew["status"] != "active": problems.append({"assignment_id": row["id"], "code": "crew_unavailable"})
            if not origin or not destination: problems.append({"assignment_id": row["id"], "code": "airport_unknown"})
            if crew:
                duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
                if (sta - duty_start).total_seconds() / 60 > max_duty: problems.append({"assignment_id": row["id"], "code": "duty_limit", "resource": crew["id"]})
            if destination:
                curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
                permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                      (row["origin"], row["destination"], row["new_sta"], row["new_sta"])).fetchone()
                arrival_clock = sta.timetz().replace(tzinfo=None)
                inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
                if inside and not (permit and permit["curfew_exempt"]): problems.append({"assignment_id": row["id"], "code": "airport_curfew"})
                if row["origin"] != row["destination"] and not permit: problems.append({"assignment_id": row["id"], "code": "route_permit_missing"})
                elif row["origin"] != row["destination"] and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                    problems.append({"assignment_id": row["id"], "code": "route_permit_window"})
            by_aircraft.setdefault(row["aircraft_id"], []).append(row)
            by_crew.setdefault(row["crew_id"], []).append(row)
        for bucket_name, buckets in (("aircraft", by_aircraft), ("crew", by_crew)):
            for resource, items in buckets.items():
                for i, left in enumerate(items):
                    for right in items[i + 1:]:
                        if overlaps(parse_time(left["new_std"]), parse_time(left["new_sta"]), parse_time(right["new_std"]), parse_time(right["new_sta"])):
                            problems.append({"code": f"{bucket_name}_overlap", "resource": resource, "assignments": [left["id"], right["id"]]})
        return problems

    def validate_plan(self, plan_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "validate_forbidden", "当前角色不能校验方案")
        with self.repo.tx() as conn:
            problems = self._validate_plan(conn, plan_id)
            if not problems:
                metrics = self._metrics(conn, plan_id)
                conn.execute("UPDATE recovery_plans SET metrics_json=?,score_json=? WHERE id=?", (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), plan_id))
            return {"valid": not problems, "problems": problems, "plan": self.get_plan(plan_id)}

    def _metrics(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        rows = conn.execute("""SELECT a.*,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)).fetchall()
        canceled = sum(1 for row in rows if row["status"] == "canceled")
        return {"flight_count": len(rows), "canceled": canceled, "total_delay_minutes": sum(max(0, row["delay_minutes"]) for row in rows),
                "affected_passengers": sum(row["passenger_count"] for row in rows), "missed_connections": sum(row["missed_connections"] for row in rows)}

    @staticmethod
    def _score(metrics: dict[str, Any]) -> dict[str, int]:
        score = metrics["canceled"] * 100000 + metrics["missed_connections"] * 5000 + metrics["total_delay_minutes"] * 100 + metrics["affected_passengers"]
        return {"cost_score": score, "lower_is_better": 1}

    def lock_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "lock_forbidden", "只有运行经理可以锁定恢复方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "locked": return self.get_plan(plan_id)
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案版本已变化")
            problems = self._validate_plan(conn, plan_id)
            if problems: raise ApiError(409, "plan_invalid", "方案未通过约束校验", problems)
            conflicts = []
            for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
                conflicting = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                    WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                    AND a.new_std<? AND a.new_sta>?""",
                    (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
                conflicts.extend({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"], "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]} for item in conflicting)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "与已锁定方案存在飞机或机组冲突", conflicts)
            metrics = self._metrics(conn, plan_id)
            conn.execute("""UPDATE recovery_plans SET status='locked',metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            for row in conn.execute("""SELECT a.*,f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)):
                conn.execute("UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,updated_at=? WHERE id=?",
                             (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]), iso(), row["flight_id"]))
                conn.execute("UPDATE assignments SET status='active' WHERE id=?", (row["id"],))
            Repository.audit(conn, plan_id, actor, role, "plan_locked", {"metrics": metrics})
            return self.get_plan(plan_id)

    def cancel_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消航班")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == "canceled": return {"flight": dict(flight), "idempotent": True}
            conn.execute("UPDATE flights SET status='canceled',cancel_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_canceled", {"flight_id": flight_id, "reason": reason})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()), "idempotent": False}

    def recover_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "recover_forbidden", "当前角色不能恢复航班")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise ApiError(409, "revision_conflict", "航班版本已变化")
            if flight["status"] != "canceled": raise ApiError(409, "not_canceled", "只有取消航班可以恢复")
            std, sta = parse_time(body.get("new_std")), parse_time(body.get("new_sta"))
            if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
            aircraft_id, crew_id = body.get("aircraft_id", flight["aircraft_id"]), body.get("crew_id", flight["crew_id"])
            conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,cancel_reason=NULL,
                            delay_minutes=0,revision=revision+1,updated_at=? WHERE id=?""",
                         (iso(std), iso(sta), aircraft_id, crew_id, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_recovered", {"flight_id": flight_id})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())}

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        with self.repo.shared() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            assignments = [dict(r) for r in conn.execute("""SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
            result = dict(plan)
            result["metrics"] = json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id)
            result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
            result["assignments"] = assignments
            return result

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        with self.repo.shared() as conn:
            ids = [r["id"] for r in conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,))]
        plans = []
        for plan_id in ids:
            plan = self.get_plan(plan_id)
            with self.repo.shared() as conn:
                if not plan["score"]:
                    problems = self._validate_plan(conn, plan_id)
                    plan["valid"] = not problems
                else:
                    plan["valid"] = True
            plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def state(self) -> dict[str, Any]:
        with self.repo.shared() as conn:
            flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
            disruptions = [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")]
            plan_ids = [r["id"] for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
            batch_rows = conn.execute("SELECT * FROM offline_batches ORDER BY id DESC LIMIT 20").fetchall()
            open_conflicts = [dict(r) for r in conn.execute("SELECT * FROM pending_conflicts WHERE status='open' ORDER BY id DESC LIMIT 50")]
            queue_summary = {
                "queued": conn.execute("SELECT COUNT(*) c FROM offline_batches WHERE status='queued'").fetchone()["c"],
                "processing": conn.execute("SELECT COUNT(*) c FROM offline_batches WHERE status='processing'").fetchone()["c"],
                "failed": conn.execute("SELECT COUNT(*) c FROM offline_batches WHERE status='failed'").fetchone()["c"],
                "conflict": conn.execute("SELECT COUNT(*) c FROM offline_batches WHERE status='conflict'").fetchone()["c"],
                "open_conflicts": len(open_conflicts),
            }
        plans = [self.get_plan(pid) for pid in plan_ids]
        batches = [self._batch_view(self.repo.conn, row) for row in batch_rows]
        return {"flights": flights, "disruptions": disruptions, "plans": plans,
                "offline_batches": batches, "open_conflicts": open_conflicts,
                "queue_summary": queue_summary, "server_time": iso()}


def respond(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: AirlineRecoveryService
    web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length: return {}
        try: value = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        if path == "/api/batches": return 200, self.service.list_batches(None)
        if len(parts) == 3 and parts[:2] == ["api", "batches"] and parts[2].isdigit(): return 200, self.service.get_batch(int(parts[2]))
        if len(parts) == 5 and parts[:2] == ["api", "plans"] and parts[2].isdigit() and parts[3] == "batches": return 200, self.service.list_batches(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        table = {
            "/api/airports": lambda: (201, self.service.seed_airport(actor, role, body)),
            "/api/aircraft": lambda: (201, self.service.seed_aircraft(actor, role, body)),
            "/api/crew": lambda: (201, self.service.seed_crew(actor, role, body)),
            "/api/permits": lambda: (201, self.service.create_permit(actor, role, body)),
            "/api/flights": lambda: (201, self.service.create_flight(actor, role, body)),
            "/api/disruptions": lambda: (201, self.service.create_disruption(actor, role, body)),
            "/api/recovery-plans": lambda: (201, self.service.create_plan(actor, role, body)),
            "/api/offline-batches": lambda: (202, self.service.submit_batch(actor, role, body)),
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[2].isdigit() and parts[3] == "retry":
            return 202, self.service.retry_batch(int(parts[2]), actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "queues"] and parts[2] == "recover" and parts[3] == "offline":
            if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "queue_forbidden", "当前角色不能恢复离线队列")
            return 200, self.service.recover_queue()
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
            respond(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            respond(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}"); respond(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = AirlineRecoveryService(db_path)
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()

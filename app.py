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
        self._init()

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

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
                missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id)
            );
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        self._migrate()

    # 离线批次相关表在 v2 迁移中建立，迁移只新增表、不动旧表数据，
    # 以保留原方案、修订号与审计记录。
    def _migrate(self) -> None:
        row = self.conn.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()
        version = int(row["value"]) if row else 1
        if version >= 2:
            return
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS offline_batches(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_key TEXT NOT NULL UNIQUE,
                plan_id INTEGER NOT NULL REFERENCES recovery_plans(id),
                baseline_revision INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                checkpoint INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                submitted_by TEXT NOT NULL,
                submitted_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                result_json TEXT,
                error_json TEXT
            );
            CREATE TABLE IF NOT EXISTS batch_operations(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES offline_batches(id) ON DELETE CASCADE,
                seq INTEGER NOT NULL,
                op TEXT NOT NULL DEFAULT 'reassign',
                flight_id INTEGER NOT NULL REFERENCES flights(id),
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                result_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(batch_id, seq)
            );
            CREATE TABLE IF NOT EXISTS batch_conflicts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES offline_batches(id) ON DELETE CASCADE,
                operation_id INTEGER REFERENCES batch_operations(id) ON DELETE CASCADE,
                flight_id INTEGER REFERENCES flights(id),
                code TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                resolution_note TEXT,
                resolved_by TEXT,
                resolved_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_batch_ops_batch ON batch_operations(batch_id, seq);
            CREATE INDEX IF NOT EXISTS idx_batches_plan_status ON offline_batches(plan_id, status);
            CREATE INDEX IF NOT EXISTS idx_batch_conflicts_status ON batch_conflicts(batch_id, status);
            """
        )
        self.conn.execute("INSERT OR IGNORE INTO schema_meta(key,value) VALUES('version','2')")
        Repository.audit(self.conn, None, "system", "system", "schema_upgraded", {"from": version, "to": 2})

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

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
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,missed_connections=?
                                WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed))
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
        conn = self.repo.conn
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        assignments = [dict(r) for r in conn.execute("""SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        result = dict(plan)
        result["metrics"] = json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id)
        result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
        result["assignments"] = assignments
        return result

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        plans = []
        for row in self.repo.conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,)):
            plan = self.get_plan(row["id"])
            if not plan["score"]:
                problems = self._validate_plan(self.repo.conn, row["id"])
                plan["valid"] = not problems
            else:
                plan["valid"] = True
            plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def submit_batch(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "batch_forbidden", "当前角色不能提交离线批次")
        batch_key = str(body.get("batch_key", "")).strip()
        baseline = body.get("baseline_revision")
        operations = body.get("operations", [])
        if not batch_key: raise ApiError(400, "batch_key_required", "batch_key 必填且用于幂等重试")
        if not isinstance(baseline, int): raise ApiError(400, "revision_required", "baseline_revision 必须是整数")
        if not isinstance(operations, list) or not operations: raise ApiError(400, "invalid_batch", "operations 必须是非空列表")
        normalized: list[dict[str, Any]] = []
        seen_seq: set[int] = set()
        for item in operations:
            if not isinstance(item, dict): raise ApiError(400, "invalid_operation", "每个操作必须是对象")
            seq = item.get("seq")
            if not isinstance(seq, int) or seq in seen_seq: raise ApiError(400, "invalid_seq", "seq 必须是批次内唯一整数")
            seen_seq.add(seq)
            flight_id = item.get("flight_id")
            if not isinstance(flight_id, int): raise ApiError(400, "invalid_operation", "flight_id 必须是整数")
            payload = {"aircraft_id": str(item.get("aircraft_id", "")).strip(), "crew_id": str(item.get("crew_id", "")).strip(),
                       "new_std": item.get("new_std"), "new_sta": item.get("new_sta"),
                       "missed_connections": item.get("missed_connections", 0), "status": item.get("status", "planned")}
            if not payload["aircraft_id"] or not payload["crew_id"] or not payload["new_std"] or not payload["new_sta"]:
                raise ApiError(400, "invalid_operation", "改派缺少 aircraft_id、crew_id、new_std 或 new_sta")
            normalized.append({"seq": seq, "op": str(item.get("op", "reassign")), "flight_id": flight_id, "payload": payload})
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能接收离线批次")
            existing = conn.execute("SELECT * FROM offline_batches WHERE batch_key=?", (batch_key,)).fetchone()
            if existing:
                batch_id = existing["id"]
            else:
                cur = conn.execute("""INSERT INTO offline_batches(batch_key,plan_id,baseline_revision,status,submitted_by,submitted_at,updated_at)
                                     VALUES(?,?,?,?,?,?,?)""", (batch_key, plan_id, baseline, "queued", actor, iso(), iso()))
                batch_id = cur.lastrowid
                for op in normalized:
                    conn.execute("""INSERT INTO batch_operations(batch_id,seq,op,flight_id,payload_json,status,created_at)
                                    VALUES(?,?,?,?,?,?,?)""",
                                 (batch_id, op["seq"], op["op"], op["flight_id"], json.dumps(op["payload"], ensure_ascii=False, sort_keys=True), "pending", iso()))
                Repository.audit(conn, plan_id, actor, role, "batch_submitted",
                                {"batch_id": batch_id, "batch_key": batch_key, "operation_count": len(normalized), "baseline_revision": baseline})
        self._process_batch(batch_id)
        return self.get_batch(batch_id)

    def list_batches(self, plan_id: int) -> list[dict[str, Any]]:
        conn = self.repo.conn
        if not conn.execute("SELECT 1 FROM recovery_plans WHERE id=?", (plan_id,)).fetchone():
            raise ApiError(404, "plan_not_found", "方案不存在")
        rows = conn.execute("""SELECT b.*,
                                      (SELECT COUNT(*) FROM batch_operations o WHERE o.batch_id=b.id) operation_count,
                                      (SELECT COUNT(*) FROM batch_conflicts c WHERE c.batch_id=b.id AND c.status='pending') pending_conflict_count
                               FROM offline_batches b WHERE b.plan_id=? ORDER BY b.id DESC""", (plan_id,)).fetchall()
        return [self._batch_summary(dict(r)) for r in rows]

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        conn = self.repo.conn
        batch = conn.execute("SELECT * FROM offline_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch: raise ApiError(404, "batch_not_found", "离线批次不存在")
        result = self._batch_summary(dict(batch))
        result["operations"] = [dict(r) for r in conn.execute("SELECT * FROM batch_operations WHERE batch_id=? ORDER BY seq", (batch_id,))]
        result["conflicts"] = [dict(r) for r in conn.execute("SELECT * FROM batch_conflicts WHERE batch_id=? ORDER BY id", (batch_id,))]
        return result

    @staticmethod
    def _batch_summary(batch: dict[str, Any]) -> dict[str, Any]:
        result = dict(batch)
        result["result"] = json.loads(batch["result_json"]) if batch.get("result_json") else None
        result["error"] = json.loads(batch["error_json"]) if batch.get("error_json") else None
        result.pop("result_json", None); result.pop("error_json", None)
        return result

    def retry_batch(self, batch_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "batch_forbidden", "当前角色不能重试批次")
        conn = self.repo.conn
        batch = conn.execute("SELECT * FROM offline_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch: raise ApiError(404, "batch_not_found", "离线批次不存在")
        if batch["status"] not in ("failed", "conflict"):
            raise ApiError(409, "batch_not_retryable", "只有失败或存在待处理冲突的批次可以重试")
        Repository.audit(conn, batch["plan_id"], actor, role, "batch_retried", {"batch_id": batch_id})
        self._process_batch(batch_id)
        return self.get_batch(batch_id)

    def resolve_conflict(self, batch_id: int, conflict_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "batch_forbidden", "当前角色不能处理冲突")
        resolution = str(body.get("resolution", "")).strip()
        note = str(body.get("note", "")).strip()
        if resolution not in {"ignored", "resolved"}: raise ApiError(400, "invalid_resolution", "resolution 必须是 ignored 或 resolved")
        with self.repo.tx() as conn:
            conflict = conn.execute("SELECT * FROM batch_conflicts WHERE id=? AND batch_id=?", (conflict_id, batch_id)).fetchone()
            if not conflict: raise ApiError(404, "conflict_not_found", "待处理冲突不存在")
            batch = conn.execute("SELECT plan_id FROM offline_batches WHERE id=?", (batch_id,)).fetchone()
            conn.execute("UPDATE batch_conflicts SET status=?,resolution_note=?,resolved_by=?,resolved_at=? WHERE id=?",
                         (resolution, note, actor, iso(), conflict_id))
            Repository.audit(conn, batch["plan_id"] if batch else None, actor, role,
                            "batch_conflict_resolved", {"batch_id": batch_id, "conflict_id": conflict_id, "resolution": resolution})
        return self.get_batch(batch_id)

    def _apply_operation(self, conn: sqlite3.Connection, plan_id: int, op: dict[str, Any]) -> None:
        payload = json.loads(op["payload_json"])
        flight_id = op["flight_id"]
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(payload["new_std"]), parse_time(payload["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        if flight["status"] == "canceled" and payload.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(payload.get("missed_connections", 0))
        if missed < 0: raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(plan_id,flight_id) DO UPDATE SET aircraft_id=excluded.aircraft_id,crew_id=excluded.crew_id,
                        new_std=excluded.new_std,new_sta=excluded.new_sta,status=excluded.status,delay_minutes=excluded.delay_minutes,
                        missed_connections=excluded.missed_connections""",
                     (plan_id, flight_id, payload["aircraft_id"], payload["crew_id"], iso(std), iso(sta),
                      payload.get("status", "planned"), delay, missed))

    def _locked_resource_conflicts(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id
                                                 WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)).fetchall()]
        conflicts: list[dict[str, Any]] = []
        for row in rows:
            for item in conn.execute("""SELECT a.*, p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                                         WHERE p.id!=? AND p.status='locked' AND a.status!='canceled'
                                         AND (a.aircraft_id=? OR a.crew_id=?) AND a.new_std<? AND a.new_sta>?""",
                                     (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall():
                resource = item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]
                conflicts.append({"assignment_id": row["id"], "flight_id": row["flight_id"], "flight_no": row["flight_no"],
                                  "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"], "resource": resource,
                                  "new_std": row["new_std"], "new_sta": row["new_sta"]})
        return conflicts

    def _process_batch(self, batch_id: int) -> None:
        conn = self.repo.conn
        batch = conn.execute("SELECT * FROM offline_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch or batch["status"] == "merged": return
        plan_id = batch["plan_id"]
        conn.execute("UPDATE offline_batches SET status='processing',attempts=attempts+1,updated_at=? WHERE id=?", (iso(), batch_id))
        conn.commit()
        ops = [dict(r) for r in conn.execute("SELECT * FROM batch_operations WHERE batch_id=? ORDER BY seq", (batch_id,)).fetchall()]
        # 同一航班只认最后一次改派：按 flight_id 取 seq 最大的 reassign。
        effective: dict[int, dict[str, Any]] = {}
        for op in ops:
            if op["op"] != "reassign": continue
            if op["flight_id"] not in effective or op["seq"] > effective[op["flight_id"]]["seq"]:
                effective[op["flight_id"]] = op
        for op in ops:
            if op["status"] != "pending": continue
            eff = effective.get(op["flight_id"])
            if not eff or eff["id"] != op["id"]:
                conn.execute("UPDATE batch_operations SET status='superseded' WHERE id=?", (op["id"],))
        conn.commit()
        checkpoint = batch["checkpoint"]
        for op in sorted(effective.values(), key=lambda item: item["seq"]):
            if op["status"] == "applied" and op["seq"] <= checkpoint:
                continue
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._apply_operation(conn, plan_id, op)
                conn.execute("UPDATE batch_operations SET status='applied',result_json=? WHERE id=?",
                             (json.dumps({"applied": True}, ensure_ascii=False), op["id"]))
                conn.execute("UPDATE offline_batches SET checkpoint=?,updated_at=? WHERE id=?", (op["seq"], iso(), batch_id))
                conn.commit()
            except ApiError as exc:
                conn.execute("ROLLBACK")
                conn.execute("UPDATE batch_operations SET status='failed',result_json=? WHERE id=?",
                             (json.dumps({"code": exc.code, "message": exc.message}, ensure_ascii=False), op["id"]))
                conn.execute("UPDATE offline_batches SET status='failed',error_json=?,updated_at=? WHERE id=?",
                             (json.dumps({"code": exc.code, "message": exc.message, "operation_seq": op["seq"]}, ensure_ascii=False), iso(), batch_id))
                conn.commit()
                return
            except Exception as exc:
                conn.execute("ROLLBACK")
                conn.execute("UPDATE offline_batches SET status='failed',error_json=?,updated_at=? WHERE id=?",
                             (json.dumps({"code": "internal_error", "message": str(exc), "operation_seq": op["seq"]}, ensure_ascii=False), iso(), batch_id))
                conn.commit()
                return
        # 合并后重新校验飞机、机组、航线许可，并检查锁定方案资源冲突。
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM batch_conflicts WHERE batch_id=?", (batch_id,))
        try:
            problems = self._validate_plan(conn, plan_id)
        except ApiError as exc:
            problems = [{"code": exc.code, "message": exc.message}]
        locked = self._locked_resource_conflicts(conn, plan_id)
        if problems or locked:
            for problem in problems:
                conn.execute("""INSERT INTO batch_conflicts(batch_id,operation_id,flight_id,code,detail_json,status,created_at)
                                VALUES(?,?,?,?,?,?,?)""",
                             (batch_id, None, problem.get("flight_id"), problem["code"],
                              json.dumps(problem, ensure_ascii=False), "pending", iso()))
            for conflict in locked:
                conn.execute("""INSERT INTO batch_conflicts(batch_id,operation_id,flight_id,code,detail_json,status,created_at)
                                VALUES(?,?,?,?,?,?,?)""",
                             (batch_id, None, conflict["flight_id"], "locked_resource_conflict",
                              json.dumps(conflict, ensure_ascii=False), "pending", iso()))
            superseded = len(ops) - len(effective)
            conn.execute("UPDATE offline_batches SET status='conflict',result_json=?,updated_at=? WHERE id=?",
                         (json.dumps({"merged": False, "validation_problems": len(problems), "locked_conflicts": len(locked),
                                      "applied_operations": len(effective), "superseded": superseded}, ensure_ascii=False), iso(), batch_id))
        else:
            superseded = len(ops) - len(effective)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            conn.execute("UPDATE offline_batches SET status='merged',result_json=?,updated_at=? WHERE id=?",
                         (json.dumps({"merged": True, "applied_operations": len(effective), "superseded": superseded}, ensure_ascii=False), iso(), batch_id))
            Repository.audit(conn, plan_id, "system", "system", "batch_merged",
                             {"batch_id": batch_id, "applied_operations": len(effective), "superseded": superseded})
        conn.commit()

    def recover_pending_batches(self) -> None:
        conn = self.repo.conn
        rows = conn.execute("SELECT id FROM offline_batches WHERE status IN ('queued','processing') ORDER BY id").fetchall()
        for row in rows:
            try:
                self._process_batch(row["id"])
            except Exception as exc:
                print(f"recover batch {row['id']} failed: {exc!r}")

    def state(self) -> dict[str, Any]:
        conn = self.repo.conn
        flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
        plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
        pending_batches = [dict(r) for r in conn.execute("""SELECT b.*, p.name plan_name FROM offline_batches b
                                                             JOIN recovery_plans p ON p.id=b.plan_id
                                                             WHERE b.status IN ('queued','processing','conflict','failed')
                                                             ORDER BY b.id DESC LIMIT 50""")]
        pending_conflicts = [dict(r) for r in conn.execute("""SELECT c.*, b.plan_id FROM batch_conflicts c
                                                              JOIN offline_batches b ON b.id=c.batch_id
                                                              WHERE c.status='pending' ORDER BY c.id DESC LIMIT 100""")]
        return {"flights": flights, "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")], "plans": plans,
                "pending_batches": pending_batches, "pending_conflicts": pending_conflicts, "server_time": iso()}


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
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit() and parts[3] == "batches": return 200, self.service.list_batches(int(parts[2]))
        if len(parts) == 3 and parts[:2] == ["api", "batches"] and parts[2].isdigit(): return 200, self.service.get_batch(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
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
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
            if action == "batches": return 201, self.service.submit_batch(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[2].isdigit():
            batch_id, action = int(parts[2]), parts[3]
            if action == "retry": return 200, self.service.retry_batch(batch_id, actor, role)
        if len(parts) == 6 and parts[:2] == ["api", "batches"] and parts[2].isdigit() and parts[3] == "conflicts" and parts[4].isdigit() and parts[5] == "resolve":
            return 200, self.service.resolve_conflict(int(parts[2]), int(parts[4]), actor, role, body)
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
    # 进程崩溃后重启：后台恢复上次未处理完的离线批次队列与待处理冲突。
    threading.Thread(target=service.recover_pending_batches, daemon=True).start()
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()

import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class OfflineBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.svc = AirlineRecoveryService(self.db)
        base = utcnow() + timedelta(days=1)
        self.base = base
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "00:00", "curfew_end": "00:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "00:00", "curfew_end": "00:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.f1 = self.make_flight("AB100", "AC1", "CR1")
        self.f2 = self.make_flight("AB200", "AC1", "CR1", hours=6)
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(base - timedelta(hours=1)), "ends_at": iso(base + timedelta(hours=3))})
        self.plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "恢复方案", "assignments": [
            {"flight_id": self.f1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(base + timedelta(hours=3)), "new_sta": iso(base + timedelta(hours=5))}]})
        self.pid = self.plan["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def make_flight(self, number, aircraft, crew, hours=0):
        start = self.base + timedelta(hours=hours)
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB",
                                                             "std": iso(start), "sta": iso(start + timedelta(hours=2)),
                                                             "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def reassign(self, flight_id, aircraft, crew, hours, missed=0):
        return {"flight_id": flight_id, "aircraft_id": aircraft, "crew_id": crew,
                "new_std": iso(self.base + timedelta(hours=hours)), "new_sta": iso(self.base + timedelta(hours=hours + 2)),
                "missed_connections": missed}

    def test_same_flight_last_assignment_wins(self):
        batch = self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC1", "CR1", 4)},
            {"seq": 2, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC2", "CR2", 6)}]})
        self.assertEqual(batch["status"], "applied")
        statuses = {op["seq"]: op["status"] for op in batch["operations"]}
        self.assertEqual(statuses, {1: "superseded", 2: "applied"})
        assignment = next(a for a in self.svc.get_plan(self.pid)["assignments"] if a["flight_id"] == self.f1["id"])
        self.assertEqual(assignment["new_std"], iso(self.base + timedelta(hours=6)))
        self.assertEqual(assignment["changed_revision"], 2)

    def test_two_dispatchers_late_batch_sees_conflict(self):
        first = self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC2", "CR2", 4)}]})
        self.assertEqual(first["status"], "applied")
        # 后到者仍以 r1 为基线：同航班已被 Alice 改为 r2 -> 待处理冲突；另一个航班未冲突 -> 自动合并
        late = self.svc.submit_batch("bob", "scheduler", {"plan_id": self.pid, "device_id": "dev-B", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC1", "CR1", 5)},
            {"seq": 2, "op_type": "reassign", "payload": self.reassign(self.f2["id"], "AC2", "CR2", 8)}]})
        self.assertEqual(late["status"], "conflict")
        blocked = [op for op in late["operations"] if op["status"] == "blocked"]
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["error_code"], "concurrent_reassign")
        self.assertTrue(any(c["conflict_type"] == "concurrent_reassign" and c["flight_id"] == self.f1["id"]
                            for c in late["pending_conflicts"]))
        applied_flights = {op["flight_id"] for op in late["operations"] if op["status"] == "applied"}
        self.assertIn(self.f2["id"], applied_flights)
        self.assertNotIn(self.f1["id"], applied_flights)
        plan = self.svc.get_plan(self.pid)
        self.assertEqual(plan["revision"], 3)
        f1_assignment = next(a for a in plan["assignments"] if a["flight_id"] == self.f1["id"])
        self.assertEqual(f1_assignment["new_std"], iso(self.base + timedelta(hours=4)))  # 未被晚到内容覆盖
        # 调度员在控制台决定采用后到方案后重试
        retried = self.svc.retry_batch(late["id"], "bob", "scheduler", {"resolutions": [
            {"flight_id": self.f1["id"], "payload": self.reassign(self.f1["id"], "AC1", "CR1", 5)}]})
        self.assertEqual(retried["status"], "applied")
        f1_after = next(a for a in self.svc.get_plan(self.pid)["assignments"] if a["flight_id"] == self.f1["id"])
        self.assertEqual(f1_after["new_std"], iso(self.base + timedelta(hours=5)))
        self.assertTrue(all(c["status"] != "open" for c in retried["pending_conflicts"]))

    def test_resource_merge_then_revalidate(self):
        # 离线期间新增一架临近保养的飞机并改派到它；合并后重新校验应报保养到期
        batch = self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "upsert_aircraft",
             "payload": {"id": "AC9", "model": "A321", "maintenance_due": iso(self.base + timedelta(hours=4))}},
            {"seq": 2, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC9", "CR2", 3)}]})
        self.assertEqual(batch["status"], "conflict")
        self.assertEqual(batch["operations"][0]["status"], "applied")
        self.assertEqual(batch["operations"][1]["status"], "applied")
        codes = {c["conflict_type"]: c for c in batch["pending_conflicts"]}
        self.assertIn("validation", codes)
        self.assertIn("maintenance_due", codes["validation"]["message"])
        self.assertTrue(any(p["code"] == "maintenance_due" for p in batch["result"]["validation_problems"]))
        # 修正资源基线（推迟保养）：合并新资源后重新校验，待处理冲突消失
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC9", "model": "A321", "maintenance_due": iso(self.base + timedelta(days=9))})
        fixed = self.svc.retry_batch(batch["id"], "alice", "scheduler", {"resolutions": []})
        self.assertEqual(fixed["status"], "applied", fixed["pending_conflicts"])
        self.assertFalse(any(c["status"] == "open" for c in fixed["pending_conflicts"]))

    def test_locked_plan_resources_never_released_by_late_batch(self):
        self.svc.lock_plan(self.pid, "ops", "ops_manager", {"expected_revision": 1})
        revision_before = self.svc.get_plan(self.pid)["revision"]
        batch = self.svc.submit_batch("bob", "scheduler", {"plan_id": self.pid, "device_id": "dev-B", "base_revision": revision_before, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC1", "CR1", 9)}]})
        self.assertEqual(batch["status"], "conflict")
        self.assertEqual(batch["operations"][0]["status"], "blocked")
        self.assertEqual(batch["operations"][0]["error_code"], "plan_locked")
        self.assertTrue(any(c["conflict_type"] == "plan_locked" for c in batch["pending_conflicts"]))
        assignment = next(a for a in self.svc.get_plan(self.pid)["assignments"] if a["flight_id"] == self.f1["id"])
        self.assertEqual((assignment["aircraft_id"], assignment["crew_id"]), ("AC2", "CR2"))
        self.assertEqual(self.svc.get_plan(self.pid)["revision"], revision_before)

    def test_write_failure_resumes_from_checkpoint(self):
        calls = {"n": 0}

        def hook(batch_id, op):
            if op["op_type"] != "reassign":
                return
            calls["n"] += 1
            if calls["n"] == 1:
                raise sqlite3.OperationalError("disk I/O error")

        self.svc._write_fail_hook = hook
        batch = self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "upsert_aircraft", "payload": {"id": "AC7", "model": "A319", "maintenance_due": iso(self.base + timedelta(days=8))}},
            {"seq": 2, "op_type": "upsert_crew", "payload": {"id": "CR7", "name": "庚组", "base": "AAA", "duty_start": iso(self.base - timedelta(hours=2)), "max_duty_minutes": 600}},
            {"seq": 3, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC7", "CR7", 4)}]})
        self.svc._write_fail_hook = None
        self.assertEqual(batch["status"], "failed")
        self.assertEqual(batch["error_code"], "write_failed")
        self.assertEqual(batch["checkpoint_seq"], 2)  # 前两步已落盘
        self.assertEqual({op["seq"]: op["status"] for op in batch["operations"]},
                         {1: "applied", 2: "applied", 3: "pending"})
        self.assertIsNotNone(self.svc.repo.conn.execute("SELECT 1 FROM aircraft WHERE id='AC7'").fetchone())
        retried = self.svc.retry_batch(batch["id"], "alice", "scheduler", {})
        self.assertEqual(retried["status"], "applied")
        self.assertEqual({op["seq"]: op["status"] for op in retried["operations"]},
                         {1: "applied", 2: "applied", 3: "applied"})
        # 失败注入只触发一次（首次改派）；重试时改派正常落盘，修订号只加一次
        self.assertEqual(calls["n"], 1)
        self.assertEqual(self.svc.get_plan(self.pid)["revision"], 2)

    def test_crash_restart_recovers_queue(self):
        # 直接构造一个处理到一半崩溃（processing）的批次：第一步已 applied，批次未终结
        batch = self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "upsert_aircraft", "payload": {"id": "AC8", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=8))}},
            {"seq": 2, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC8", "CR2", 2)}]})
        self.assertEqual(batch["status"], "applied")
        conn = self.svc.repo.conn
        conn.execute("UPDATE offline_batches SET status='processing' WHERE id=?", (batch["id"],))
        # 模拟第 2 步事务提交前崩溃：op2、方案修订号都未落盘，只有 op1 和断点落盘
        conn.execute("UPDATE batch_operations SET status='pending', applied_at=NULL WHERE batch_id=? AND seq=2", (batch["id"],))
        conn.execute("UPDATE offline_batches SET checkpoint_seq=1 WHERE id=?", (batch["id"],))
        conn.execute("UPDATE recovery_plans SET revision=1 WHERE id=?", (self.pid,))
        conn.execute("UPDATE assignments SET changed_revision=1 WHERE plan_id=? AND flight_id=?", (self.pid, self.f1["id"]))
        # 进程崩溃后重启：队列复位并自动续跑
        svc2 = AirlineRecoveryService(self.db)
        self.assertEqual(svc2.recover_queue()["recovered_batches"], [])  # 构造函数已自动恢复
        view = svc2.get_batch(batch["id"])
        self.assertEqual(view["status"], "applied")
        self.assertEqual({op["seq"]: op["status"] for op in view["operations"]}, {1: "applied", 2: "applied"})
        self.assertEqual(svc2.get_plan(self.pid)["revision"], 2)
        state = svc2.state()
        self.assertEqual(state["queue_summary"]["processing"], 0)

    def test_pending_conflict_survives_restart(self):
        self.svc.submit_batch("alice", "scheduler", {"plan_id": self.pid, "device_id": "dev-A", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC2", "CR2", 4)}]})
        late = self.svc.submit_batch("bob", "scheduler", {"plan_id": self.pid, "device_id": "dev-B", "base_revision": 1, "operations": [
            {"seq": 1, "op_type": "reassign", "payload": self.reassign(self.f1["id"], "AC1", "CR1", 5)}]})
        self.assertEqual(late["status"], "conflict")
        svc2 = AirlineRecoveryService(self.db)
        view = svc2.get_batch(late["id"])
        self.assertEqual(view["status"], "conflict")
        self.assertTrue(any(c["status"] == "open" and c["conflict_type"] == "concurrent_reassign" for c in view["pending_conflicts"]))
        self.assertEqual(svc2.state()["queue_summary"]["open_conflicts"], 1)


    def test_batch_permissions(self):
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_batch("view", "viewer", {"plan_id": self.pid, "base_revision": 1, "operations": []})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_batch("a", "scheduler", {"plan_id": self.pid, "base_revision": 1, "operations": []})
        self.assertEqual(ctx.exception.code, "invalid_batch")
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_batch("a", "scheduler", {"plan_id": self.pid, "base_revision": 1,
                                                      "operations": [{"seq": 1, "op_type": "reassign", "payload": {}}]})
        self.assertEqual(ctx.exception.code, "invalid_operation")
class LegacyUpgradeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "legacy.db"
        self.base = utcnow() + timedelta(days=1)

    def tearDown(self):
        self.tmp.cleanup()

    def test_legacy_database_upgrade_keeps_data(self):
        # 用旧版结构（无批次表、assignments 无 changed_revision）和数据建库
        legacy = sqlite3.connect(self.db)
        legacy.executescript("""
            CREATE TABLE airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
            CREATE TABLE flights(id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL, std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL, crew_id TEXT NOT NULL, passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL);
            CREATE TABLE disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
            CREATE TABLE recovery_plans(id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT);
            CREATE TABLE assignments(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL, flight_id INTEGER NOT NULL, aircraft_id TEXT NOT NULL, crew_id TEXT NOT NULL, new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0, missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id));
            CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """)
        legacy.execute("INSERT INTO airports VALUES('AAA','CN','00:00','00:00')")
        legacy.execute("INSERT INTO airports VALUES('BBB','CN','00:00','00:00')")
        legacy.execute("INSERT INTO aircraft VALUES('AC1','A320',?, 'active')", (iso(self.base + timedelta(days=5)),))
        legacy.execute("INSERT INTO crew VALUES('CR1','旧机组','AAA',?,720,'active')", (iso(self.base - timedelta(hours=2)),))
        legacy.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES('AAA','BBB',?,?,0)",
                       (iso(self.base - timedelta(days=1)), iso(self.base + timedelta(days=2))))
        legacy.execute("INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,updated_at) VALUES('AB9','AAA','BBB',?,?, 'AC1','CR1',?)",
                       (iso(self.base), iso(self.base + timedelta(hours=2)), iso()))
        legacy.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES('aircraft_fault','AC1',?,?,?)",
                       (iso(self.base - timedelta(hours=1)), iso(self.base + timedelta(hours=3)), iso()))
        legacy.execute("INSERT INTO recovery_plans(disruption_id,name,status,revision,created_by,created_at) VALUES(1,'旧方案','draft',7,'oldhand',?)", (iso(),))
        legacy.execute("INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta) VALUES(1,1,'AC1','CR1',?,?)",
                       (iso(self.base + timedelta(hours=1)), iso(self.base + timedelta(hours=3))))
        legacy.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(1,'oldhand','scheduler','plan_created','{}',?)", (iso(),))
        legacy.commit()
        legacy.close()

        svc = AirlineRecoveryService(self.db)  # 触发升级
        plan = svc.get_plan(1)
        self.assertEqual(plan["revision"], 7)              # 修订号保留
        self.assertEqual(plan["name"], "旧方案")            # 原方案保留
        self.assertEqual(plan["assignments"][0]["changed_revision"], 1)
        audit = svc.repo.conn.execute("SELECT action FROM audit_log WHERE actor='oldhand'").fetchall()
        self.assertEqual([r["action"] for r in audit], ["plan_created"])  # 审计保留
        # 升级后离线批次功能可用，且旧改派按基线前数据处理
        batch = svc.submit_batch("newhand", "scheduler", {"plan_id": 1, "device_id": "dev", "base_revision": 7, "operations": [
            {"seq": 1, "op_type": "reassign",
             "payload": {"flight_id": 1, "aircraft_id": "AC1", "crew_id": "CR1",
                         "new_std": iso(self.base + timedelta(hours=2)), "new_sta": iso(self.base + timedelta(hours=4))}}]})
        self.assertEqual(batch["status"], "applied")


if __name__ == "__main__":
    unittest.main()

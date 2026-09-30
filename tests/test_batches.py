import json, sqlite3, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class OfflineBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.svc = AirlineRecoveryService(self.db)
        base = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self):
        self.tmp.cleanup()

    def make_flight(self, number, aircraft="AC1", crew="CR1"):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB",
                                                               "std": iso(self.base), "sta": iso(self.base + timedelta(hours=2)),
                                                               "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def make_plan(self, name="方案一", flight=None):
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1",
                                                                        "starts_at": iso(self.base - timedelta(hours=1)),
                                                                        "ends_at": iso(self.base + timedelta(hours=3))})
        assignments = [] if flight is None else [{"flight_id": flight["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                                                   "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2))}]
        return self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": name, "assignments": assignments})

    def reassign(self, seq, flight, aircraft="AC2", crew="CR2", start=None, end=None):
        start = start or self.base + timedelta(hours=3)
        end = end or self.base + timedelta(hours=5)
        return {"seq": seq, "op": "reassign", "flight_id": flight["id"], "aircraft_id": aircraft, "crew_id": crew,
                "new_std": iso(start), "new_sta": iso(end), "missed_connections": 0}

    def submit(self, plan, key, ops, baseline=1):
        return self.svc.submit_batch(plan["id"], "sched", "scheduler", {"batch_key": key, "baseline_revision": baseline, "operations": ops})

    def test_batch_merge_last_write_wins_and_superseded(self):
        flight = self.make_flight("AB100")
        plan = self.make_plan(flight=flight)
        batch = self.submit(plan, "k1", [
            self.reassign(1, flight, start=self.base + timedelta(hours=3), end=self.base + timedelta(hours=5)),
            self.reassign(2, flight, start=self.base + timedelta(hours=4), end=self.base + timedelta(hours=6)),
        ])
        self.assertEqual(batch["status"], "merged")
        self.assertEqual(batch["result"]["applied_operations"], 1)
        self.assertEqual(batch["result"]["superseded"], 1)
        ops = {o["seq"]: o for o in batch["operations"]}
        self.assertEqual(ops[1]["status"], "superseded")
        self.assertEqual(ops[2]["status"], "applied")
        assignment = self.svc.get_plan(plan["id"])["assignments"][0]
        self.assertEqual(assignment["new_std"], iso(self.base + timedelta(hours=4)))
        self.assertEqual(assignment["aircraft_id"], "AC2")

    def test_two_dispatchers_late_batch_wins(self):
        flight = self.make_flight("AB101")
        plan = self.make_plan(flight=flight)
        self.submit(plan, "dispA", [self.reassign(1, flight, start=self.base + timedelta(hours=3), end=self.base + timedelta(hours=5))])
        late = self.submit(plan, "dispB", [self.reassign(1, flight, start=self.base + timedelta(hours=6), end=self.base + timedelta(hours=8))])
        self.assertEqual(late["status"], "merged")
        assignment = self.svc.get_plan(plan["id"])["assignments"][0]
        self.assertEqual(assignment["new_std"], iso(self.base + timedelta(hours=6)))

    def test_locked_resource_conflict_late_cannot_release(self):
        flight1 = self.make_flight("AB102")
        plan1 = self.make_plan("锁定方案", flight=flight1)
        self.submit(plan1, "lock-batch", [self.reassign(1, flight1, start=self.base + timedelta(hours=2), end=self.base + timedelta(hours=4))])
        self.svc.lock_plan(plan1["id"], "ops", "ops_manager", {"expected_revision": self.svc.get_plan(plan1["id"])["revision"]})
        flight2 = self.make_flight("AB103")
        plan2 = self.make_plan("晚到方案", flight=flight2)
        batch = self.submit(plan2, "late-batch", [self.reassign(1, flight2, start=self.base + timedelta(hours=2, minutes=30), end=self.base + timedelta(hours=4, minutes=30))])
        self.assertEqual(batch["status"], "conflict")
        codes = [c["code"] for c in batch["conflicts"]]
        self.assertIn("locked_resource_conflict", codes)
        state = self.svc.state()
        self.assertTrue(any(c["code"] == "locked_resource_conflict" for c in state["pending_conflicts"]))

    def test_retry_from_checkpoint_after_failure(self):
        flight1 = self.make_flight("AB104")
        flight2 = self.make_flight("AB105")
        plan = self.make_plan(flight=flight1)
        canceled = self.svc.cancel_flight(flight2["id"], "sched", "scheduler", {"reason": "临时停场"})["flight"]
        batch = self.submit(plan, "retry-batch", [
            self.reassign(1, flight1, aircraft="AC2", crew="CR2", start=self.base + timedelta(hours=2), end=self.base + timedelta(hours=4)),
            self.reassign(2, flight2, aircraft="AC2", crew="CR2", start=self.base + timedelta(hours=6), end=self.base + timedelta(hours=8))])
        self.assertEqual(batch["status"], "failed")
        self.assertEqual(batch["error"]["code"], "canceled_flight")
        self.assertEqual(batch["error"]["operation_seq"], 2)
        self.assertEqual(batch["checkpoint"], 1)
        ops = {o["seq"]: o for o in batch["operations"]}
        self.assertEqual(ops[1]["status"], "applied")
        self.assertEqual(ops[2]["status"], "failed")
        self.svc.recover_flight(flight2["id"], "sched", "scheduler", {"expected_revision": canceled["revision"],
                                                                       "new_std": iso(self.base + timedelta(hours=6)),
                                                                       "new_sta": iso(self.base + timedelta(hours=8))})
        retried = self.svc.retry_batch(batch["id"], "sched", "scheduler")
        self.assertEqual(retried["status"], "merged")
        self.assertEqual(retried["checkpoint"], 2)
        ops = {o["seq"]: o for o in retried["operations"]}
        self.assertEqual(ops[1]["status"], "applied")
        self.assertEqual(ops[2]["status"], "applied")

    def test_crash_recovery_resumes_queued_batch(self):
        flight = self.make_flight("AB106")
        plan = self.make_plan(flight=flight)
        payload = json.dumps({"aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)),
                              "new_sta": iso(self.base + timedelta(hours=5)), "missed_connections": 0, "status": "planned"}, sort_keys=True)
        conn = self.svc.repo.conn
        cur = conn.execute("""INSERT INTO offline_batches(batch_key,plan_id,baseline_revision,status,submitted_by,submitted_at,updated_at)
                              VALUES(?,?,?,?,?,?,?)""", ("crash-batch", plan["id"], 1, "queued", "sched", iso(), iso()))
        batch_id = cur.lastrowid
        conn.execute("""INSERT INTO batch_operations(batch_id,seq,op,flight_id,payload_json,status,created_at)
                        VALUES(?,?,?,?,?,?,?)""", (batch_id, 1, "reassign", flight["id"], payload, "pending", iso()))
        self.svc.recover_pending_batches()
        recovered = self.svc.get_batch(batch_id)
        self.assertEqual(recovered["status"], "merged")
        self.assertEqual(self.svc.get_plan(plan["id"])["assignments"][0]["aircraft_id"], "AC2")

    def test_idempotent_batch_key_no_double_apply(self):
        flight = self.make_flight("AB107")
        plan = self.make_plan(flight=flight)
        body = {"batch_key": "idem", "baseline_revision": 1, "operations": [self.reassign(1, flight)]}
        first = self.svc.submit_batch(plan["id"], "sched", "scheduler", body)
        second = self.svc.submit_batch(plan["id"], "sched", "scheduler", body)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["status"], "merged")
        conn = self.svc.repo.conn
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM offline_batches WHERE batch_key=?", ("idem",)).fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM assignments WHERE plan_id=? AND flight_id=?", (plan["id"], flight["id"])).fetchone()[0], 1)

    def test_batch_rejected_on_locked_plan(self):
        flight = self.make_flight("AB108")
        plan = self.make_plan(flight=flight)
        self.submit(plan, "pre-lock", [self.reassign(1, flight)])
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": self.svc.get_plan(plan["id"])["revision"]})
        with self.assertRaises(ApiError) as ctx:
            self.submit(plan, "post-lock", [self.reassign(1, flight)])
        self.assertEqual(ctx.exception.code, "plan_locked")

    def test_schema_upgrade_preserves_revision_audit_and_plan(self):
        v1 = """
        CREATE TABLE airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
        CREATE TABLE aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
        CREATE TABLE flights(id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL, std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id), passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL);
        CREATE TABLE disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
        CREATE TABLE recovery_plans(id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT);
        CREATE TABLE assignments(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE, flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id), new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0, missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id));
        CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        """
        db_path = Path(self.tmp.name) / "legacy.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript(v1)
        base = iso(self.base)
        conn.execute("INSERT INTO airports VALUES('AAA','CN','23:00','05:00')")
        conn.execute("INSERT INTO aircraft VALUES('AC1','A320',?,'active')", (iso(self.base + timedelta(days=5)),))
        conn.execute("INSERT INTO crew VALUES('CR1','甲组','AAA',?,720,'active')", (iso(self.base - timedelta(hours=2)),))
        conn.execute("INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,updated_at) VALUES('AB900','AAA','BBB',?,?, 'AC1','CR1',?)", (base, iso(self.base + timedelta(hours=2)), iso()))
        conn.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES('aircraft_fault','AC1',?,?,?)", (iso(self.base - timedelta(hours=1)), iso(self.base + timedelta(hours=3)), iso()))
        conn.execute("INSERT INTO recovery_plans(disruption_id,name,created_by,created_at) VALUES(1,'旧方案','sched',?)", (iso(),))
        conn.execute("INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta) VALUES(1,1,'AC1','CR1',?,?)", (base, iso(self.base + timedelta(hours=2))))
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(1,'sched','scheduler','plan_created','{}',?)", (iso(),))
        conn.commit(); conn.close()

        upgraded = AirlineRecoveryService(db_path)
        plan = upgraded.get_plan(1)
        self.assertEqual(plan["revision"], 1)
        self.assertEqual(plan["name"], "旧方案")
        self.assertEqual(len(plan["assignments"]), 1)
        logs = [dict(r) for r in upgraded.repo.conn.execute("SELECT action FROM audit_log ORDER BY id")]
        self.assertEqual(logs[0]["action"], "plan_created")
        self.assertEqual(logs[-1]["action"], "schema_upgraded")
        version = upgraded.repo.conn.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()["value"]
        self.assertEqual(version, "2")
        for table in ("offline_batches", "batch_operations", "batch_conflicts"):
            self.assertIsNotNone(upgraded.repo.conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


if __name__ == "__main__":
    unittest.main()

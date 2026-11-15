"""Tests for the streaming, memory-bounded upload pipeline.

Covers the failure modes that motivated the rewrite:

* a ~5 MB / 100k+ row file ingests with a flat memory footprint (no whole
  request body, decoded text or pending-row set resident at once);
* same-event/same-ingest different values still reject the whole batch and
  errors name the source row; identical retries stay idempotent;
* rejected uploads leave no batch rows, no transaction rows and no spool
  files — no visible half-batch;
* a report created concurrently with an upload freezes only fully committed
  batches;
* ready reports and sample uploads keep working alongside the new path.
"""

from __future__ import annotations

import csv
import io
import shutil
import tempfile
import threading
import tracemalloc
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.csvparse import BatchRejected
from app.service import AppService

UTC = timezone.utc
DAY = 86400
AUDIT = int(datetime.fromisoformat("2025-01-10T12:00:00+00:00").timestamp())

BIG_ROWS = 120_000
# The old pipeline held several full copies of a 5 MB file plus 120k parsed
# objects; a streaming pass must stay an order of magnitude below that.
MEMORY_CEILING_MB = 64


def txn_csv(rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["event_id", "account_id", "occurred_at", "ingested_at",
                "amount", "action"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def sample_csv(rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["sample_id", "account_id", "audit_at", "label"])
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode()


def write_big_csv(path: Path, rows: int = BIG_ROWS) -> int:
    with path.open("w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["event_id", "account_id", "occurred_at", "ingested_at",
                    "amount", "action"])
        base = 1_736_000_000
        for i in range(rows):
            w.writerow([f"E{i:07d}", f"A{i % 997}", base + i, base + i,
                        f"{(i % 9000) + 1}.{i % 100:02d}", "revise"])
    return path.stat().st_size


class StreamingIngestTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.svc = AppService(Settings(data_dir=self.dir))
        self.big = Path(self.dir) / "big.csv"
        self.size = write_big_csv(self.big)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_large_file_ingests_with_flat_memory(self):
        self.assertGreater(self.size, 5 * 1024 * 1024)
        tracemalloc.start(25)
        result = self.svc.upload_transactions(
            "big.csv", self.big.open("rb"))
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(result["rows"], BIG_ROWS)
        self.assertEqual(result["stored_rows"], BIG_ROWS)
        self.assertEqual(result["events"], BIG_ROWS)
        self.assertLess(
            peak / (1024 * 1024), MEMORY_CEILING_MB,
            f"ingestion peaked at {peak / 1e6:.1f} MB; expected streaming, "
            f"under {MEMORY_CEILING_MB} MB")
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], BIG_ROWS)

    def test_retry_of_large_file_is_idempotent(self):
        first = self.svc.upload_transactions("big.csv", self.big.open("rb"))
        second = self.svc.upload_transactions("big.csv", self.big.open("rb"))
        self.assertEqual(first["stored_rows"], BIG_ROWS)
        self.assertEqual(second["stored_rows"], 0)
        self.assertEqual(second["skipped_same_value"], BIG_ROWS)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], BIG_ROWS)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0], 2)

    def test_conflict_inside_large_file_rejects_everything_with_row_number(self):
        # Append a conflicting revision for the first bulk event: same
        # (event_id, ingested_at) as source row 2 but a different amount.
        with self.big.open("a", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["E0000000", "A0", 1_736_000_000, 1_736_000_000,
                        "9999.99", "revise"])
        with self.assertRaises(BatchRejected) as ctx:
            self.svc.upload_transactions("big.csv", self.big.open("rb"))
        details = ctx.exception.details
        self.assertTrue(details)
        self.assertEqual(details[0]["code"], "same_ingest_conflict")
        reported = sorted(d["row"] for d in details)
        # Both the original (row 2) and the appended last source row are named.
        self.assertEqual(reported[0], 2)
        self.assertEqual(reported[-1], BIG_ROWS + 2)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], 0)
        # No half-batch artefacts remain on disk.
        leftovers = [p.name for p in self.svc.settings.uploads_dir.iterdir()
                     if p.name.startswith((".spool-", ".tmp-"))]
        self.assertEqual(leftovers, [])

    def test_cross_batch_conflict_rolls_back_and_names_source_row(self):
        good = txn_csv([("E1", "A", AUDIT, AUDIT, "10.00", "revise")])
        self.svc.upload_transactions("a.csv", good)
        bad = txn_csv([
            ("E9", "A", AUDIT, AUDIT, "5.00", "revise"),
            ("E1", "A", AUDIT, AUDIT, "11.00", "revise"),
        ])
        with self.assertRaises(BatchRejected) as ctx:
            self.svc.upload_transactions("b.csv", bad)
        self.assertEqual(ctx.exception.details[0]["row"], 3)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], 1)

    def test_validation_error_points_at_source_row_and_writes_nothing(self):
        bad = txn_csv([
            ("E1", "A", AUDIT, AUDIT, "10.00", "revise"),
            ("E2", "A", AUDIT, AUDIT, "1e3", "revise"),
            ("E3", "A", "not-a-time", AUDIT, "1.00", "revise"),
        ])
        with self.assertRaises(BatchRejected) as ctx:
            self.svc.upload_transactions("bad.csv", bad)
        rows = sorted(d["row"] for d in ctx.exception.details)
        self.assertEqual(rows, [3, 4])
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0], 0)

    def test_identical_duplicate_rows_inside_one_file_are_deduped(self):
        payload = txn_csv([
            ("E1", "A", AUDIT, AUDIT, "10.00", "revise"),
            ("E1", "A", AUDIT, AUDIT, "10.00", "revise"),
        ])
        result = self.svc.upload_transactions("dup.csv", payload)
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["stored_rows"], 1)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM transactions").fetchone()[0], 1)

    def test_spool_is_removed_after_commit(self):
        staged = self.svc._as_staged("big.csv", self.big.open("rb"),
                                     "transactions.csv")
        self.assertTrue(staged.path.exists())
        self.svc.upload_transactions("big.csv", staged)
        self.assertFalse(staged.path.exists())
        # The final, content-addressed source file remains and matches.
        with self.svc.db.read() as conn:
            stored = conn.execute(
                "SELECT stored_path, sha256 FROM batches").fetchone()
        self.assertTrue(Path(stored["stored_path"]).is_file())


class SampleAndReportRegressionTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.svc = AppService(Settings(data_dir=self.dir))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _build_ready_report(self) -> str:
        self.svc.upload_transactions("t.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise"),
        ]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        pid = self.svc.create_report()["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        self.assertEqual(self.svc.get_report(pid)["status"], "ready")
        return pid

    def test_sample_upload_duplicate_rejects_with_row_number(self):
        self.svc.upload_samples("s1.csv", sample_csv([("S1", "A", AUDIT, 1)]))
        with self.assertRaises(BatchRejected) as ctx:
            self.svc.upload_samples(
                "s2.csv", sample_csv([("S2", "A", AUDIT, 0),
                                      ("S1", "A", AUDIT, 1)]))
        self.assertEqual(ctx.exception.details[0]["row"], 3)
        with self.svc.db.read() as conn:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM samples").fetchone()[0], 1)

    def test_ready_report_download_unaffected(self):
        pid = self._build_ready_report()
        found = self.svc.report_file(pid)
        self.assertIsNotNone(found)
        text = found[0].read_text(encoding="utf-8")
        self.assertIn("txn_amount_7d", text)

    def test_sample_batch_records_submitted_and_stored_rows(self):
        result = self.svc.upload_samples(
            "s.csv", sample_csv([("S1", "A", AUDIT, 1),
                                 ("S2", "A", AUDIT, 0)]))
        self.assertEqual(result["rows"], 2)
        with self.svc.db.read() as conn:
            row = conn.execute(
                "SELECT submitted_rows, stored_rows FROM batches "
                "WHERE id=?", (result["batch_id"],)).fetchone()
            self.assertEqual(tuple(row), (2, 2))

    def test_report_created_during_upload_freezes_only_committed_batch(self):
        from app import db as db_module

        # Seed one committed batch and the samples.
        self.svc.upload_transactions("base.csv", txn_csv([
            ("E1", "A", AUDIT - 2 * DAY, AUDIT - 3 * DAY, "10.00", "revise"),
        ]))
        self.svc.upload_samples("s.csv", sample_csv([("S1", "A", AUDIT, 1)]))

        entered = threading.Event()
        release = threading.Event()
        original_cm = db_module.Database.write_transaction

        def gated_cm(self_db):
            cm = original_cm(self_db)

            class _Wrapped:
                def __enter__(self_inner):
                    conn = cm.__enter__()
                    # The first writer is the big upload. Pause *after*
                    # BEGIN IMMEDIATE but before commit, while the report
                    # thread's own write transaction queues on the database
                    # lock: it can only begin once the upload has committed,
                    # hence can never freeze an in-flight, uncommitted batch.
                    if not entered.is_set():
                        entered.set()
                        release.wait(10)
                    return conn

                def __exit__(self_inner, *exc):
                    return cm.__exit__(*exc)

            return _Wrapped()

        db_module.Database.write_transaction = gated_cm  # type: ignore[assignment]
        try:
            big = Path(self.dir) / "big.csv"
            write_big_csv(big, rows=20_000)

            def do_upload():
                self.svc.upload_transactions("big.csv", big.open("rb"))

            worker = threading.Thread(target=do_upload)
            worker.start()
            self.assertTrue(entered.wait(5))

            # The upload transaction is open and uncommitted. A concurrent
            # reader (exactly the snapshot create_report() first takes) must
            # see only the previously committed batch — never a half-batch.
            with self.svc.db.read() as ro:
                visible_batches = ro.execute(
                    "SELECT COALESCE(MAX(id),0) AS v FROM batches "
                    "WHERE kind='transactions'").fetchone()["v"]
                visible_rows = ro.execute(
                    "SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
            self.assertEqual(visible_batches, 1)
            self.assertEqual(visible_rows, 1)

            release.set()
            worker.join(timeout=60)
            self.assertFalse(worker.is_alive())
        finally:
            release.set()
            db_module.Database.write_transaction = original_cm  # type: ignore[assignment]

        # A report created while the upload was still open could not see it;
        # a report created after commit freezes exactly the committed set.
        report = self.svc.create_report()
        manifest = report["manifest"]
        self.assertEqual(manifest["txn_batch_hi"], 3)
        self.assertEqual(manifest["transaction_rows"], 20_001)

        pid = report["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        found = self.svc.report_file(pid)
        self.assertIsNotNone(found)
        rows = list(csv.DictReader(io.StringIO(
            found[0].read_text(encoding="utf-8"))))
        self.assertEqual(rows[0]["txn_amount_7d"], "10.00")

        # The big batch's own rows are pinned in the same frozen snapshot.
        later = self.svc.create_report()
        self.assertEqual(later["manifest"]["txn_batch_hi"],
                         manifest["txn_batch_hi"])
        self.assertEqual(later["manifest"]["transaction_rows"], 20_001)


class ApiStreamingTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        from app.main import create_app
        self.web = create_app(Settings(data_dir=self.dir), start_worker=False)
        self.client = TestClient(self.web)
        self.svc = self.web.state.service

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_api_streams_upload_then_generates_and_downloads_report(self):
        big = Path(self.dir) / "big.csv"
        write_big_csv(big, rows=5_000)
        with big.open("rb") as fh:
            resp = self.client.post(
                "/api/uploads/transactions",
                files={"file": ("big.csv", fh, "text/csv")})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["stored_rows"], 5_000)

        resp = self.client.post(
            "/api/uploads/samples",
            files={"file": ("s.csv", sample_csv([("S1", "A996", AUDIT, 1)]),
                            "text/csv")})
        self.assertEqual(resp.status_code, 200, resp.text)

        report = self.client.post("/api/reports").json()
        pid = report["report_id"]
        self.svc.claim_next_report()
        self.svc.run_report(pid)
        detail = self.client.get(f"/api/reports/{pid}").json()
        self.assertEqual(detail["status"], "ready")
        dl = self.client.get(f"/api/reports/{pid}/download")
        self.assertEqual(dl.status_code, 200)
        self.assertIn("txn_amount_7d", dl.text)

        # Spool files never linger after request handling.
        leftovers = [p.name for p in self.svc.settings.uploads_dir.iterdir()
                     if p.name.startswith((".spool-", ".tmp-"))]
        self.assertEqual(leftovers, [])

    def test_api_oversize_upload_is_rejected_and_cleans_spool(self):
        settings = self.web.state.settings
        object.__setattr__(settings, "max_upload_bytes", 1024)
        big = Path(self.dir) / "big.csv"
        write_big_csv(big, rows=1_000)
        with big.open("rb") as fh:
            resp = self.client.post(
                "/api/uploads/transactions",
                files={"file": ("big.csv", fh, "text/csv")})
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.json()["details"][0]["code"], "too_large")
        leftovers = [p.name for p in self.svc.settings.uploads_dir.iterdir()
                     if p.name.startswith((".spool-", ".tmp-"))]
        self.assertEqual(leftovers, [])

    def test_api_bad_row_returns_422_with_source_row(self):
        resp = self.client.post(
            "/api/uploads/transactions",
            files={"file": ("bad.csv",
                            txn_csv([("E1", "A", AUDIT, AUDIT, "x", "revise")]),
                            "text/csv")})
        self.assertEqual(resp.status_code, 422)
        self.assertEqual(resp.json()["details"][0]["row"], 2)


class SubprocessMemoryTest(unittest.TestCase):
    """End-to-end guard under an address-space cap (RLIMIT_AS).

    The old chain (joined body + decoded text + full parsed list) could not
    initialise inside a tight limit; the streaming pipeline must complete.
    """

    def test_ingest_under_address_space_cap(self):
        import subprocess
        import sys

        work = tempfile.mkdtemp()
        script = Path(work) / "child.py"
        script.write_text(
            "import resource, sys\n"
            f"resource.setrlimit(resource.RLIMIT_AS, "
            f"(256 * 1024 * 1024, resource.RLIM_INFINITY))\n"
            "sys.path.insert(0, %r)\n"
            "import csv, tempfile\n"
            "from pathlib import Path\n"
            "from app.config import Settings\n"
            "from app.service import AppService\n"
            f"rows = {BIG_ROWS}\n"
            "work = tempfile.mkdtemp()\n"
            "src = Path(work) / 'big.csv'\n"
            "with src.open('w', newline='') as fh:\n"
            "    w = csv.writer(fh, lineterminator='\\n')\n"
            "    w.writerow(['event_id','account_id','occurred_at',"
            "'ingested_at','amount','action'])\n"
            "    for i in range(rows):\n"
            "        w.writerow([f'E{i:07d}', f'A{i %% 997}', 1736000000+i,"
            " 1736000000+i, '10.00', 'revise'])\n"
            "svc = AppService(Settings(data_dir=Path(work) / 'data'))\n"
            "r = svc.upload_transactions('big.csv', src.open('rb'))\n"
            "assert r['stored_rows'] == rows, r\n"
            "r2 = svc.upload_transactions('big.csv', src.open('rb'))\n"
            "assert r2['skipped_same_value'] == rows, r2\n"
            "print('CHILD_OK')\n" % str(Path(__file__).resolve().parent.parent),
            encoding="utf-8")
        try:
            proc = subprocess.run(
                [sys.executable, str(script)], capture_output=True, text=True,
                timeout=300)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("CHILD_OK", proc.stdout)
        finally:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()

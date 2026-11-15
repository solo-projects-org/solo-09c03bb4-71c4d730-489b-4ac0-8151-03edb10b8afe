"""Domain service: streaming batch ingestion, report pinning, atomic output."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from . import features
from .config import Settings
from .csvparse import (
    BatchRejected, stream_samples, stream_transactions,
)
from .db import Database

UTC = timezone.utc

# Prefixes that always denote transient, uncommitted upload/working files.
SPOOL_PREFIXES = (".spool-", ".tmp-")
CHUNK = 1 << 20


def now_epoch() -> int:
    return int(time.time())


class ConflictError(Exception):
    def __init__(self, message: str, existing_batch_id: int | None = None):
        super().__init__(message)
        self.message = message
        self.existing_batch_id = existing_batch_id


@dataclass(frozen=True)
class StagedUpload:
    """An upload already streamed to disk by the web layer.

    The service takes ownership of ``path`` and removes it as soon as the
    batch is either committed or rejected, so request handling never needs the
    whole request body in memory.
    """

    path: Path
    byte_size: int
    sha256: str
    filename: str


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


class AppService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.ensure_dirs()
        self._cleanup_stale_spools()
        self.db = Database(settings.db_path)
        self.db.init()

    def _cleanup_stale_spools(self) -> None:
        """Remove spool/temp files left by a process that died mid-upload."""
        for entry in self.settings.uploads_dir.iterdir():
            if entry.is_file() and entry.name.startswith(SPOOL_PREFIXES):
                try:
                    entry.unlink()
                except OSError:
                    pass

    # ------------------------------------------------------------------ #
    # Upload staging
    # ------------------------------------------------------------------ #
    def spool_bytes(self, filename: str, data: bytes) -> StagedUpload:
        """Stage an in-memory payload (e.g. direct API/test callers).

        HTTP uploads are streamed to disk by ``app.main._spool_upload``
        instead; this keeps the service callable with the original
        ``(filename, bytes)`` signature.
        """
        if len(data) > self.settings.max_upload_bytes:
            raise BatchRejected(
                "上传文件超过大小限制。",
                [{"row": 0, "field": "", "code": "too_large",
                  "message": f"上限 {self.settings.max_upload_bytes} 字节。",
                  "value": ""}])
        if not data:
            raise BatchRejected(
                "上传文件为空。",
                [{"row": 0, "field": "", "code": "empty",
                  "message": "0 字节。", "value": ""}])
        digest = hashlib.sha256(data).hexdigest()
        fd, name = tempfile.mkstemp(prefix=".spool-", suffix=".csv",
                                    dir=self.settings.uploads_dir)
        path = Path(name)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return StagedUpload(path=path, byte_size=len(data), sha256=digest,
                            filename=filename)

    # ------------------------------------------------------------------ #
    # Shared commit machinery
    # ------------------------------------------------------------------ #
    def _commit_batch(self, staged: StagedUpload, kind: str, submitted_rows: int,
                      insert_fn) -> int:
        """Insert the batch header and rows in one DB transaction, then publish.

        ``insert_fn`` streams rows from the spool while it still carries its
        hidden ``.spool-`` staging name; any validation/conflict error raises
        inside the transaction, which rolls back before the source file is
        ever renamed into place. Only after every row has settled does
        :func:`os.replace` publish the content-addressed final name, and if
        the commit itself still fails the published file is unlinked, so no
        half-batch is ever visible. The UUID suffix means a retry never
        overwrites a source pinned to an existing report.
        """
        safe_name = Path(staged.filename).name[:120] or f"{kind}.csv"
        target = (self.settings.uploads_dir
                  / f"{staged.sha256[:16]}-{uuid.uuid4().hex[:12]}-{safe_name}")
        try:
            with self.db.write_transaction() as conn:
                cur = conn.execute(
                    """INSERT INTO batches(kind, filename, byte_size, sha256,
                                           submitted_rows, stored_rows, stored_path,
                                           uploaded_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (kind, safe_name, staged.byte_size, staged.sha256,
                     submitted_rows, submitted_rows, str(target), now_epoch()),
                )
                batch_id = cur.lastrowid
                # Stream rows from the spool while it still exists under its
                # staging name; a conflict raises here and rolls everything
                # back before the file is ever published.
                inserted = insert_fn(conn, batch_id)
                if inserted is not None and inserted != submitted_rows:
                    conn.execute(
                        "UPDATE batches SET stored_rows=? WHERE id=?",
                        (inserted, batch_id))
                os.replace(staged.path, target)
            return batch_id
        except BaseException:
            try:
                target.unlink(missing_ok=True)
            finally:
                pass
            raise

    def _as_staged(self, filename: str,
                   source: StagedUpload | str | os.PathLike | bytes | BinaryIO,
                   default_name: str) -> StagedUpload:
        """Normalise the accepted input shapes to an owned StagedUpload."""
        if isinstance(source, StagedUpload):
            if source.byte_size > self.settings.max_upload_bytes:
                source.path.unlink(missing_ok=True)
                raise BatchRejected(
                    "上传文件超过大小限制。",
                    [{"row": 0, "field": "", "code": "too_large",
                      "message": f"上限 {self.settings.max_upload_bytes} 字节。",
                      "value": ""}])
            return source
        if isinstance(source, (str, os.PathLike)):
            # A caller-provided path is not ours to rename or delete: copy it
            # into a service-owned spool so commit/replace/cleanup semantics
            # stay uniform and the caller's file remains untouched.
            src = Path(source)
            size = src.stat().st_size
            if size > self.settings.max_upload_bytes:
                raise BatchRejected(
                    "上传文件超过大小限制。",
                    [{"row": 0, "field": "", "code": "too_large",
                      "message": f"上限 {self.settings.max_upload_bytes} 字节。",
                      "value": ""}])
            fd, name = tempfile.mkstemp(prefix=".spool-", suffix=".csv",
                                        dir=self.settings.uploads_dir)
            path = Path(name)
            digest = hashlib.sha256()
            try:
                with os.fdopen(fd, "wb") as out, src.open("rb") as inp:
                    while True:
                        chunk = inp.read(CHUNK)
                        if not chunk:
                            break
                        digest.update(chunk)
                        out.write(chunk)
                    out.flush()
                    os.fsync(out.fileno())
            except BaseException:
                path.unlink(missing_ok=True)
                raise
            return StagedUpload(path=path, byte_size=size,
                                sha256=digest.hexdigest(),
                                filename=filename or default_name)
        if isinstance(source, (bytes, bytearray, memoryview)):
            return self.spool_bytes(filename or default_name, bytes(source))
        if hasattr(source, "read"):
            return self._spool_stream(filename or default_name, source)
        raise TypeError(f"不支持的上传数据类型：{type(source)!r}")

    def _spool_stream(self, filename: str, stream: BinaryIO) -> StagedUpload:
        """Copy an open binary stream to an owned spool file in bounded chunks."""
        fd, name = tempfile.mkstemp(prefix=".spool-", suffix=".csv",
                                    dir=self.settings.uploads_dir)
        path = Path(name)
        digest = hashlib.sha256()
        total = 0
        try:
            with os.fdopen(fd, "wb") as fh:
                while True:
                    chunk = stream.read(CHUNK)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.settings.max_upload_bytes:
                        raise BatchRejected(
                            "上传文件超过大小限制。",
                            [{"row": 0, "field": "", "code": "too_large",
                              "message": f"上限 {self.settings.max_upload_bytes} 字节。",
                              "value": ""}])
                    digest.update(chunk)
                    fh.write(chunk)
                fh.flush()
                os.fsync(fh.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        if total == 0:
            path.unlink(missing_ok=True)
            raise BatchRejected(
                "上传文件为空。",
                [{"row": 0, "field": "", "code": "empty", "message": "0 字节。",
                  "value": ""}])
        return StagedUpload(path=path, byte_size=total,
                            sha256=digest.hexdigest(), filename=filename)

    # ------------------------------------------------------------------ #
    # Uploads
    # ------------------------------------------------------------------ #
    def upload_transactions(self, filename: str, source) -> dict:
        """Stream a transaction CSV from its spool into the database.

        The spool is parsed exactly once, row by row, inside the batch's
        IMMEDIATE transaction: validated rows land in a connection-local
        temporary staging table in bounded chunks, then set-based SQL
        arbitrates (event_id, ingested_at). Identical retries are skipped,
        differing values reject and roll the *whole* batch back — including
        the batch header row — so no half-batch is ever visible. Memory stays
        flat because neither parsed objects nor pending rows are retained.
        """
        staged = self._as_staged(filename, source, "transactions.csv")
        try:
            counters = {"stored": 0, "skipped": 0}
            conflicts: list[dict] = []
            chunk_size = 1000

            def insert(conn: sqlite3.Connection, batch_id: int) -> int:
                # Scratch table for this one connection/transaction.
                conn.execute(self.db.temp_staging_ddl())
                conn.execute(self.db.temp_staging_index())
                chunk: list[tuple] = []
                submitted_rows = 0

                def flush() -> None:
                    if not chunk:
                        return
                    conn.executemany(
                        "INSERT INTO temp.txn_staging(row_number, event_id, "
                        "account_id, occurred_at, ingested_at, amount_minor, "
                        "amount_text, action, raw_hash) "
                        "VALUES (?,?,?,?,?,?,?,?,?)", chunk)
                    chunk.clear()

                # Single streaming pass: validation errors raise here, before
                # any real table has received a committed row.
                for t in stream_transactions(staged.path):
                    submitted_rows += 1
                    chunk.append((t.row_number, t.event_id, t.account_id,
                                  t.occurred_at, t.ingested_at, t.amount_minor,
                                  t.amount_text, t.action, t.raw_hash))
                    if len(chunk) >= chunk_size:
                        flush()
                flush()
                if submitted_rows == 0:  # defensive; parser already guards
                    raise BatchRejected(
                        "交易 CSV 没有有效数据行。",
                        [{"row": 2, "field": "", "code": "no_rows",
                          "message": "至少需要一行交易。", "value": ""}])
                conn.execute(
                    "UPDATE batches SET submitted_rows=? WHERE id=?",
                    (submitted_rows, batch_id))

                # Rule 1: within this very upload, the same (event, ingest
                # instant) carrying different raw_hash rejects the whole
                # batch; identical duplicates are silently de-duplicated.
                intra = conn.execute(
                    "SELECT event_id, ingested_at, COUNT(DISTINCT raw_hash) AS v "
                    "FROM temp.txn_staging GROUP BY event_id, ingested_at "
                    "HAVING COUNT(DISTINCT raw_hash) > 1").fetchall()
                if intra:
                    marks = {(r["event_id"], r["ingested_at"]) for r in intra}
                    for r in conn.execute(
                            "SELECT row_number, event_id, ingested_at "
                            "FROM temp.txn_staging ORDER BY row_number"):
                        if (r["event_id"], r["ingested_at"]) in marks:
                            conflicts.append({
                                "row": r["row_number"], "field": "event_id",
                                "code": "same_ingest_conflict",
                                "message": f"事件 {r['event_id']} 在入库时刻 "
                                           f"{r['ingested_at']} 在本批次内出现不同取值，"
                                           "整批拒收。",
                                "value": r["event_id"]})
                            if len(conflicts) >= 100:
                                break
                    raise BatchRejected(
                        "交易批次整批拒收：同一事件同一入库时刻出现不同取值。",
                        conflicts)

                # Rule 2: against already-committed revisions, a differing
                # raw_hash rejects the whole batch; an identical one is an
                # idempotent retry and is skipped.
                cross = conn.execute(
                    "SELECT s.row_number, s.event_id, s.ingested_at, s.raw_hash "
                    "FROM temp.txn_staging s "
                    "JOIN transactions x ON x.event_id = s.event_id "
                    "AND x.ingested_at = s.ingested_at "
                    "WHERE x.raw_hash <> s.raw_hash "
                    "ORDER BY s.row_number LIMIT 100").fetchall()
                if cross:
                    for r in cross:
                        conflicts.append({
                            "row": r["row_number"], "field": "event_id",
                            "code": "same_ingest_conflict",
                            "message": f"事件 {r['event_id']} 在入库时刻 "
                                       f"{r['ingested_at']} 已存在不同取值的修订，"
                                       "整批拒收。",
                            "value": r["event_id"]})
                    raise BatchRejected(
                        "交易批次整批拒收：同一事件同一入库时刻出现不同取值。",
                        conflicts)

                # One row per (event, ingest instant, content); intra-batch
                # identical duplicates collapse via GROUP BY, and keys that
                # already exist with the same raw_hash are not re-inserted.
                inserted = conn.execute(
                    """INSERT INTO transactions(batch_id, event_id, account_id,
                           occurred_at, ingested_at, amount_minor, amount_text,
                           action, raw_hash, row_number)
                       SELECT ?, event_id, MIN(account_id), MIN(occurred_at),
                              ingested_at, MIN(amount_minor), MIN(amount_text),
                              MIN(action), raw_hash, MIN(row_number)
                       FROM temp.txn_staging
                       GROUP BY event_id, ingested_at, raw_hash
                       HAVING NOT EXISTS (
                           SELECT 1 FROM transactions x
                           WHERE x.event_id = temp.txn_staging.event_id
                             AND x.ingested_at = temp.txn_staging.ingested_at)""",
                    (batch_id,)).rowcount
                # Every staging row that did not become a new revision is an
                # identical-value duplicate: either collapsed by GROUP BY
                # (repeated inside this upload) or excluded by NOT EXISTS
                # (idempotent retry of an already committed revision).
                skipped = submitted_rows - inserted
                counters["stored"] = inserted
                counters["skipped"] = skipped
                counters["submitted"] = submitted_rows
                return inserted

            batch_id = self._commit_batch(staged, "transactions", 0, insert)
            submitted_rows = counters["submitted"]
        except BaseException:
            staged.path.unlink(missing_ok=True)
            raise
        with self.db.read() as conn:
            event_groups = conn.execute(
                "SELECT DISTINCT event_id FROM transactions "
                "WHERE batch_id=?", (batch_id,)).fetchall()
        return {
            "batch_id": batch_id,
            "kind": "transactions",
            "rows": submitted_rows,
            "stored_rows": counters["stored"],
            "skipped_same_value": counters["skipped"],
            "events": len(event_groups),
            "sha256": staged.sha256,
        }

    def upload_samples(self, filename: str, source) -> dict:
        """Streaming counterpart of :meth:`upload_transactions`.

        Same single-pass, rollback-safe shape: rows stream into a temp staging
        table inside the batch transaction, and a set-based duplicate check
        against the globally unique ``sample_id`` rejects the whole batch.
        """
        staged = self._as_staged(filename, source, "samples.csv")
        try:
            counters = {"submitted": 0}
            conflicts: list[dict] = []

            def insert(conn: sqlite3.Connection, batch_id: int) -> int:
                conn.execute(
                    "CREATE TEMP TABLE sample_staging ("
                    "row_number INTEGER NOT NULL, sample_id TEXT NOT NULL, "
                    "account_id TEXT NOT NULL, audit_at INTEGER NOT NULL, "
                    "label INTEGER NOT NULL)")
                chunk: list[tuple] = []
                submitted_rows = 0
                for s in stream_samples(staged.path):
                    submitted_rows += 1
                    chunk.append((s.row_number, s.sample_id, s.account_id,
                                  s.audit_at, s.label))
                    if len(chunk) >= 1000:
                        conn.executemany(
                            "INSERT INTO temp.sample_staging VALUES (?,?,?,?,?)",
                            chunk)
                        chunk.clear()
                if chunk:
                    conn.executemany(
                        "INSERT INTO temp.sample_staging VALUES (?,?,?,?,?)",
                        chunk)
                if submitted_rows == 0:
                    raise BatchRejected(
                        "样本 CSV 没有有效数据行。",
                        [{"row": 2, "field": "", "code": "no_rows",
                          "message": "至少需要一行样本。", "value": ""}])
                conn.execute(
                    "UPDATE batches SET submitted_rows=? WHERE id=?",
                    (submitted_rows, batch_id))
                dupes = conn.execute(
                    "SELECT s.row_number, s.sample_id, x.batch_id "
                    "FROM temp.sample_staging s "
                    "JOIN samples x ON x.sample_id = s.sample_id "
                    "ORDER BY s.row_number LIMIT 100").fetchall()
                if dupes:
                    for r in dupes:
                        conflicts.append({
                            "row": r["row_number"], "field": "sample_id",
                            "code": "duplicate_sample_id",
                            "message": f"样本号 {r['sample_id']} 已在批次 "
                                       f"#{r['batch_id']} 中存在；"
                                       "样本号必须全局唯一。",
                            "value": r["sample_id"]})
                    raise BatchRejected("样本批次整批拒收：样本号重复。", conflicts)
                inserted = conn.execute(
                    """INSERT INTO samples(batch_id, sample_id, account_id,
                           audit_at, label, row_number)
                       SELECT ?, sample_id, account_id, audit_at, label,
                              row_number FROM temp.sample_staging""",
                    (batch_id,)).rowcount
                counters["submitted"] = submitted_rows
                return inserted

            batch_id = self._commit_batch(staged, "samples", 0, insert)
            submitted_rows = counters["submitted"]
        except BaseException:
            staged.path.unlink(missing_ok=True)
            raise
        return {
            "batch_id": batch_id,
            "kind": "samples",
            "rows": submitted_rows,
            "sha256": staged.sha256,
        }

    # ------------------------------------------------------------------ #
    # Reports
    # ------------------------------------------------------------------ #
    def create_report(self) -> dict:
        """Freeze the source high-watermarks at the instant of creation.

        Only batches committed before this transaction's BEGIN IMMEDIATE
        snapshot are included; later uploads get a higher batch_id and are
        invisible to this report forever, even if they arrive during
        computation.
        """
        with self.db.write_transaction() as conn:
            txn_hi = conn.execute(
                "SELECT COALESCE(MAX(id),0) AS v FROM batches WHERE kind='transactions'"
            ).fetchone()["v"]
            sample_hi = conn.execute(
                "SELECT COALESCE(MAX(id),0) AS v FROM batches WHERE kind='samples'"
            ).fetchone()["v"]
            has_samples = conn.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE batch_id <= ?", (sample_hi,)
            ).fetchone()["n"]
            if sample_hi == 0 or has_samples == 0:
                raise ConflictError("尚无已上传样本，无法生成报告。")
            if txn_hi == 0:
                raise ConflictError("尚无已上传交易，无法生成报告。")
            digest, txn_rows, sample_rows = features.source_digest(
                conn, txn_hi, sample_hi)
            manifest = {
                "txn_batch_hi": txn_hi,
                "sample_batch_hi": sample_hi,
                "txn_batches": self._batch_manifest(conn, "transactions", txn_hi),
                "sample_batches": self._batch_manifest(conn, "samples", sample_hi),
                "transaction_rows": txn_rows,
                "sample_rows": sample_rows,
                "source_digest": digest,
                "window_seconds": features.WINDOW_SECONDS,
                "spec": "latest revision with ingested_at <= audit_at; count "
                        "revise revisions whose occurred_at lies in "
                        "[audit_at-7d, audit_at).",
            }
            public_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO reports(public_id, status, txn_batch_hi,
                       sample_batch_hi, created_at, manifest_json)
                   VALUES (?,?,?,?,?,?)""",
                (public_id, "pending", txn_hi, sample_hi, now_epoch(),
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
            )
        return {"report_id": public_id, "status": "pending", "manifest": manifest}

    def _batch_manifest(self, conn, kind: str, hi: int) -> list[dict]:
        return [
            {"batch_id": r["id"], "filename": r["filename"],
             "sha256": r["sha256"], "stored_rows": r["stored_rows"]}
            for r in conn.execute(
                "SELECT id, filename, sha256, stored_rows FROM batches "
                "WHERE kind = ? AND id <= ? ORDER BY id", (kind, hi))
        ]

    def get_report(self, public_id: str) -> dict | None:
        with self.db.read() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE public_id = ?", (public_id,)
            ).fetchone()
        if row is None:
            return None
        out = {
            "report_id": row["public_id"],
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "sample_count": row["sample_count"],
            "output_bytes": row["output_bytes"],
            "output_sha256": row["output_sha256"],
            "error": row["error"],
            "manifest": json.loads(row["manifest_json"]),
            "downloadable": False,
        }
        if row["status"] == "ready":
            path = Path(row["output_path"]) if row["output_path"] else None
            if path is not None and path.is_file():
                out["downloadable"] = True
            else:
                out["downloadable"] = False
                out["integrity_error"] = "报告文件缺失，需按原版本重新生成。"
        return out

    def report_file(self, public_id: str) -> tuple[Path, str] | None:
        """Return the ready file only if its integrity checks pass."""
        report = self.get_report(public_id)
        if report is None or report["status"] != "ready":
            return None
        with self.db.read() as conn:
            row = conn.execute(
                "SELECT output_path, output_sha256 FROM reports WHERE public_id = ?",
                (public_id,),
            ).fetchone()
        if not row or not row["output_path"]:
            return None
        path = Path(row["output_path"])
        if not path.is_file():
            self._mark_failed(
                public_id, "报告文件缺失，需按原版本重新生成。", allow_ready=True)
            return None
        digest = _hash_file(path)
        if digest != row["output_sha256"]:
            self._mark_failed(
                public_id, "报告文件校验失败（内容与生成时不一致）。", allow_ready=True)
            return None
        return path, digest

    def list_reports(self) -> list[dict]:
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT * FROM reports ORDER BY id DESC").fetchall()
        return [{
            "report_id": r["public_id"],
            "status": r["status"],
            "created_at": r["created_at"],
            "completed_at": r["completed_at"],
            "sample_count": r["sample_count"],
            "txn_batch_hi": r["txn_batch_hi"],
            "sample_batch_hi": r["sample_batch_hi"],
        } for r in rows]

    def _mark_failed(self, public_id: str, message: str,
                     allow_ready: bool = False) -> None:
        guard = "" if allow_ready else " AND status <> 'ready'"
        with self.db.write_transaction() as conn:
            conn.execute(
                "UPDATE reports SET status='failed', error=?, completed_at=?,"
                " output_sha256=NULL WHERE public_id=?" + guard,
                (message, now_epoch(), public_id),
            )

    def claim_next_report(self) -> str | None:
        with self.db.write_transaction() as conn:
            row = conn.execute(
                "SELECT public_id FROM reports WHERE status='pending' "
                "ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE reports SET status='running', started_at=? "
                "WHERE public_id=?", (now_epoch(), row["public_id"]))
            return row["public_id"]

    def run_report(self, public_id: str) -> None:
        """Build output in a temp file, fsync, then atomically publish.

        ``status`` flips to ``ready`` only after the file is durably in place,
        so a partial write can never be downloaded.
        """
        try:
            with self.db.read() as conn:
                row = conn.execute(
                    "SELECT * FROM reports WHERE public_id=?", (public_id,)
                ).fetchone()
                if row is None:
                    return
                manifest = json.loads(row["manifest_json"])
                digest_before, _, _ = features.source_digest(
                    conn, row["txn_batch_hi"], row["sample_batch_hi"])
                if digest_before != manifest["source_digest"]:
                    raise RuntimeError("源数据与冻结清单不一致，拒绝生成。")
                rows = features.compute_rows(
                    conn, row["txn_batch_hi"], row["sample_batch_hi"])
            payload = features.render_csv(rows)
            if not rows:
                raise RuntimeError("样本集为空，拒绝产出空报告。")

            final_path = self.settings.reports_dir / f"{public_id}.csv"
            fd, tmp_name = tempfile.mkstemp(
                dir=self.settings.reports_dir, prefix=f".{public_id}-", suffix=".part")
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(payload)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_name, final_path)
                self._fsync_dir(self.settings.reports_dir)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise
            out_digest = hashlib.sha256(payload).hexdigest()
            with self.db.write_transaction() as conn:
                current = conn.execute(
                    "SELECT status FROM reports WHERE public_id=?",
                    (public_id,)).fetchone()
                if current is None or current["status"] != "running":
                    # Attempt was superseded; discard unpublished output.
                    final_path.unlink(missing_ok=True)
                    return
                conn.execute(
                    """UPDATE reports SET status='ready', completed_at=?,
                           sample_count=?, output_path=?, output_sha256=?,
                           output_bytes=?, error=NULL
                       WHERE public_id=?""",
                    (now_epoch(), len(rows), str(final_path), out_digest,
                     len(payload), public_id))
        except Exception as exc:  # noqa: BLE001 - surfaced to operator
            self._mark_failed(public_id, f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            dfd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    def recover_interrupted(self) -> int:
        """A crash mid-build leaves 'running' rows; requeue them."""
        with self.db.write_transaction() as conn:
            cur = conn.execute(
                "UPDATE reports SET status='pending', started_at=NULL "
                "WHERE status='running'")
            return cur.rowcount

    # ------------------------------------------------------------------ #
    # Regeneration from the original version
    # ------------------------------------------------------------------ #
    def regenerate_report(self, public_id: str) -> dict:
        """Re-run against the frozen high-watermarks, never current data."""
        report = self.get_report(public_id)
        if report is None:
            raise LookupError(public_id)
        manifest = report["manifest"]
        with self.db.write_transaction() as conn:
            digest_now, _, _ = features.source_digest(
                conn, manifest["txn_batch_hi"], manifest["sample_batch_hi"])
            if digest_now != manifest["source_digest"]:
                raise ConflictError(
                    "冻结源数据校验失败：已入库批次内容被篡改，无法按原版本重取。")
            new_id = uuid.uuid4().hex
            conn.execute(
                """INSERT INTO reports(public_id, status, txn_batch_hi,
                       sample_batch_hi, created_at, manifest_json)
                   VALUES (?,?,?,?,?,?)""",
                (new_id, "pending", manifest["txn_batch_hi"],
                 manifest["sample_batch_hi"], now_epoch(),
                 json.dumps({**manifest, "regenerated_from": public_id},
                            ensure_ascii=False, sort_keys=True)),
            )
        return {"report_id": new_id, "status": "pending",
                "regenerated_from": public_id}

    # ------------------------------------------------------------------ #
    # Preview surfaces for the UI
    # ------------------------------------------------------------------ #
    def batch_summary(self) -> dict:
        with self.db.read() as conn:
            batches = [
                dict(r) for r in conn.execute(
                    """SELECT id, kind, filename, byte_size, sha256, stored_rows,
                              uploaded_at FROM batches ORDER BY id DESC""")
            ]
        return {"batches": batches}

    def preview_report(self, public_id: str, limit: int) -> dict | None:
        report = self.get_report(public_id)
        if report is None:
            return None
        rows: list[features.ReportRow] = []
        if report["status"] == "ready" and report["downloadable"]:
            m = report["manifest"]
            with self.db.read() as conn:
                rows = features.compute_rows(conn, m["txn_batch_hi"],
                                             m["sample_batch_hi"])
        return {
            "report": report,
            "headers": features.OUTPUT_HEADERS,
            "rows": [list(r) for r in features.iter_preview(rows, limit)],
            "total": len(rows),
        }


def format_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%d %H:%M:%SZ")

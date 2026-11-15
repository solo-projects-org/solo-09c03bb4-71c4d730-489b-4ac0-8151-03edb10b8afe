"""Strict, *streaming* CSV parsing, canonicalisation and UTC-timestamp handling.

The parser is deliberately unforgiving: a submitted batch is accepted in full
or rejected in full. Every problem is collected so the operator sees the
diagnosis in one pass; the detail list is capped (a counter is retained) so a
pathological file can never exhaust memory through error objects either.

Parsing works off an on-disk spool file, one CSV record at a time, so a
hundred-thousand-row upload never has its decoded text, row dictionaries or
parsed objects resident in memory simultaneously.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator, BinaryIO

UTC = timezone.utc
MIN_EPOCH = -62_135_596_800   # 0001-01-01T00:00:00Z
MAX_EPOCH = 253_402_300_799   # 9999-12-31T23:59:59Z

# Keep error payloads bounded: details beyond this many are summarised rather
# than retained, while the total count is still reported.
MAX_REPORTED_ISSUES = 100

TRANSACTION_HEADERS = [
    "event_id", "account_id", "occurred_at", "ingested_at",
    "amount", "action",
]
SAMPLE_HEADERS = ["sample_id", "account_id", "audit_at", "label"]

TRANSACTION_ALIASES = {
    "event_id": {"event_id", "event", "事件号", "事件id", "事件编号"},
    "account_id": {"account_id", "account", "账户", "账户号", "账号"},
    "occurred_at": {"occurred_at", "occurred", "发生时刻", "发生时间", "交易时刻"},
    "ingested_at": {"ingested_at", "ingested", "源系统入库时刻", "入库时刻",
                    "入库时间", "入湖时刻"},
    "amount": {"amount", "金额", "交易金额"},
    "action": {"action", "更新/撤销", "操作", "动作", "更新撤销"},
}
SAMPLE_ALIASES = {
    "sample_id": {"sample_id", "sample", "样本号", "样本id", "样本编号"},
    "account_id": {"account_id", "account", "账户", "账户号", "账号"},
    "audit_at": {"audit_at", "audit", "审核时刻", "审核时间", "审核点"},
    "label": {"label", "标签", "目标", "y"},
}
ACTION_REVISE = "revise"
ACTION_CANCEL = "cancel"

_ASCII_WS = re.compile(r"[ \t\r\n\f\v]+")
_EPOCH_RE = re.compile(r"-?\d{1,11}")
_DECIMAL_RE = re.compile(r"-?\d{1,15}(?:\.\d{1,2})?")
_LABEL_RE = re.compile(r"0|1")


class ParseError(Exception):
    """Base class for upload failures that map to HTTP 422."""

    code = "invalid_csv"

    def __init__(self, message: str, details: list[dict] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or []

    def payload(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


class BatchRejected(ParseError):
    """The whole batch is refused because of one or more row-level problems."""

    code = "batch_rejected"

    def __init__(self, message: str, details: list[dict]):
        super().__init__(message, details)


@dataclass(frozen=True)
class TransactionRevision:
    row_number: int
    event_id: str
    account_id: str
    occurred_at: int
    ingested_at: int
    amount: Decimal
    amount_minor: int
    amount_text: str
    action: str
    raw_hash: str


@dataclass(frozen=True)
class SampleRow:
    row_number: int
    sample_id: str
    account_id: str
    audit_at: int
    label: int


@dataclass
class ParsedBatch:
    transactions: list[TransactionRevision] = field(default_factory=list)


@dataclass
class ParsedSamples:
    samples: list[SampleRow] = field(default_factory=list)


def _row_error(row: int, field_name: str, code: str, message: str, value: str = "") -> dict:
    return {"row": row, "field": field_name, "code": code,
            "message": message, "value": value}


class _IssueCollector:
    """Accumulates row errors with a hard cap on retained detail objects."""

    def __init__(self) -> None:
        self.details: list[dict] = []
        self.total = 0

    def add(self, detail: dict) -> None:
        self.total += 1
        if len(self.details) < MAX_REPORTED_ISSUES:
            self.details.append(detail)

    def summary(self) -> list[dict]:
        if self.total > len(self.details):
            return self.details + [_row_error(
                0, "", "more_issues",
                f"仅展示前 {MAX_REPORTED_ISSUES} 条问题，本批次共 {self.total} 处。")]
        return self.details


def _normalise_key(cell: str) -> str:
    return cell.replace("﻿", "").strip().lower().replace(" ", "").replace("-", "_")


def _map_header(header: list[str], expected: list[str], kind: str,
                aliases: dict[str, set[str]] | None) -> tuple[dict[str, int], int]:
    """Validate the header row and return column indexes plus row width."""
    clean = [cell.replace("﻿", "").strip() for cell in header]
    if aliases is None:
        if clean != expected:
            raise BatchRejected(
                f"{kind} CSV 表头必须严格为：{', '.join(expected)}。",
                [_row_error(1, "header", "header", "表头不符合约定",
                            ", ".join(clean))],
            )
        return {name: i for i, name in enumerate(expected)}, len(header)
    normalized: dict[str, int] = {}
    alias_keys = {
        name: {_normalise_key(n) for n in names}
        for name, names in aliases.items()
    }
    for i, cell in enumerate(header):
        key = _normalise_key(cell)
        canonical = None
        for name, keys in alias_keys.items():
            if key in keys:
                canonical = name
                break
        if canonical is None:
            continue  # extra columns are tolerated and ignored
        if canonical in normalized:
            raise BatchRejected(
                f"{kind} CSV 表头含重复列 {canonical}。",
                [_row_error(1, canonical, "duplicate_header",
                            f"第 {normalized[canonical] + 1} 列与第 {i + 1} 列重复。")],
            )
        normalized[canonical] = i
    missing = [name for name in expected if name not in normalized]
    if missing:
        raise BatchRejected(
            f"{kind} CSV 缺少必需列：{', '.join(missing)}。",
            [_row_error(1, "header", "missing_header",
                        f"缺少列：{', '.join(missing)}；实际表头：{', '.join(clean)}")],
        )
    return normalized, len(header)


def _iter_records(source: str | os.PathLike | BinaryIO, kind: str
                  ) -> Iterator[tuple[int, list[str]]]:
    """Yield ``(row_number, cells)`` one CSV record at a time.

    The file is decoded incrementally as UTF-8 (BOM tolerated); neither the
    decoded text nor the full row list is ever materialised. Decode and CSV
    structural errors are translated to :class:`BatchRejected` with the
    offending source row number.
    """
    owns = not hasattr(source, "read")
    fh: BinaryIO | None = None
    try:
        if owns:
            fh = open(source, "rb")  # type: ignore[arg-type]
            stream: BinaryIO = fh
        else:
            stream = source  # type: ignore[assignment]
        text = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="",
                                errors="strict")
        reader = csv.reader(text, strict=True)
        row_number = 0
        while True:
            row_number += 1
            try:
                record = next(reader)
            except StopIteration:
                return
            except UnicodeDecodeError as exc:
                raise BatchRejected(
                    f"{kind} CSV 必须是 UTF-8 编码（可带 BOM）。",
                    [_row_error(row_number, "", "encoding",
                                f"无法按 UTF-8 解码（第 {row_number} 行附近）：{exc}")],
                )
            except csv.Error as exc:
                raise BatchRejected(
                    f"{kind} CSV 格式损坏，无法解析：{exc}",
                    [_row_error(row_number, "", "malformed_csv", str(exc))],
                )
            yield row_number, record
    finally:
        if owns and fh is not None:
            fh.close()


def _is_blank(cells: list[str]) -> bool:
    for c in cells:
        if c.strip():
            return False
    return True


def _has_nul(cells: list[str]) -> bool:
    for c in cells:
        if "\x00" in c:
            return True
    return False


def stream_transactions(source: str | os.PathLike | BinaryIO
                        ) -> Iterator[TransactionRevision]:
    """Yield validated transaction revisions one row at a time.

    Header/encoding/structural failures raise immediately; row validation
    failures are collected and raised as :class:`BatchRejected` when iteration
    of the spool completes, so no valid-looking consumer can swallow a
    rejected batch.
    """
    records = _iter_records(source, "交易")
    try:
        header_row, header = next(records)
    except StopIteration:
        raise BatchRejected("交易 CSV 为空。",
                            [_row_error(1, "", "empty", "未找到表头。")])
    column_map, width = _map_header(header, TRANSACTION_HEADERS, "交易",
                                    TRANSACTION_ALIASES)
    issues = _IssueCollector()
    data_rows = 0
    emitted = 0
    for row_number, cells in records:
        if _is_blank(cells):
            continue
        data_rows += 1
        if _has_nul(cells):
            issues.add(_row_error(row_number, "", "nul_byte",
                                  "文件包含 NUL 字符。"))
            continue
        if len(cells) != width:
            issues.add(_row_error(row_number, "", "column_count",
                                  f"应有 {width} 列，实际 {len(cells)} 列。"))
            continue
        row_issues: list[dict] = []
        n = row_number
        event_id = _parse_text(cells[column_map["event_id"]], n, "event_id",
                               row_issues, "事件号")
        account_id = _parse_text(cells[column_map["account_id"]], n,
                                 "account_id", row_issues, "账户")
        occurred = _parse_epoch(cells[column_map["occurred_at"]], n,
                                "occurred_at", row_issues, "发生时刻")
        ingested = _parse_epoch(cells[column_map["ingested_at"]], n,
                                "ingested_at", row_issues, "入库时刻")
        amount = _parse_decimal(cells[column_map["amount"]], n, "amount",
                                row_issues, "金额")
        action = _parse_action(cells[column_map["action"]], n, row_issues)
        if action == ACTION_REVISE and amount is not None and amount == 0:
            row_issues.append(_row_error(n, "amount", "amount_positive",
                                         "更新交易的金额必须大于 0。",
                                         cells[column_map["amount"]]))
        for detail in row_issues:
            issues.add(detail)
        if row_issues:
            continue
        if not (event_id and account_id and occurred is not None
                and ingested is not None and amount is not None and action):
            continue
        emitted += 1
        yield TransactionRevision(
            row_number=n, event_id=event_id, account_id=account_id,
            occurred_at=occurred, ingested_at=ingested, amount=amount,
            amount_minor=int(amount * 100),
            amount_text=format(amount, ".2f"),
            action=action,
            raw_hash=canonical_raw_hash([
                event_id, account_id, str(occurred), str(ingested),
                format(amount, ".2f"), action,
            ]),
        )
    if issues.total:
        raise BatchRejected(
            f"交易批次整批拒收：共 {data_rows} 行，发现 {issues.total} 处问题，"
            "未写入任何数据。",
            issues.summary(),
        )
    if emitted == 0:
        raise BatchRejected("交易 CSV 没有有效数据行。",
                            [_row_error(2, "", "no_rows", "至少需要一行交易。")])


def stream_samples(source: str | os.PathLike | BinaryIO) -> Iterator[SampleRow]:
    """Yield validated sample rows one at a time (in-file duplicates checked)."""
    records = _iter_records(source, "样本")
    try:
        _, header = next(records)
    except StopIteration:
        raise BatchRejected("样本 CSV 为空。",
                            [_row_error(1, "", "empty", "未找到表头。")])
    column_map, width = _map_header(header, SAMPLE_HEADERS, "样本",
                                    SAMPLE_ALIASES)
    issues = _IssueCollector()
    seen: dict[str, int] = {}
    data_rows = 0
    emitted = 0
    for row_number, cells in records:
        if _is_blank(cells):
            continue
        data_rows += 1
        if _has_nul(cells):
            issues.add(_row_error(row_number, "", "nul_byte",
                                  "文件包含 NUL 字符。"))
            continue
        if len(cells) != width:
            issues.add(_row_error(row_number, "", "column_count",
                                  f"应有 {width} 列，实际 {len(cells)} 列。"))
            continue
        n = row_number
        row_issues: list[dict] = []
        sample_id = _parse_text(cells[column_map["sample_id"]], n, "sample_id",
                                row_issues, "样本号")
        account_id = _parse_text(cells[column_map["account_id"]], n,
                                 "account_id", row_issues, "账户")
        audit = _parse_epoch(cells[column_map["audit_at"]], n, "audit_at",
                             row_issues, "审核时刻")
        label_raw = cells[column_map["label"]].strip()
        label = None
        if not label_raw:
            row_issues.append(_row_error(n, "label", "required", "标签不能为空。"))
        elif not _LABEL_RE.fullmatch(label_raw):
            row_issues.append(_row_error(n, "label", "invalid_label",
                                         "标签只能是 0 或 1。",
                                         cells[column_map["label"]]))
        else:
            label = int(label_raw)
        for detail in row_issues:
            issues.add(detail)
        if row_issues:
            continue
        if not (sample_id and account_id and audit is not None
                and label is not None):
            continue
        if sample_id in seen:
            issues.add(_row_error(n, "sample_id", "duplicate_sample_id",
                                  f"样本号与第 {seen[sample_id]} 行重复。"))
            continue
        seen[sample_id] = n
        emitted += 1
        yield SampleRow(n, sample_id, account_id, audit, label)
    if issues.total:
        raise BatchRejected(
            f"样本批次整批拒收：共 {data_rows} 行，发现 {issues.total} 处问题，"
            "未写入任何数据。",
            issues.summary(),
        )
    if emitted == 0:
        raise BatchRejected("样本 CSV 没有有效数据行。",
                            [_row_error(2, "", "no_rows", "至少需要一行样本。")])


def _spool_bytes(data: bytes) -> Path:
    """Back-compat helper: park raw bytes in a temp file for the streamers."""
    fd, name = tempfile.mkstemp(prefix="csv-spool-", suffix=".csv")
    path = Path(name)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    return path


def parse_transactions(data: bytes) -> ParsedBatch:
    """Whole-bytes convenience wrapper around the streaming parser.

    Production ingestion streams from the upload spool file; this remains for
    callers and tests that already hold the payload in memory.
    """
    path = _spool_bytes(data)
    try:
        return ParsedBatch(transactions=list(stream_transactions(path)))
    finally:
        path.unlink(missing_ok=True)


def parse_samples(data: bytes) -> ParsedSamples:
    path = _spool_bytes(data)
    try:
        return ParsedSamples(samples=list(stream_samples(path)))
    finally:
        path.unlink(missing_ok=True)


def _parse_text(value: str, row: int, name: str, issues: list[dict],
                label: str, max_len: int = 128) -> str:
    v = value.strip()
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return ""
    if len(v) > max_len:
        issues.append(_row_error(row, name, "too_long",
                                 f"{label}超过 {max_len} 字符。", v[:40]))
        return ""
    return v


def _parse_epoch(value: str, row: int, name: str, issues: list[dict],
                 label: str) -> int | None:
    v = value.strip()
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return None
    if not _EPOCH_RE.fullmatch(v):
        issues.append(_row_error(
            row, name, "not_utc_seconds",
            f"{label}必须是 UTC 秒整数（如 1736071200），不接受日期字符串或小数。", v))
        return None
    n = int(v)
    if not (MIN_EPOCH <= n <= MAX_EPOCH):
        issues.append(_row_error(row, name, "out_of_range",
                                 f"{label}超出可表示范围。", v))
        return None
    return n


def _parse_decimal(value: str, row: int, name: str, issues: list[dict],
                   label: str) -> Decimal | None:
    v = _ASCII_WS.sub("", value)
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return None
    # Plain decimal only: no scientific notation, no thousands separators,
    # no currency symbols. At most two fractional digits keeps the stored
    # integer minor-unit value exact.
    if not _DECIMAL_RE.fullmatch(v):
        issues.append(_row_error(
            row, name, "not_decimal",
            f"{label}必须是十进制金额（最多两位小数，如 125.00），且不得为科学计数法。", v))
        return None
    try:
        d = Decimal(v)
    except InvalidOperation:  # pragma: no cover - regex already guards this
        issues.append(_row_error(row, name, "not_decimal", f"{label}无法解析。", v))
        return None
    if d < 0:
        issues.append(_row_error(row, name, "amount_negative",
                                 f"{label}不能为负。", v))
        return None
    return d


def _parse_action(value: str, row: int, issues: list[dict]) -> str | None:
    v = value.strip().lower()
    if v in (ACTION_REVISE, "update", "correct", "修订", "更新"):
        return ACTION_REVISE
    if v in (ACTION_CANCEL, "void", "reverse", "撤销", "作废"):
        return ACTION_CANCEL
    if not v:
        issues.append(_row_error(row, "action", "required", "action 不能为空。"))
    else:
        issues.append(_row_error(row, "action", "invalid_action",
                                 "action 只能是 revise/update/correct 或 cancel/void/撤销。", value))
    return None


def canonical_raw_hash(fields: list[str]) -> str:
    """Hash the *semantic* revision identity.

    Same event + same ingested_at must carry identical content. We hash the
    parsed semantic values rather than raw bytes so insignificant whitespace,
    quote style, or CRLF differences do not create phantom conflicts.
    """
    h = hashlib.sha256()
    for part in fields:
        h.update(part.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def fmt_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


WINDOW_SECONDS = 7 * 24 * 3600

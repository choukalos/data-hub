"""Generic CSV/TSV dataset adapter (v2a).

Drop any CSV/TSV into the inbox (or POST it) and it lands idempotently in a
MySQL table under the hub database. Per-dataset config comes from a
hub.datasets registry row (plain dict): name, source_pattern, target_table,
delimiter, has_header, column_map (JSON {"csv_col": "target_col TYPE"} or
NULL), key_columns (JSON list of target columns or NULL).

Column inference (only when column_map is NULL): sample up to 200 rows; per
column: all-integer -> BIGINT, integer-or-float -> DECIMAL(18,4), ISO-ish
dates -> DATE, else VARCHAR(512). Empty values -> NULL. Target column names:
lowercased, non-alnum -> _.

content_hash = sha256(target_table | key column values joined by \\x1f) when
key_columns is set, else sha256 of all normalized values (in column order,
joined by \\x1f).
"""

import csv
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from fnmatch import fnmatch
from typing import Optional

from .base import Adapter, LoadResult

SAMPLE_ROWS = 200

# Internal columns of every target table (id PK, content_hash, ingested_at).
# Data columns that slugify to one of these get a col_ prefix instead.
RESERVED_COLS = frozenset({"id", "content_hash", "ingested_at"})


def safe_column_name(tcol: str) -> str:
    """Rename data columns that would collide with the table's internals."""
    return f"col_{tcol}" if tcol in RESERVED_COLS else tcol

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


@dataclass
class DatasetRow:
    """One normalized generic-dataset row (target column -> value)."""

    values: dict  # target column -> normalized value (None = NULL)
    content_hash: str


def slugify(name: str) -> str:
    """Lowercase; every non-alphanumeric character -> _ (per spec)."""
    s = re.sub(r"[^0-9a-z]", "_", name.strip().lower())
    return s or "col"


def _parse_iso_date(raw: str) -> Optional[date]:
    """ISO-ish date (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD) -> date, else None."""
    s = raw.strip()
    for cand in (s, s.replace("/", "-"), s.replace(".", "-")):
        try:
            return date.fromisoformat(cand)
        except ValueError:
            pass
    return None


def infer_type(values: list) -> str:
    """Infer a MySQL column type from non-empty string values (a sample).

    All-integer -> BIGINT; integer-or-float -> DECIMAL(18,4); all ISO-ish
    dates -> DATE; else VARCHAR(512). Empty sample -> VARCHAR(512).
    """
    if not values:
        return "VARCHAR(512)"
    if all(_INT_RE.match(v) for v in values):
        return "BIGINT"
    if all(_INT_RE.match(v) or _FLOAT_RE.match(v) for v in values):
        return "DECIMAL(18,4)"
    if all(_parse_iso_date(v) is not None for v in values):
        return "DATE"
    return "VARCHAR(512)"


def parse_column_map(raw) -> Optional[dict]:
    """hub.datasets.column_map (JSON str / dict / None) -> {csv_col: "target TYPE"}."""
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw.strip():
            return None
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("column_map must be a JSON object")
    return {str(k): str(v) for k, v in raw.items()}


def parse_key_columns(raw) -> Optional[list]:
    """hub.datasets.key_columns (JSON str / list / None) -> [target col, ...]."""
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw.strip():
            return None
        raw = json.loads(raw)
    if not isinstance(raw, list):
        raise ValueError("key_columns must be a JSON list")
    return [str(c) for c in raw]


def split_entry(entry: str) -> tuple:
    """'target_col TYPE' -> (target_col, TYPE)."""
    tcol, sep, ctype = entry.rpartition(" ")
    if not sep or not tcol or not ctype:
        raise ValueError(f"bad column_map entry {entry!r}")
    return tcol, ctype


def normalize_value(ctype: str, raw: str):
    """Normalize one raw CSV cell for a column type. Empty -> None (NULL)."""
    s = (raw or "").strip()
    if not s:
        return None
    if ctype == "BIGINT":
        try:
            return int(s)
        except ValueError:
            return None
    if ctype == "DECIMAL(18,4)":
        try:
            return Decimal(s)
        except InvalidOperation:
            return None
    if ctype == "DATE":
        return _parse_iso_date(s)
    return s


def _hash_repr(v) -> str:
    """Canonical string form of a normalized value for content hashing."""
    if v is None:
        return ""
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        try:
            return str(v.quantize(Decimal("0.0001")))
        except InvalidOperation:
            return str(v)
    return str(v)


def content_hash(target_table: str, key_columns, values: dict, ordered_cols: list) -> str:
    """sha256(target_table | key values joined by \\x1f); all values if no keys."""
    if key_columns:
        parts = [target_table] + [_hash_repr(values.get(k)) for k in key_columns]
    else:
        parts = [_hash_repr(values.get(c)) for c in ordered_cols]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _dedupe_slugs(cmap: dict) -> dict:
    """Rename reserved-colliding target names; suffix other collisions (_2, ...)."""
    used = set()
    out = {}
    for k, v in cmap.items():
        tcol, ctype = split_entry(v)
        tcol = safe_column_name(tcol)
        cand, i = tcol, 1
        while cand in used:
            i += 1
            cand = f"{tcol}_{i}"
        used.add(cand)
        out[k] = f"{cand} {ctype}"
    return out


class GenericCsvAdapter(Adapter):
    """Generic CSV/TSV adapter, configured by a hub.datasets row (dict)."""

    name = "generic_csv"

    def __init__(self, dataset: dict):
        self.dataset = dataset or {}
        # Final {csv_col: "target TYPE"} map; set by load() (frozen map from
        # the registry, or inferred + deduped on first sight).
        self.column_map: Optional[dict] = None

    @property
    def dataset_name(self) -> str:
        return str(self.dataset.get("name") or "dataset")

    @property
    def target_table(self) -> str:
        return str(self.dataset.get("target_table") or f"hub.ds_{self.dataset_name}")

    def _delimiter(self) -> str:
        return str(self.dataset.get("delimiter") or ",")

    def detect(self, path: str) -> bool:
        """True if the file basename matches the dataset's source_pattern
        (fnmatch) and looks like CSV/TSV."""
        pattern = self.dataset.get("source_pattern")
        if not pattern:
            return False
        base = os.path.basename(path)
        if not fnmatch(base, str(pattern)):
            return False
        return base.lower().endswith((".csv", ".tsv"))

    def load(self, path: str) -> LoadResult:
        """Read + normalize the file into DatasetRows. Pure: no database."""
        result = LoadResult()
        delim = self._delimiter()
        h = self.dataset.get("has_header")
        has_header = (int(h) if h is not None else 1) != 0
        column_map = parse_column_map(self.dataset.get("column_map"))
        key_columns = parse_key_columns(self.dataset.get("key_columns"))
        target_table = self.target_table
        fname = os.path.basename(path)

        with open(path, newline="", encoding="utf-8-sig") as f:
            raw_rows = list(csv.reader(f, delimiter=delim))
        # Drop truly blank lines (trailing newlines etc.); a row of all-empty
        # cells carries no data and would only collide on its hash.
        rows = [r for r in raw_rows if r and any((v or "").strip() for v in r)]

        if not rows:
            result.issues.append(f"{fname}: empty file — no rows")
            self.column_map = column_map or {}
            return result

        if has_header:
            header = [(x or "").strip() for x in rows[0]]
            data_rows = rows[1:]
            first_line = 2
        else:
            header = [f"col_{i + 1}" for i in range(len(rows[0]))]
            data_rows = rows
            first_line = 1

        if not data_rows:
            result.issues.append(f"{fname}: header only, no data rows")
            self.column_map = column_map or {}
            return result

        n_cols = len(header)
        valid = []
        for off, r in enumerate(data_rows):
            lineno = first_line + off
            if len(r) != n_cols:
                result.skipped += 1
                result.issues.append(
                    f"line {lineno}: column count {len(r)} != {n_cols} — row skipped"
                )
                continue
            valid.append((lineno, r))

        if column_map is None:
            # Infer from up to SAMPLE_ROWS valid rows (per spec).
            sample = [r for _, r in valid[:SAMPLE_ROWS]]
            column_map = {}
            for i, hcol in enumerate(header):
                vals = [r[i].strip() for r in sample if i < len(r) and (r[i] or "").strip()]
                column_map[hcol] = f"{slugify(hcol)} {infer_type(vals)}"
            column_map = _dedupe_slugs(column_map)
        else:
            # Frozen/user map: sanitize reserved-colliding target names so
            # values/hashes and the pipeline DDL stay consistent.
            column_map = {
                k: f"{safe_column_name(split_entry(v)[0])} {split_entry(v)[1]}"
                for k, v in column_map.items()
            }
            for hcol in header:
                if hcol not in column_map:
                    result.issues.append(f"column {hcol!r} not in column_map — ignored")
            for k in column_map:
                if k not in header:
                    result.issues.append(f"column_map key {k!r} not in file — ignored")
        self.column_map = column_map

        ordered = [split_entry(column_map[hcol])[0] for hcol in header if hcol in column_map]
        out = []
        for lineno, r in valid:
            values = {}
            for i, hcol in enumerate(header):
                entry = column_map.get(hcol)
                if entry is None:
                    continue
                tcol, ctype = split_entry(entry)
                v = normalize_value(ctype, r[i])
                if v is None and (r[i] or "").strip():
                    result.issues.append(
                        f"line {lineno}: column {hcol!r}: bad {ctype} value "
                        f"{(r[i] or '').strip()!r} — set to NULL"
                    )
                values[tcol] = v
            out.append(DatasetRow(values=values,
                                  content_hash=content_hash(target_table, key_columns, values, ordered)))
        result.rows = out
        return result
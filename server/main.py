import json
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Optional

import mysql.connector
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel

from . import categorize, config, db, ingest
from .adapters.generic_csv import RESERVED_COLS, split_entry
from .etl.pipeline import _qualify, hub_schema_ready

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "migrations", "finance")
HUB_MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "migrations", "hub")

log = logging.getLogger("data-hub.migrate")


def apply_hub_migrations():
    """Apply hub migrations (v2a generic datasets).

    Best-effort: the hub db may not be provisioned yet, so any failure only
    logs a warning and returns [] — finance ingest must never break because
    of it. Returns the list of migration versions applied now.
    """
    try:
        conn = db.get_conn(config.HUB_DB)
    except mysql.connector.Error as e:
        log.warning("hub db unreachable, skipping hub migrations: %s", e)
        return []
    try:
        return db.apply_migrations(HUB_MIGRATIONS_DIR, config.HUB_DB, conn)
    except mysql.connector.Error as e:
        log.warning("hub migrations failed on %r db, skipping: %s", config.HUB_DB, e)
        return []
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = db.connect_with_retry(config.FINANCE_DB)
    try:
        app.state.migrations_applied = db.apply_migrations(MIGRATIONS_DIR, config.FINANCE_DB, conn)
    finally:
        conn.close()
    app.state.hub_migrations_applied = apply_hub_migrations()
    app.state.watcher = ingest.start_watcher()
    yield
    ingest.stop_watcher()


app = FastAPI(title="data-hub", version="0.2.0", lifespan=lifespan)


@app.get("/api/health")
def health():
    conn = db.get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) AS n FROM transactions")
        txn_count = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM load_log")
        load_count = cur.fetchone()["n"]
        cur.execute("SELECT ts, source_file, status FROM load_log ORDER BY ts DESC LIMIT 1")
        last = cur.fetchone()
    finally:
        conn.close()
    return {
        "status": "ok",
        "transactions": txn_count,
        "loads": load_count,
        "last_load": last,
        "pending_files": ingest.discover_files(),
        "migrations_applied_on_start": app.state.migrations_applied,
        "hub_migrations_applied_on_start": getattr(app.state, "hub_migrations_applied", []),
    }


@app.post("/api/migrate")
def migrate():
    """Apply pending migrations (also runs automatically on startup)."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        applied = db.apply_migrations(MIGRATIONS_DIR, config.FINANCE_DB, conn)
    finally:
        conn.close()
    hub_applied = apply_hub_migrations()
    return {"applied": applied, "hub_applied": hub_applied}


@app.get("/api/loads")
def loads(limit: int = Query(50, le=500), db_name: str = Query("finance", alias="db")):
    """Recent loads. ?db=finance (default, unchanged) | hub (v2a generic)."""
    if db_name not in ("finance", "hub"):
        raise HTTPException(400, "db must be 'finance' or 'hub'")
    conn = _hub_conn() if db_name == "hub" else db.get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM load_log ORDER BY ts DESC LIMIT %s", (limit,))
        return {"loads": cur.fetchall()}
    finally:
        conn.close()


@app.get("/api/loads/{load_id}")
def load_detail(load_id: int):
    conn = db.get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM load_log WHERE id = %s", (load_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "load not found")
        return row
    finally:
        conn.close()


@app.post("/api/ingest")
async def ingest_file_upload(
    file: UploadFile = File(...),
    source: str = Query(..., description="source dir: starone | tax"),
):
    """Upload a file and ingest it immediately (same pipeline as the watcher)."""
    data = await file.read()
    try:
        report = ingest.ingest_upload(data, file.filename or "upload", source)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if report["status"] == "error":
        raise HTTPException(500, f"ingest failed: {report['issues']}")
    return report


@app.post("/api/ingest/run")
def ingest_run():
    """Manual trigger: ingest everything currently sitting in watched dirs."""
    reports = ingest.scan_and_ingest()
    return {"ingested": reports}


@app.get("/api/sources")
def sources():
    """Source registry (watched dirs + adapters)."""
    doc = ingest.load_sources()
    return {"inbox_root": config.INBOX_ROOT, **doc}


# --- v2a: generic datasets (hub db) -------------------------------------------

_DATASET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_MYSQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Qualified table 'db.tbl'. Both parts may contain hyphens: dataset slugs
# allow '-', so default target tables look like hub.ds_mcp-e2e. Safe to
# interpolate because _qualify() backticks both parts (backticks themselves
# are rejected here).
_QUALIFIED_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*\.[A-Za-z_][A-Za-z0-9_-]*$")


class DatasetBody(BaseModel):
    name: str
    description: Optional[str] = ""
    source_pattern: str
    delimiter: Optional[str] = ","
    has_header: Optional[int] = 1
    column_map: Optional[dict] = None
    key_columns: Optional[list] = None


def _hub_conn():
    """Connect to the hub db; 503 if it is not provisioned/migrated yet.

    'Not ready' covers both the db being unreachable and the db existing
    without the v2a tables (migrations not yet applied — e.g. the container
    started before provision.sh ran). Both degrade to 503, never 500.
    """
    try:
        conn = db.get_conn(config.HUB_DB)
    except mysql.connector.Error as e:
        raise HTTPException(503, f"hub db unavailable: {e}")
    try:
        ready = hub_schema_ready(conn)
    except mysql.connector.Error as e:
        conn.close()
        raise HTTPException(503, f"hub db unavailable: {e}")
    if not ready:
        conn.close()
        raise HTTPException(
            503,
            "hub db not migrated (hub.datasets missing) — restart data-hub "
            "or POST /api/migrate",
        )
    return conn


def _validate_dataset(body: DatasetBody) -> str:
    """Validate a registration body; return the normalized name (400 on bad)."""
    name = (body.name or "").strip()
    if not _DATASET_NAME_RE.match(name) or len(name) > 70:
        raise HTTPException(400, "name must be a slug (lowercase letters, digits, _ or -)")
    pattern = (body.source_pattern or "").strip()
    if not pattern:
        raise HTTPException(400, "source_pattern is required")
    if body.delimiter not in (",", "\t"):
        raise HTTPException(400, "delimiter must be ',' or '\\t'")
    if int(body.has_header or 0) not in (0, 1):
        raise HTTPException(400, "has_header must be 0 or 1")
    if body.column_map is not None:
        if not isinstance(body.column_map, dict) or not body.column_map:
            raise HTTPException(400, "column_map must be a non-empty object")
        for k, v in body.column_map.items():
            if not str(k).strip():
                raise HTTPException(400, f"column_map key {k!r} is empty")
            try:
                tcol, _ctype = split_entry(str(v))
            except ValueError:
                raise HTTPException(400, f"column_map value for {k!r} must be 'target_col TYPE'")
            if not _MYSQL_IDENT_RE.match(tcol):
                raise HTTPException(400, f"column_map target {tcol!r} is not a valid identifier")
    if body.key_columns is not None:
        if (not isinstance(body.key_columns, list) or not body.key_columns
                or not all(isinstance(c, str) and _MYSQL_IDENT_RE.match(c)
                           for c in body.key_columns)):
            raise HTTPException(400, "key_columns must be a non-empty list of column names")
        reserved = [c for c in body.key_columns if c in RESERVED_COLS]
        if reserved:
            raise HTTPException(
                400,
                f"key_columns {reserved} are reserved (id/content_hash/ingested_at) and "
                "are renamed on import (e.g. id -> col_id); use the renamed target "
                "column name instead",
            )
    return name


def _get_dataset(name: str) -> dict:
    conn = _hub_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM datasets WHERE name=%s", (name,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, f"dataset {name!r} not found")
    return row


def _dataset_row_count(conn, target_table) -> int:
    if not target_table or not _QUALIFIED_TABLE_RE.match(str(target_table)):
        return 0
    try:
        cur = conn.cursor()  # tuple cursor (the list cursor is dictionary)
        cur.execute(f"SELECT COUNT(*) FROM {_qualify(target_table)}")
        return cur.fetchone()[0]
    except mysql.connector.Error:
        return 0  # target table not created yet (no import yet)


@app.get("/api/datasets")
def list_datasets():
    """Generic dataset registry (hub db) with target-table row counts."""
    conn = _hub_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM datasets ORDER BY name")
        out = []
        for ds in cur.fetchall():
            ds["row_count"] = _dataset_row_count(conn, ds.get("target_table"))
            out.append(ds)
        return {"datasets": out}
    finally:
        conn.close()


@app.post("/api/datasets")
def create_dataset(body: DatasetBody):
    """Register a generic dataset. target_table defaults to hub.ds_<name>."""
    name = _validate_dataset(body)
    target_table = f"{config.HUB_DB}.ds_{name}"
    conn = _hub_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO datasets
               (name, description, source_pattern, target_table, delimiter,
                has_header, column_map, key_columns)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (name, body.description or "", body.source_pattern.strip(),
             target_table, body.delimiter, int(body.has_header or 1),
             json.dumps(body.column_map) if body.column_map is not None else None,
             json.dumps(body.key_columns) if body.key_columns is not None else None),
        )
        conn.commit()
    except mysql.connector.IntegrityError:
        raise HTTPException(409, f"dataset {name!r} already exists")
    finally:
        conn.close()
    return _get_dataset(name)


@app.delete("/api/datasets/{name}")
def deactivate_dataset(name: str):
    """Deactivate a dataset (rows kept); the watcher stops claiming its files."""
    conn = _hub_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE datasets SET is_active=0 WHERE name=%s", (name,))
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, f"dataset {name!r} not found")
    finally:
        conn.close()
    return {"name": name, "is_active": 0}


@app.get("/api/datasets/{name}/preview")
def dataset_preview(name: str, limit: int = Query(50, le=500)):
    """Latest rows from the dataset's target table (newest first)."""
    ds = _get_dataset(name)
    ttable = ds.get("target_table")
    if not ttable or not _QUALIFIED_TABLE_RE.match(str(ttable)):
        raise HTTPException(500, f"dataset {name!r} has a bad target_table {ttable!r}")
    conn = _hub_conn()
    try:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"SELECT * FROM {_qualify(ttable)} ORDER BY id DESC LIMIT %s", (limit,)
            )
            rows = cur.fetchall()
        except mysql.connector.Error:
            rows = []  # target table not created yet (no import yet)
    finally:
        conn.close()
    return {"dataset": name, "target_table": ttable, "count": len(rows), "rows": rows}


# --- phase 2 (categorization) -------------------------------------------------

class ReviewBody(BaseModel):
    suggestion_id: int
    action: str  # approve | reject | edit
    category_id: Optional[int] = None  # required for action='edit'


@app.get("/api/categories")
def categories():
    """Nested category tree (buckets -> children) with txn counts + totals."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        return categorize.category_tree(conn)
    finally:
        conn.close()


@app.get("/api/categories/pending")
def pending_suggestions():
    """AI suggestions awaiting review (with category name/bucket + impact)."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        return categorize.pending_suggestions(conn)
    finally:
        conn.close()


@app.post("/api/categories/apply-rules")
def apply_rules():
    """Re-run the rules engine over existing NULL-category transactions."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        return categorize.apply_rules(conn)
    finally:
        conn.close()


@app.post("/api/categories/suggest")
def suggest_categories():
    """Run one AI suggestion pass now (LiteLLM); returns count + LLM log."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        return categorize.suggest_categories(conn)
    finally:
        conn.close()


@app.post("/api/categories/review")
def review_suggestion(body: ReviewBody):
    """Approve / reject / edit a pending suggestion.

    approve -> creates an exact payee rule, backfills NULL-category txns,
    rebuilds monthly_category_summary. edit -> change category, then approve.
    """
    conn = db.get_conn(config.FINANCE_DB)
    try:
        return categorize.review_suggestion(
            conn, body.suggestion_id, body.action, body.category_id
        )
    except LookupError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()
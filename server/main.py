import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import mysql.connector
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel

from . import categorize, config, db, ingest

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
def loads(limit: int = Query(50, le=500)):
    conn = db.get_conn(config.FINANCE_DB)
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
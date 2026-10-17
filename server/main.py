import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from . import config, db

MIGRATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "migrations", "finance")


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = db.connect_with_retry(config.FINANCE_DB)
    try:
        app.state.migrations_applied = db.apply_migrations(MIGRATIONS_DIR, config.FINANCE_DB, conn)
    finally:
        conn.close()
    yield


app = FastAPI(title="data-hub", version="0.1.0", lifespan=lifespan)


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
        "migrations_applied_on_start": app.state.migrations_applied,
    }


@app.post("/api/migrate")
def migrate():
    """Apply pending migrations (also runs automatically on startup)."""
    conn = db.get_conn(config.FINANCE_DB)
    try:
        applied = db.apply_migrations(MIGRATIONS_DIR, config.FINANCE_DB, conn)
    finally:
        conn.close()
    return {"applied": applied}


@app.get("/api/loads")
def loads(limit: int = 50):
    conn = db.get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM load_log ORDER BY ts DESC LIMIT %s", (min(limit, 500),))
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
async def ingest():
    # Phase 1: file upload + inbox watcher + adapters + staging/upsert.
    raise HTTPException(501, "ingest lands in phase 1 (waiting on sample file formats)")


@app.get("/api/categories")
def categories():
    # Phase 2: category tree + review flow.
    raise HTTPException(501, "categorization lands in phase 2")


@app.get("/api/categories/pending")
def pending_suggestions():
    raise HTTPException(501, "categorization lands in phase 2")


@app.post("/api/categories/review")
def review_suggestion():
    raise HTTPException(501, "categorization lands in phase 2")
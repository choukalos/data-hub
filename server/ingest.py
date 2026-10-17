"""Ingest orchestration: file discovery, watcher thread, upload path.

Doors (v1):
  - watcher: polls INBOX_ROOT/<dir> for files (sources.yaml), ingests new ones,
    moves them to processed/
  - API: POST /api/ingest (multipart upload, ?source=<dir>) -> same pipeline

Manual by design: chuck drops files when he exports them; every ingest
produces a load_log row + report so issues surface at ingest time.
"""

import os
import shutil
import threading
import json
import logging
from datetime import datetime, timezone

import yaml

from . import categorize, config
from .adapters import ADAPTERS
from .db import get_conn
from .etl.pipeline import _write_error_load, ingest_file

log = logging.getLogger("data-hub.ingest")

INGEST_LOCK = threading.Lock()
_stop = threading.Event()


def load_sources():
    with open(config.SOURCES_FILE) as f:
        doc = yaml.safe_load(f)
    return {
        "sources": doc.get("sources", []),
        "poll_seconds": int(doc.get("poll_seconds", config.POLL_SECONDS)),
        "processed_dirname": doc.get("processed_dirname", config.PROCESSED_DIRNAME),
    }


def _sha256(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def discover_files():
    """List (dir, path) for files in watched dirs, oldest name first."""
    doc = load_sources()
    out = []
    for src in doc["sources"]:
        d = os.path.join(config.INBOX_ROOT, src["dir"])
        if not os.path.isdir(d):
            continue
        for name in sorted(os.listdir(d)):
            p = os.path.join(d, name)
            if os.path.isfile(p) and not name.startswith("."):
                out.append((src["dir"], p))
    return out


def ingest_path(path: str, source_dir: str) -> dict:
    """Run one file through the pipeline; move it to processed/ when done."""
    doc = load_sources()
    src = next((s for s in doc["sources"] if s["dir"] == source_dir), None)
    if src is None:
        raise ValueError(f"unknown source dir {source_dir!r}")
    adapter = ADAPTERS[src["adapter"]]
    filename = os.path.basename(path)

    with INGEST_LOCK:
        conn = get_conn(config.FINANCE_DB)
        try:
            report = ingest_file(conn, path, adapter)
        except Exception as e:
            conn.rollback()
            _write_error_load(conn, filename, _sha256(path), adapter.name, e)
            report = {
                "file": filename, "adapter": adapter.name, "status": "error",
                "issues": [f"ERROR: {e}"], "rows_total": 0, "rows_new": 0,
                "rows_updated": 0, "date_min": None, "date_max": None,
                "spend_cents": None, "income_cents": None,
            }
        finally:
            conn.close()

    # phase 2: categorize new data (best-effort — failures must not fail the
    # ingest; they are appended to the load_log issues instead)
    if report["status"] in ("ok", "partial"):
        cat_issues = _post_ingest_categorize(report)
        if cat_issues:
            report["issues"].extend(cat_issues)
            _record_load_issues(report["load_id"], cat_issues)

    # move to processed/ (ok, partial, or error — load_log keeps the record;
    # error files get an ERR_ prefix so the watcher doesn't retry in a loop)
    try:
        doc = load_sources()
        proc = os.path.join(config.INBOX_ROOT, doc["processed_dirname"])
        os.makedirs(proc, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        prefix = "ERR_" if report["status"] == "error" else ""
        shutil.move(path, os.path.join(proc, f"{prefix}{ts}_{filename}"))
    except OSError as e:
        report["issues"] = report.get("issues", []) + [f"could not move file to processed/: {e}"]
    return report


def _post_ingest_categorize(report: dict) -> list:
    """Rules + AI suggestions after a successful load (best-effort).

    Never raises. Returns issue strings to append to the load_log row.
    """
    issues = []
    try:
        conn = get_conn(config.FINANCE_DB)
        try:
            rules = categorize.apply_rules(conn)
            report["rules_applied"] = {
                "matched": rules["matched"],
                "payees_matched": rules["payees_matched"],
                "categories_touched": rules["categories_touched"],
            }
        finally:
            conn.close()
    except Exception as e:
        log.warning("apply_rules after ingest %s failed: %s", report["file"], e)
        issues.append(f"categorization: apply_rules failed: {e}")
    try:
        conn = get_conn(config.FINANCE_DB)
        try:
            report["ai_suggestions"] = categorize.suggest_categories(conn)
        finally:
            conn.close()
    except Exception as e:
        log.warning("suggest_categories after ingest %s failed: %s", report["file"], e)
        issues.append(f"categorization: suggest_categories failed: {e}")
    return issues


def _record_load_issues(load_id: int, issues: list) -> None:
    """Append post-ingest categorization issues to the committed load_log row."""
    conn = get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor()
        cur.execute("SELECT issues FROM load_log WHERE id=%s", (load_id,))
        row = cur.fetchone()
        if not row:
            return
        existing = json.loads(row[0]) if row[0] else []
        existing.extend(issues)
        cur.execute(
            "UPDATE load_log SET issues=%s, status='partial' WHERE id=%s",
            (json.dumps(existing), load_id),
        )
        conn.commit()
    except Exception as e:
        log.warning("could not record categorization issues for load %s: %s", load_id, e)
        conn.rollback()
    finally:
        conn.close()


def scan_and_ingest() -> list:
    """Ingest all pending files in watched dirs (manual trigger + watcher)."""
    reports = []
    for source_dir, path in discover_files():
        reports.append(ingest_path(path, source_dir))
    return reports


def ingest_upload(data: bytes, filename: str, source_dir: str) -> dict:
    """API door: write upload into the source dir, then run the pipeline."""
    doc = load_sources()
    if not any(s["dir"] == source_dir for s in doc["sources"]):
        raise ValueError(f"unknown source dir {source_dir!r}")
    d = os.path.join(config.INBOX_ROOT, source_dir)
    os.makedirs(d, exist_ok=True)
    safe = "".join(c for c in filename if c.isalnum() or c in "._-") or "upload"
    dest = os.path.join(d, safe)
    n = 1
    while os.path.exists(dest):
        stem, dot, ext = safe.rpartition(".")
        dest = os.path.join(d, f"{stem}.{n}.{ext}" if dot else f"{safe}.{n}")
        n += 1
    with open(dest, "wb") as f:
        f.write(data)
    try:
        return ingest_path(dest, source_dir)
    except FileNotFoundError:
        # the watcher beat us to it (file already moved to processed/)
        return {"file": safe, "status": "ok", "note": "watcher already ingested this file"}


def watch_loop():
    """Background thread: poll watched dirs every poll_seconds."""
    doc = load_sources()
    poll = doc["poll_seconds"]
    while not _stop.is_set():
        try:
            for source_dir, path in discover_files():
                ingest_path(path, source_dir)
        except Exception:
            pass  # surfaced via load_log; keep watching
        _stop.wait(poll)

def start_watcher():
    t = threading.Thread(target=watch_loop, name="inbox-watcher", daemon=True)
    t.start()
    return t


def stop_watcher():
    _stop.set()
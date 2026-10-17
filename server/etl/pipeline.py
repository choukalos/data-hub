"""ETL pipeline: adapter output -> canonical MySQL tables -> load_log.

One file, one transaction. Steps:
  1. sha256 the file
  2. adapter.load() -> normalized rows + issues (pure, no DB)
  3. upsert accounts (name+institution unique)
  4. upsert categories (bucket rows + bucket->category rows)
  5. upsert transactions ON DUPLICATE KEY UPDATE (mutable fields only;
     imported_at preserved) — content_hash dedupe
  6. (tax) upsert tax_years keyed on year
  7. rebuild monthly_category_summary
  8. insert load_log row (counts, date range, status, issues)
"""

import hashlib
import os

from ..adapters.base import NormalizedTxn


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _upsert_accounts(cur, accounts: list[dict]) -> dict[str, int]:
    """Upsert accounts; return {name: id}."""
    ids = {}
    for a in accounts:
        cur.execute(
            """INSERT INTO accounts (name, institution, type)
               VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE institution=VALUES(institution), type=VALUES(type)""",
            (a["name"], a["institution"], a["type"]),
        )
        cur.execute("SELECT id FROM accounts WHERE name=%s AND institution=%s", (a["name"], a["institution"]))
        ids[a["name"]] = cur.fetchone()[0]
    return ids


def _upsert_categories(cur, rows: list[NormalizedTxn]) -> dict[tuple[str, str], int]:
    """Upsert category tree: bucket row (parent_id=0) + child rows.

    Returns {(parent, child): category_id}.
    """
    pairs = {(r.parent_category, r.category or r.parent_category) for r in rows}
    ids: dict[tuple[str, str], int] = {}
    parent_ids: dict[str, int] = {}
    for parent, child in sorted(pairs):
        if parent not in parent_ids:
            cur.execute(
                """INSERT INTO categories (name, parent_id, bucket)
                   VALUES (%s, 0, %s)
                   ON DUPLICATE KEY UPDATE bucket=VALUES(bucket)""",
                (parent, parent),
            )
            cur.execute("SELECT id FROM categories WHERE name=%s AND parent_id=0", (parent,))
            parent_ids[parent] = cur.fetchone()[0]
        pid = parent_ids[parent]
        cur.execute(
            """INSERT INTO categories (name, parent_id, bucket)
               VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE bucket=VALUES(bucket)""",
            (child, pid, parent),
        )
        cur.execute("SELECT id FROM categories WHERE name=%s AND parent_id=%s", (child, pid))
        ids[(parent, child)] = cur.fetchone()[0]
    return ids


def _rebuild_summary(cur) -> None:
    cur.execute("DELETE FROM monthly_category_summary")
    cur.execute(
        """INSERT INTO monthly_category_summary (year, month, category_id, amount_cents, txn_count)
           SELECT YEAR(txn_date), MONTH(txn_date), category_id, SUM(amount_cents), COUNT(*)
           FROM transactions
           WHERE category_id IS NOT NULL
           GROUP BY YEAR(txn_date), MONTH(txn_date), category_id"""
    )


def ingest_file(conn, path: str, adapter) -> dict:
    """Run one file through the pipeline. Returns the load report dict.

    Commits on success. On error, raises — the caller rolls back and writes
    the error load_log row (see ingest.ingest_path).
    """
    filename = os.path.basename(path)
    sha = _sha256(path)

    result = adapter.load(path)

    cur = conn.cursor()
    issues = list(result.issues)
    rows_new = rows_updated = 0
    date_min = date_max = None

    if adapter.name == "starone_csv":
        acct_ids = _upsert_accounts(cur, result.accounts)
        cat_ids = _upsert_categories(cur, result.rows)
        for r in result.rows:
            cid = cat_ids.get((r.parent_category, r.category or r.parent_category))
            h = _txn_hash(acct_ids[r.account_name], r)
            cur.execute(
                """INSERT INTO transactions
                   (account_id, txn_date, amount_cents, payee, description,
                    category_id, content_hash, source_file)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     description = VALUES(description),
                     category_id = VALUES(category_id),
                     source_file = VALUES(source_file)""",
                (acct_ids[r.account_name], r.txn_date, r.amount_cents, r.payee,
                 r.original_description, cid, h, filename),
            )
            rc = cur.rowcount  # 1=inserted, 2=updated, 0=unchanged
            if rc == 1:
                rows_new += 1
            elif rc == 2:
                rows_updated += 1
            if date_min is None or r.txn_date < date_min:
                date_min = r.txn_date
            if date_max is None or r.txn_date > date_max:
                date_max = r.txn_date
    elif adapter.name == "tax_xlsx":
        for r in result.rows:
            cur.execute(
                """INSERT INTO tax_years
                   (year, income_cents, income_tax_paid_cents, savings_est_cents, savings_rate, notes)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     income_cents=VALUES(income_cents),
                     income_tax_paid_cents=VALUES(income_tax_paid_cents),
                     savings_est_cents=VALUES(savings_est_cents),
                     savings_rate=VALUES(savings_rate),
                     notes=VALUES(notes)""",
                (r["year"], r["income_cents"], r["income_tax_paid_cents"],
                 r["savings_est_cents"], r["savings_rate"], r["notes"]),
            )
            rc = cur.rowcount  # 1=inserted, 2=updated
            if rc == 1:
                rows_new += 1
            elif rc == 2:
                rows_updated += 1
    else:
        raise ValueError(f"no pipeline for adapter {adapter.name!r}")

    if adapter.name == "starone_csv":
        _rebuild_summary(cur)

    status = "partial" if issues else "ok"
    cur.execute(
        """INSERT INTO load_log
           (source_file, sha256, adapter, rows_total, rows_new, rows_updated,
            rows_skipped, date_min, date_max, status, issues)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (filename, sha, adapter.name, len(result.rows), rows_new, rows_updated,
         0, date_min, date_max, status, _json(issues)),
    )
    conn.commit()

    spend = sum(r.amount_cents for r in result.rows if isinstance(r, NormalizedTxn) and r.amount_cents < 0)
    income = sum(r.amount_cents for r in result.rows if isinstance(r, NormalizedTxn) and r.amount_cents > 0)
    return {
        "file": filename,
        "sha256": sha,
        "adapter": adapter.name,
        "rows_total": len(result.rows),
        "rows_new": rows_new,
        "rows_updated": rows_updated,
        "date_min": date_min,
        "date_max": date_max,
        "spend_cents": spend if adapter.name == "starone_csv" else None,
        "income_cents": income if adapter.name == "starone_csv" else None,
        "status": status,
        "issues": issues,
    }


def _txn_hash(account_id: int, r: NormalizedTxn) -> str:
    raw = f"{account_id}|{r.txn_date}|{r.amount_cents}|{r.payee}|{r.original_description}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _write_error_load(conn, filename, sha, adapter_name, error):
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO load_log
               (source_file, sha256, adapter, rows_total, rows_new, rows_updated,
                rows_skipped, status, issues)
               VALUES (%s, %s, %s, 0, 0, 0, 0, 'error', %s)""",
            (filename, sha, adapter_name, _json([f"ERROR: {error}"])),
        )
        conn.commit()
    except Exception:
        conn.rollback()


def _json(obj) -> str:
    import json

    return json.dumps(obj)
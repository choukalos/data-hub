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
import json
import os

from .. import config
from ..adapters.base import NormalizedTxn
from ..adapters.generic_csv import parse_column_map, safe_column_name, split_entry


def hub_schema_ready(conn) -> bool:
    """v2a: True if the hub db has the datasets table (migrations applied).

    The hub db may exist without tables (provisioned, but data-hub not yet
    restarted / /api/migrate not yet called). Callers use this to degrade
    to the 'hub unavailable' path (503 / file stays pending) instead of
    failing with 1146 (table doesn't exist).
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name='datasets' LIMIT 1",
        (config.HUB_DB,),
    )
    return cur.fetchone() is not None


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
            # NOTE: category_id is only set on INSERT, never overwritten on
            # re-import — manual tags (Car Purchase, Kids Allowance, College
            # Living, ...) and rule/AI assignments must survive re-loads of
            # exports that still carry the original CSV categories. Only
            # brand-new (NULL) rows take the CSV's category.
            cur.execute(
                """INSERT INTO transactions
                   (account_id, txn_date, amount_cents, payee, description,
                    category_id, content_hash, source_file)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     description = VALUES(description),
                     category_id = IF(category_id IS NULL, VALUES(category_id), category_id),
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
    load_id = cur.lastrowid
    conn.commit()

    spend = sum(r.amount_cents for r in result.rows if isinstance(r, NormalizedTxn) and r.amount_cents < 0)
    income = sum(r.amount_cents for r in result.rows if isinstance(r, NormalizedTxn) and r.amount_cents > 0)
    return {
        "file": filename,
        "load_id": load_id,
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


# --- v2a: generic datasets (hub db) -------------------------------------------

def _qualify(target_table: str) -> str:
    """'hub.ds_x' -> `hub`.`ds_x` (backticked)."""
    if "." in target_table:
        dbpart, tblpart = target_table.split(".", 1)
        return f"`{dbpart}`.`{tblpart}`"
    return f"`{target_table}`"


def upsert_dataset_rows(conn, dataset, rows, column_map=None, *,
                        source_file="", sha256="", issues=None, skipped=0,
                        datasets_table="datasets", load_log_table="load_log",
                        db=None):
    """v2a: upsert generic dataset rows into the dataset's target table.

    Creates the target table if missing (id BIGINT PK AI, content_hash
    CHAR(64) UNIQUE, ingested_at TIMESTAMP, data columns from column_map),
    upserts rows ON DUPLICATE KEY UPDATE (all data columns mutable;
    ingested_at preserved), freezes an inferred column_map into
    <db>.datasets on first import, and writes a load_log row to
    <db>.load_log (hub.load_log in production).

    rows: list[DatasetRow] from GenericCsvAdapter.load(). column_map:
    {csv_col: "target TYPE"} — pass the adapter's final map; if omitted, it
    is taken from dataset["column_map"].

    rows_updated counts rows that matched an existing content_hash (changed
    or unchanged), so re-ingesting an identical file reports 0 new / N
    updated (per spec E2E).

    Commits on success. On error, writes an 'error' load_log row
    (best-effort) and re-raises.
    """
    db = db or config.HUB_DB
    name = str(dataset.get("name") or "dataset")
    target_table = str(dataset.get("target_table") or f"{db}.ds_{name}")
    issues = list(issues or [])

    cmap = column_map if column_map is not None else parse_column_map(dataset.get("column_map"))
    cols = [(safe_column_name(t), c)
            for t, c in (split_entry(e) for e in (cmap or {}).values())]
    tcols = [t for t, _ in cols]
    qtable = _qualify(target_table)

    cur = conn.cursor()
    rows_new = rows_updated = 0
    date_min = date_max = None
    try:
        if cols:
            col_ddl = ",\n".join(f"  `{t}` {c} NULL" for t, c in cols)
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {qtable} (
                      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                      content_hash CHAR(64) NOT NULL,
                      ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      {col_ddl},
                      UNIQUE KEY uq_ds_content (content_hash)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
            )
            ins_cols = ", ".join(f"`{t}`" for t in tcols)
            sql = (f"INSERT INTO {qtable} (content_hash, {ins_cols}) "
                   f"VALUES ({', '.join(['%s'] * (len(tcols) + 1))}) "
                   f"ON DUPLICATE KEY UPDATE "
                   + ", ".join(f"`{t}`=VALUES(`{t}`)" for t in tcols))
            for r in rows:
                cur.execute(sql, [r.content_hash] + [r.values.get(t) for t in tcols])
                rc = cur.rowcount  # 1=inserted, 2=changed, 0=matched unchanged
                if rc == 1:
                    rows_new += 1
                else:
                    rows_updated += 1

        # Freeze inferred schema on first import (column_map was NULL).
        if not dataset.get("column_map") and cmap:
            cur.execute(
                f"UPDATE `{db}`.`{datasets_table}` SET column_map=%s WHERE name=%s",
                (json.dumps(cmap), name),
            )

        date_col = next((t for t, c in cols if c == "DATE"), None)
        if date_col:
            ds = sorted(r.values.get(date_col) for r in rows
                        if r.values.get(date_col) is not None)
            if ds:
                date_min, date_max = ds[0], ds[-1]

        status = "partial" if (issues or skipped) else "ok"
        cur.execute(
            f"""INSERT INTO `{db}`.`{load_log_table}`
                 (source_file, sha256, adapter, dataset_name, rows_total, rows_new,
                  rows_updated, rows_skipped, date_min, date_max, status, issues)
                 VALUES (%s, %s, 'generic', %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (source_file, sha256, name, len(rows), rows_new, rows_updated,
             skipped, date_min, date_max, status, json.dumps(issues)),
        )
        conn.commit()
    except Exception as e:
        try:
            cur.execute(
                f"""INSERT INTO `{db}`.`{load_log_table}`
                     (source_file, sha256, adapter, dataset_name, status, issues)
                     VALUES (%s, %s, 'generic', %s, 'error', %s)""",
                (source_file, sha256, name, json.dumps([f"ERROR: {e}"])),
            )
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    return {
        "file": source_file,
        "sha256": sha256,
        "adapter": "generic",
        "dataset": name,
        "target_table": target_table,
        "rows_total": len(rows),
        "rows_new": rows_new,
        "rows_updated": rows_updated,
        "rows_skipped": skipped,
        "date_min": date_min,
        "date_max": date_max,
        "status": status,
        "issues": issues,
    }


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
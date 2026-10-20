"""v2b: manual row edits — a generic, audited write path for data-hub tables.

Complements the import path. Imports upsert rows by content_hash and record
provenance in source_file; manual edits (PATCH/POST/DELETE/bulk via the
/api/tables endpoints) are logged in the `manual_edits` audit table (one row
per affected row) and protected from being clobbered on re-import — the
pipeline consults locked_fields() when building ON DUPLICATE KEY UPDATE
clauses, so a field that was manually edited keeps its value across
re-imports of exports that still carry the original value.

Safety model:
- Per-table whitelist of editable fields (TABLES registry; hub.ds_* tables
  derive their whitelist from the dataset's frozen column_map).
- All identifiers validated against IDENT_RE and backtick-quoted.
- DELETE removes manual rows (source_file='manual', or a row created via a
  manual insert) by default; imported rows need force=True.
- Every change is logged with old->new values and an optional reason.

Conventions:
- Operations take the caller's conn (the endpoint manages it; hub connections
  come from _hub_conn() so an un-migrated hub degrades to 503). They commit
  on success, roll back on error, and never close the conn.
- table_name in manual_edits is the BARE table name (the audit table is
  per-db); row_id references the table's primary key.
"""

import datetime
import hashlib
import json
import re
import uuid
from decimal import Decimal

import mysql.connector

from . import config, db
from .adapters.generic_csv import parse_column_map, safe_column_name, split_entry

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BULK_CAP = 5000  # max rows a single bulk edit may touch

# Table name -> registry entry (finance tables; hub.ds_* resolved dynamically
# from the datasets registry in table_spec()).
#   pk:         primary key column (API row_id addresses this)
#   editable:   fields a manual edit may set (PATCH / bulk set)
#   insertable: manual inserts allowed
#   required:   fields required for manual inserts
#   provenance: column holding 'manual' on manually inserted rows (None for
#               tables without one — the audit table is then the source of
#               truth for "manual row")
#   on_edit:    post-write hook (None | 'summary')
TABLES = {
    "transactions": {
        "pk": "id",
        "editable": ("category_id", "payee", "description", "amount_cents", "txn_date"),
        "insertable": True,
        "required": ("account_id", "txn_date", "amount_cents", "payee"),
        "provenance": "source_file",
        "on_edit": "summary",
    },
    "tax_years": {
        "pk": "year",
        "editable": ("income_cents", "income_tax_paid_cents", "savings_est_cents",
                     "savings_rate", "notes"),
        "insertable": True,
        "required": ("year",),
        "provenance": None,
        "on_edit": None,
    },
}

# Columns a manual insert may never set directly (data-hub owns them).
RESERVED_INSERT = ("id", "content_hash", "ingested_at", "source_file",
                   "imported_at", "updated_at")


def _qualify(target_table: str) -> str:
    """'hub.ds_x' -> `hub`.`ds_x` (backticked). Mirrors etl.pipeline._qualify
    (imported lazily there to avoid a module import cycle)."""
    if "." in target_table:
        dbpart, tblpart = target_table.split(".", 1)
        return f"`{dbpart}`.`{tblpart}`"
    return f"`{target_table}`"


def table_spec(db_name, table, conn):
    """Resolve the manual-edit spec for db.table; None when not editable.

    finance tables come from the TABLES registry (any db with that table,
    e.g. a scratch db in tests); hub.ds_* tables are resolved from the
    datasets registry (editable = the dataset's data columns).
    """
    if table in TABLES:
        return TABLES[table]
    if db_name == config.HUB_DB:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM datasets WHERE target_table=%s", (f"{db_name}.{table}",))
        ds = cur.fetchone()
        if not ds:
            return None
        cmap = parse_column_map(ds.get("column_map"))
        editable = tuple(sorted({safe_column_name(t) for t, _ in
                                 (split_entry(v) for v in (cmap or {}).values())}))
        return {
            "pk": "id",
            "editable": editable,
            "insertable": True,
            "required": (),
            "provenance": None,
            "on_edit": None,
        }
    return None


def _scalar(val):
    return val is None or isinstance(val, (str, int, float, bool))


def _differs(old, new):
    """True when new differs from old (with Decimal/date coercion)."""
    if new is None:
        return old is not None
    if old is None:
        return True
    if isinstance(old, Decimal):
        try:
            return old != Decimal(str(new))
        except Exception:
            return True
    if isinstance(old, (datetime.date, datetime.datetime)):
        return str(old) != str(new)
    return old != new


def _check_exists(conn, table, col, val, label):
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM `{table}` WHERE `{col}`=%s LIMIT 1", (val,))
    if not cur.fetchone():
        raise ValueError(f"{label} {val!r} does not exist")


def _validate_fields(spec, table, conn, fields, *, for_insert=False):
    """Validate field names (whitelist) + values; return {col: value}.

    Raises ValueError on any problem.
    """
    if not isinstance(fields, dict) or not fields:
        raise ValueError("fields must be a non-empty object")
    allowed = set(spec["editable"])
    if for_insert:
        allowed |= set(spec["required"])
    out = {}
    for col, val in fields.items():
        if not IDENT_RE.match(col):
            raise ValueError(f"column {col!r} is not a valid identifier")
        if for_insert and col in RESERVED_INSERT:
            raise ValueError(f"column {col!r} is reserved (set by data-hub)")
        if col not in allowed:
            verb = "insert into" if for_insert else "edit on"
            raise ValueError(f"column {col!r} is not editable for {verb} {table} "
                             f"(allowed: {sorted(allowed)})")
        if not _scalar(val):
            raise ValueError(f"value for {col!r} must be a scalar (str/int/float/null)")
        out[col] = val
    if table == "transactions":
        if "amount_cents" in out and out["amount_cents"] is not None \
                and not isinstance(out["amount_cents"], int):
            raise ValueError("amount_cents must be an integer")
        if "category_id" in out and out["category_id"] is not None:
            if not isinstance(out["category_id"], int) or out["category_id"] < 1:
                raise ValueError("category_id must be a positive integer or null")
            _check_exists(conn, "categories", "id", out["category_id"], "category_id")
        if "txn_date" in out and out["txn_date"] is not None \
                and not DATE_RE.match(str(out["txn_date"])):
            raise ValueError("txn_date must be 'YYYY-MM-DD'")
        if for_insert and "account_id" in out and out["account_id"] is not None:
            if not isinstance(out["account_id"], int) or out["account_id"] < 1:
                raise ValueError("account_id must be a positive integer")
            _check_exists(conn, "accounts", "id", out["account_id"], "account_id")
    return out


def _json_default(o):
    """JSON encoder for audit payloads (row values may carry dates/decimals)."""
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"not JSON serializable: {o!r}")


def _audit(cur, table, row_id, action, changes, reason):
    cur.execute(
        """INSERT INTO manual_edits (table_name, row_id, action, changes, reason)
           VALUES (%s, %s, %s, %s, %s)""",
        (table, row_id, action, json.dumps(changes, default=_json_default), reason or ""),
    )


def _hook(conn, spec):
    """Post-write hook (runs inside the caller's transaction, pre-commit)."""
    if spec.get("on_edit") == "summary":
        from .etl.pipeline import _rebuild_summary  # lazy: import cycle
        cur = conn.cursor()
        _rebuild_summary(cur)


def _manual_hash(table: str) -> str:
    """Synthetic content_hash for a manual row (never collides with imports)."""
    return hashlib.sha256(f"manual:{table}:{uuid.uuid4().hex}".encode()).hexdigest()


def _get_row(conn, qual, pk, pk_val):
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM {_qualify(qual)} WHERE `{pk}`=%s", (pk_val,))
    return cur.fetchone()


# --- operations (conn managed by the caller; commit on success) -----------------

def patch_row(conn, db_name, table, row_id, fields, reason=""):
    """Set whitelisted fields on one row (row_id = the table's primary key).

    Audited in manual_edits; edited fields are then protected from re-imports.
    """
    qual = f"{db_name}.{table}"
    try:
        spec = table_spec(db_name, table, conn)
        if spec is None:
            raise LookupError(f"table {qual!r} is not registered for manual edits")
        cur = conn.cursor(dictionary=True)
        row = _get_row(conn, qual, spec["pk"], row_id)
        if not row:
            raise LookupError(f"row {row_id} not found in {qual}")
        vals = _validate_fields(spec, table, conn, fields)
        changes = {c: {"old": row.get(c), "new": v}
                   for c, v in vals.items() if _differs(row.get(c), v)}
        if changes:
            sets = ", ".join(f"`{c}`=%s" for c in changes)
            cur.execute(f"UPDATE {_qualify(qual)} SET {sets} WHERE `{spec['pk']}`=%s",
                        [changes[c]["new"] for c in changes] + [row_id])
            _audit(cur, table, row_id, "edit", changes, reason)
            _hook(conn, spec)
            conn.commit()
        return {"table": qual, "row_id": row_id, "changed": len(changes),
                "changes": changes,
                "row": _get_row(conn, qual, spec["pk"], row_id)}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def insert_row(conn, db_name, table, fields, reason=""):
    """Insert a manual row (source_file='manual' where the column exists)."""
    qual = f"{db_name}.{table}"
    try:
        spec = table_spec(db_name, table, conn)
        if spec is None:
            raise LookupError(f"table {qual!r} is not registered for manual edits")
        if not spec["insertable"]:
            raise ValueError(f"manual inserts are not allowed on {qual}")
        vals = _validate_fields(spec, table, conn, fields, for_insert=True)
        for req in spec["required"]:
            if vals.get(req) is None:
                raise ValueError(f"field {req!r} is required for manual inserts into {qual}")
        if table == "transactions":
            vals.setdefault("description", "")
            vals.setdefault("category_id", None)
            vals["content_hash"] = _manual_hash(table)
            vals["source_file"] = "manual"
        elif db_name == config.HUB_DB:
            vals["content_hash"] = _manual_hash(table)
        cur = conn.cursor(dictionary=True)
        cols = ", ".join(f"`{c}`" for c in vals)
        cur.execute(
            f"INSERT INTO {_qualify(qual)} ({cols}) VALUES ({', '.join(['%s'] * len(vals))})",
            list(vals.values()),
        )
        row_id = cur.lastrowid
        _audit(cur, table, row_id, "insert",
               {c: {"old": None, "new": v} for c, v in vals.items()}, reason)
        _hook(conn, spec)
        conn.commit()
        return {"table": qual, "row_id": row_id,
                "row": _get_row(conn, qual, spec["pk"], row_id)}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def delete_row(conn, db_name, table, row_id, reason="", force=False):
    """Delete one row. Default: manual rows only; force=True for imported rows.

    The full pre-delete row is captured in the audit trail.
    """
    qual = f"{db_name}.{table}"
    try:
        spec = table_spec(db_name, table, conn)
        if spec is None:
            raise LookupError(f"table {qual!r} is not registered for manual edits")
        cur = conn.cursor(dictionary=True)
        row = _get_row(conn, qual, spec["pk"], row_id)
        if not row:
            raise LookupError(f"row {row_id} not found in {qual}")
        if not force:
            is_manual = (row.get(spec["provenance"]) == "manual") \
                if spec.get("provenance") else False
            if not is_manual:
                cur.execute(
                    "SELECT 1 FROM manual_edits WHERE table_name=%s AND row_id=%s "
                    "AND action='insert' LIMIT 1", (table, row_id))
                is_manual = cur.fetchone() is not None
            if not is_manual:
                raise PermissionError(
                    f"row {row_id} of {qual} was imported (not a manual row); "
                    "retry with force=true to delete it anyway")
        changes = {c: {"old": v, "new": None}
                   for c, v in row.items() if c not in ("updated_at", "imported_at")}
        cur.execute(f"DELETE FROM {_qualify(qual)} WHERE `{spec['pk']}`=%s", (row_id,))
        _audit(cur, table, row_id, "delete", changes, reason or "manual delete")
        _hook(conn, spec)
        conn.commit()
        return {"table": qual, "row_id": row_id, "deleted": True, "row": row}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def bulk_update(conn, db_name, table, where, sets, reason):
    """Set fields on every row matching `where` (equality map). Audited per row.

    `where` may reference any live column; `sets` is restricted to the
    editable whitelist. Refuses to touch more than BULK_CAP rows.
    """
    if not reason:
        raise ValueError("reason is required for bulk edits")
    qual = f"{db_name}.{table}"
    try:
        spec = table_spec(db_name, table, conn)
        if spec is None:
            raise LookupError(f"table {qual!r} is not registered for manual edits")
        if not isinstance(where, dict) or not where:
            raise ValueError("where must be a non-empty object of equality conditions")
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SHOW COLUMNS FROM {_qualify(qual)}")
        live_cols = {r["Field"] for r in cur.fetchall()}
        for col, val in where.items():
            if not IDENT_RE.match(col) or col not in live_cols:
                raise ValueError(f"where column {col!r} does not exist on {table}")
            if not _scalar(val):
                raise ValueError(f"where value for {col!r} must be a scalar")
        vals = _validate_fields(spec, table, conn, sets)
        where_sql = " AND ".join(f"`{c}`=%s" for c in where)
        cur.execute(f"SELECT * FROM {_qualify(qual)} WHERE {where_sql}",
                    list(where.values()))
        rows = cur.fetchall()
        if len(rows) > BULK_CAP:
            raise ValueError(f"bulk edit matches {len(rows)} rows (cap {BULK_CAP}); "
                             "narrow the where clause")
        changed = 0
        for row in rows:
            changes = {c: {"old": row.get(c), "new": v}
                       for c, v in vals.items() if _differs(row.get(c), v)}
            if not changes:
                continue
            sets_sql = ", ".join(f"`{c}`=%s" for c in changes)
            cur.execute(f"UPDATE {_qualify(qual)} SET {sets_sql} WHERE `{spec['pk']}`=%s",
                        [changes[c]["new"] for c in changes] + [row[spec["pk"]]])
            _audit(cur, table, row[spec["pk"]], "bulk", changes, reason)
            changed += 1
        _hook(conn, spec)
        conn.commit()
        return {"table": qual, "matched": len(rows), "changed": changed,
                "set": dict(vals), "reason": reason}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def list_edits(conn, db_name, table, row_id=None, limit=100):
    """Audit trail of manual edits (newest first)."""
    cur = conn.cursor(dictionary=True)
    if row_id is not None:
        cur.execute("SELECT * FROM manual_edits WHERE table_name=%s AND row_id=%s "
                    "ORDER BY id DESC LIMIT %s", (table, row_id, limit))
    else:
        cur.execute("SELECT * FROM manual_edits WHERE table_name=%s "
                    "ORDER BY id DESC LIMIT %s", (table, limit))
    return {"table": f"{db_name}.{table}", "edits": cur.fetchall()}


def list_tables():
    """Registry view: finance tables + active hub datasets (editable fields)."""
    out = []
    for table, spec in TABLES.items():
        out.append({"db": config.FINANCE_DB, "table": table, "pk": spec["pk"],
                    "editable": list(spec["editable"]),
                    "insertable": spec["insertable"],
                    "required": list(spec["required"])})
    try:
        conn = db.get_conn(config.HUB_DB)
    except mysql.connector.Error:
        return {"tables": out}
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT name, target_table, column_map FROM datasets "
                    "WHERE is_active=1 ORDER BY name")
        for ds in cur.fetchall():
            cmap = parse_column_map(ds.get("column_map"))
            editable = sorted({safe_column_name(t) for t, _ in
                               (split_entry(v) for v in (cmap or {}).values())})
            out.append({"db": config.HUB_DB,
                        "table": str(ds["target_table"]).split(".", 1)[-1],
                        "pk": "id", "editable": editable,
                        "insertable": True, "required": [],
                        "dataset": ds["name"]})
    except mysql.connector.Error:
        pass
    finally:
        conn.close()
    return {"tables": out}


# --- re-import protection (consumed by etl.pipeline) ---------------------------

def locked_fields(cur, table, row_ids):
    """{row_id: set(fields)} — fields with manual edits (re-import protection).

    `table` is the bare table name (the audit table is per-db). Rows not in
    the result have no manual edits. `cur` must be a tuple cursor.
    """
    ids = [i for i in row_ids if i is not None]
    if not ids:
        return {}
    out = {}
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        ph = ", ".join(["%s"] * len(chunk))
        cur.execute(f"SELECT row_id, changes FROM manual_edits "
                    f"WHERE table_name=%s AND row_id IN ({ph})", [table] + chunk)
        for row_id, changes in cur.fetchall():
            try:
                fields = set(json.loads(changes).keys())
            except (TypeError, ValueError):
                continue
            out.setdefault(row_id, set()).update(fields)
    return out


def txn_upsert_clause(locked):
    """ON DUPLICATE KEY UPDATE clause for one transactions row.

    `locked`: set of manually edited field names — never overwritten on
    re-import. category_id keeps its fill-NULLs-only rule when unlocked.
    """
    parts = []
    if "description" not in locked:
        parts.append("description = VALUES(description)")
    if "category_id" not in locked:
        parts.append("category_id = IF(category_id IS NULL, VALUES(category_id), category_id)")
    if "source_file" not in locked:
        parts.append("source_file = VALUES(source_file)")
    return ", ".join(parts) if parts else "id = id"


TAX_MUTABLE = ("income_cents", "income_tax_paid_cents", "savings_est_cents",
               "savings_rate", "notes")


def tax_upsert_clause(locked):
    parts = [f"{c}=VALUES({c})" for c in TAX_MUTABLE if c not in locked]
    return ", ".join(parts) if parts else "year = year"


def ds_upsert_clause(tcols, locked):
    """ON DUPLICATE KEY UPDATE clause for one generic-dataset row."""
    parts = [f"`{t}`=VALUES(`{t}`)" for t in tcols if t not in locked]
    return ", ".join(parts) if parts else "id = id"
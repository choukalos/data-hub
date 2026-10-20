"""Unit + E2E tests for v2b manual row edits (manual.py + pipeline protection).

Run from the repo root (needs the data-hub image env, e.g. inside the
container or via docker run with the server deps):
    python tests/test_manual_edits.py

Unit tests are pure (no database). E2E tests use:
  - a scratch finance database (me_e2e_finance_<rand>, dropped at the end)
  - a scratch hub dataset (hub.ds_me_e2e_<rand>, dropped at the end)
skipped when MySQL is unreachable.
"""

import glob
import hashlib
import json
import os
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server import config, db, manual  # noqa: E402
from server.adapters.base import NormalizedTxn  # noqa: E402
from server.adapters.generic_csv import DatasetRow  # noqa: E402
from server.etl import pipeline  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIGRATIONS_DIR = os.path.join(REPO_ROOT, "migrations", "finance")
HUB_MIGRATIONS_DIR = os.path.join(REPO_ROOT, "migrations", "hub")


# --- unit tests (pure) ----------------------------------------------------------

def test_txn_upsert_clause_unlocked():
    c = manual.txn_upsert_clause(set())
    assert "description = VALUES(description)" in c
    assert "category_id = IF(category_id IS NULL, VALUES(category_id), category_id)" in c
    assert "source_file = VALUES(source_file)" in c


def test_txn_upsert_clause_locked_category():
    c = manual.txn_upsert_clause({"category_id"})
    assert "category_id" not in c
    assert "description = VALUES(description)" in c
    assert "source_file = VALUES(source_file)" in c


def test_txn_upsert_clause_all_locked():
    assert manual.txn_upsert_clause({"description", "category_id", "source_file"}) == "id = id"


def test_ds_upsert_clause():
    cols = ("a", "b", "c")
    assert manual.ds_upsert_clause(cols, set()) == \
        "`a`=VALUES(`a`), `b`=VALUES(`b`), `c`=VALUES(`c`)"
    assert manual.ds_upsert_clause(cols, {"b"}) == "`a`=VALUES(`a`), `c`=VALUES(`c`)"
    assert manual.ds_upsert_clause(cols, set(cols)) == "id = id"


def test_tax_upsert_clause():
    c = manual.tax_upsert_clause(set())
    assert "income_cents=VALUES(income_cents)" in c and "notes=VALUES(notes)" in c
    assert "notes" not in manual.tax_upsert_clause({"notes"})
    assert manual.tax_upsert_clause(set(manual.TAX_MUTABLE)) == "year = year"


def test_differs():
    from decimal import Decimal
    import datetime
    assert not manual._differs(Decimal("0.2500"), 0.25)
    assert manual._differs(Decimal("0.2500"), 0.26)
    assert not manual._differs(datetime.date(2024, 10, 21), "2024-10-21")
    assert manual._differs(datetime.date(2024, 10, 21), "2024-10-22")
    assert manual._differs(None, 5) and manual._differs(5, None)
    assert not manual._differs(5, 5) and manual._differs(5, 6)
    assert not manual._differs("x", "x") and manual._differs("x", "y")


def test_ident_and_scalar():
    assert manual.IDENT_RE.match("col_1")
    assert not manual.IDENT_RE.match("col;drop")
    assert not manual.IDENT_RE.match("1col")
    assert not manual.IDENT_RE.match("col`")
    assert manual._scalar(None) and manual._scalar(1) and manual._scalar(1.5)
    assert manual._scalar("s") and manual._scalar(True)
    assert not manual._scalar([1]) and not manual._scalar({"a": 1})


def test_validate_fields_whitelist():
    spec = manual.TABLES["transactions"]
    try:
        manual._validate_fields(spec, "transactions", None, {"bogus_col": 1})
        raise AssertionError("expected ValueError for non-editable column")
    except ValueError as e:
        assert "bogus_col" in str(e)
    try:
        manual._validate_fields(spec, "transactions", None, {"amount_cents": "12.50"})
        raise AssertionError("expected ValueError for non-int amount_cents")
    except ValueError as e:
        assert "integer" in str(e)
    try:
        manual._validate_fields(spec, "transactions", None, {"txn_date": "10/21/2024"})
        raise AssertionError("expected ValueError for non-ISO date")
    except ValueError as e:
        assert "YYYY-MM-DD" in str(e)
    # reserved columns rejected on insert
    try:
        manual._validate_fields(spec, "transactions", None,
                                {"account_id": 1, "txn_date": "2024-10-21",
                                 "amount_cents": -100, "payee": "p",
                                 "content_hash": "x" * 64}, for_insert=True)
        raise AssertionError("expected ValueError for reserved column")
    except ValueError as e:
        assert "reserved" in str(e)


def test_table_spec():
    assert manual.table_spec("finance", "transactions", None) is manual.TABLES["transactions"]
    assert manual.table_spec("finance", "nope", None) is None
    # registry is keyed by table name, so any db with that table resolves
    assert manual.table_spec("scratch_db", "transactions", None) is manual.TABLES["transactions"]


# --- E2E helpers -----------------------------------------------------------------

def _mysql_available():
    try:
        conn = db.get_conn(config.FINANCE_DB)
        conn.close()
        return True
    except Exception:
        return False


class _FakeStaroneResult:
    def __init__(self, rows, accounts):
        self.rows = rows
        self.accounts = accounts
        self.issues = []


class _FakeStaroneAdapter:
    name = "starone_csv"

    def __init__(self, rows, accounts):
        self._rows = rows
        self._accounts = accounts

    def load(self, path):
        return _FakeStaroneResult(self._rows, self._accounts)


def _starone_rows(tag):
    """Two NormalizedTxn rows; `tag` varies the description (re-import signal)."""
    return [
        NormalizedTxn(account_name="Checking Account", institution="Star One",
                      account_type="checking", txn_date="2024-01-05",
                      amount_cents=-1250, payee="P1",
                      original_description=f"DESC1 {tag}",
                      parent_category="Food", category="Groceries"),
        NormalizedTxn(account_name="Checking Account", institution="Star One",
                      account_type="checking", txn_date="2024-01-06",
                      amount_cents=-2500, payee="P2",
                      original_description=f"DESC2 {tag}",
                      parent_category="Auto", category="Fuel"),
    ]


def _tmpfile(tag):
    d = tempfile.mkdtemp(prefix="me2e-")
    p = os.path.join(d, f"starone-{tag}.csv")
    with open(p, "w") as f:
        f.write("fake")
    return p


# --- E2E: finance (transactions) path --------------------------------------------
# Runs against the HUB db (dh_app has no CREATE DATABASE privilege): the starone
# pipeline uses unqualified table names, so the finance tables are created in hub
# temporarily and dropped in the finally block. The finance migration versions
# land in hub.schema_migrations with the same version strings the hub migrations
# use, so no extra rows remain.

def _apply_finance_ddl(conn):
    """Apply the finance DDL to the hub db without the migration runner
    (test-only: keeps hub.schema_migrations clean)."""
    cur = conn.cursor()
    for path in sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql"))):
        with open(path) as f:
            sql = f.read()
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    conn.commit()


FINANCE_E2E_TABLES = ("transactions", "accounts", "categories", "category_rules",
                      "category_suggestions", "tax_years", "monthly_category_summary")


def _cleanup_finance_e2e(conn):
    """Drop the finance tables from the hub db + test audit/load rows.
    Idempotent: also used at test start to clear a crashed previous run."""
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in FINANCE_E2E_TABLES:
        cur.execute(f"DROP TABLE IF EXISTS `{t}`")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    cur.execute("DELETE FROM manual_edits WHERE table_name='transactions'")
    cur.execute("DELETE FROM load_log WHERE source_file IN "
                "('starone-a.csv', 'starone-b.csv')")
    conn.commit()



def test_e2e_finance():
    if not _mysql_available():
        print("SKIP e2e_finance: MySQL unreachable")
        return
    conn = db.get_conn(config.HUB_DB)
    _cleanup_finance_e2e(conn)  # clear residue from a crashed previous run
    _apply_finance_ddl(conn)  # finance tables in hub (test-only)
    try:
        # seed account + categories
        cur = conn.cursor()
        cur.execute("INSERT INTO accounts (name, institution, type) "
                    "VALUES ('Checking Account', 'Star One', 'checking')")
        cur.execute("INSERT INTO categories (name, parent_id, bucket) "
                    "VALUES ('Food', 0, 'Food')")
        cur.execute("INSERT INTO categories (name, parent_id, bucket) "
                    "VALUES ('Groceries', LAST_INSERT_ID(), 'Food')")
        cur.execute("INSERT INTO categories (name, parent_id, bucket) "
                    "VALUES ('Auto', 0, 'Auto')")
        cur.execute("INSERT INTO categories (name, parent_id, bucket) "
                    "VALUES ('Fuel', LAST_INSERT_ID(), 'Auto')")
        conn.commit()
        cur.execute("SELECT id FROM categories WHERE name='Groceries' AND parent_id<>0")
        groceries_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM categories WHERE name='Fuel' AND parent_id<>0")
        fuel_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM accounts WHERE name='Checking Account'")
        acct_id = cur.fetchone()[0]

        # 1. import a file (two rows; CSV categories applied on insert)
        rep1 = pipeline.ingest_file(conn, _tmpfile("a"),
                                    _FakeStaroneAdapter(_starone_rows("a"),
                                                        [{"name": "Checking Account",
                                                          "institution": "Star One",
                                                          "type": "checking"}]))
        assert rep1["rows_new"] == 2, rep1
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT id, description, category_id FROM transactions ORDER BY id")
        t1, t2 = cur.fetchall()
        assert t1["category_id"] == groceries_id and t2["category_id"] == fuel_id

        # 2. manual edit: lock t1's category_id + description
        p = manual.patch_row(conn, config.HUB_DB, "transactions", t1["id"],
                             {"category_id": fuel_id, "description": "MANUAL DESC"},
                             "e2e patch")
        assert p["changed"] == 2, p

        # 3. manual insert
        ins = manual.insert_row(conn, config.HUB_DB, "transactions",
                                {"account_id": acct_id, "txn_date": "2024-01-07",
                                 "amount_cents": -3000, "payee": "Manual Payee",
                                 "category_id": fuel_id}, "e2e manual")
        assert ins["row"]["source_file"] == "manual"

        # 4. bulk: lock t2's category_id
        b = manual.bulk_update(conn, config.HUB_DB, "transactions",
                               {"payee": "P2"}, {"category_id": groceries_id}, "e2e bulk")
        assert b["matched"] == 1 and b["changed"] == 1, b

        # 5. re-import the SAME rows (same content_hash) under a new file
        #    name -> locked fields survive; unlocked source_file refreshes
        rep2 = pipeline.ingest_file(conn, _tmpfile("b"),
                                    _FakeStaroneAdapter(_starone_rows("a"),
                                                        [{"name": "Checking Account",
                                                          "institution": "Star One",
                                                          "type": "checking"}]))
        assert rep2["rows_updated"] == 2, rep2
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT description, category_id, source_file "
                    "FROM transactions WHERE id IN (%s, %s) ORDER BY id",
                    (t1["id"], t2["id"]))
        t1b, t2b = cur.fetchall()
        assert t1b["description"] == "MANUAL DESC", "manual description clobbered"
        assert t1b["category_id"] == fuel_id, "manual category clobbered"
        assert t1b["source_file"] == "starone-b.csv", "unlocked field did not refresh"
        assert t2b["description"] == "DESC2 a", "row2 description changed unexpectedly"
        assert t2b["category_id"] == groceries_id, "bulk category clobbered"
        assert t2b["source_file"] == "starone-b.csv", "unlocked field did not refresh"
        cur.execute("SELECT category_id FROM transactions WHERE payee='Manual Payee'")
        assert cur.fetchone()["category_id"] == fuel_id, "manual row touched by import"

        # 6. delete: manual row ok, imported row needs force
        d = manual.delete_row(conn, config.HUB_DB, "transactions", ins["row_id"], "e2e cleanup")
        assert d["deleted"] is True
        try:
            manual.delete_row(conn, config.HUB_DB, "transactions", t1["id"], "nope")
            raise AssertionError("expected PermissionError for imported row")
        except PermissionError:
            pass

        # 7. audit trail + summary hook
        ed = manual.list_edits(conn, config.HUB_DB, "transactions", limit=50)
        actions = {e["action"] for e in ed["edits"]}
        assert {"edit", "insert", "bulk", "delete"} <= actions, actions
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM monthly_category_summary")
        assert cur.fetchone()[0] >= 1
    finally:
        _cleanup_finance_e2e(conn)
        conn.close()
    print("E2E finance OK")


# --- E2E: hub (generic dataset) path ----------------------------------------------

def _me2e_hash(slug, i):
    return hashlib.sha256(f"me2e:{slug}:{i}".encode()).hexdigest()


def test_e2e_hub():
    if not _mysql_available():
        print("SKIP e2e_hub: MySQL unreachable")
        return
    try:
        conn = db.get_conn(config.HUB_DB)
    except Exception as e:
        print(f"SKIP e2e_hub: hub db unreachable ({e})")
        return
    db.apply_migrations(HUB_MIGRATIONS_DIR, config.HUB_DB, conn)  # manual_edits
    slug = f"me_e2e_{uuid.uuid4().hex[:6]}"
    table = f"ds_{slug}"
    cmap = {"note": "note VARCHAR(512)", "qty": "qty BIGINT"}

    def dsrow():
        return {"name": slug, "target_table": f"{config.HUB_DB}.{table}",
                "column_map": json.dumps(cmap)}

    def mkrows(tag):
        return [DatasetRow({"note": f"row1 {tag}", "qty": 1}, _me2e_hash(slug, 1)),
                DatasetRow({"note": f"row2 {tag}", "qty": 2}, _me2e_hash(slug, 2)),
                DatasetRow({"note": f"row3 {tag}", "qty": 3}, _me2e_hash(slug, 3))]

    cur = conn.cursor(dictionary=True)
    cur.execute(
        "INSERT INTO datasets (name, description, source_pattern, target_table, "
        "delimiter, has_header, column_map) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (slug, "e2e scratch", f"{slug}-*.csv", f"{config.HUB_DB}.{table}", ",", 1,
         json.dumps(cmap)))
    conn.commit()
    try:
        # 1. import three rows
        rep1 = pipeline.upsert_dataset_rows(conn, dsrow(), mkrows("a"), column_map=cmap,
                                            source_file=f"{slug}-a.csv", sha256="x")
        assert rep1["rows_new"] == 3, rep1
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT id, note, qty FROM `{config.HUB_DB}`.`{table}` ORDER BY id")
        r1, r2, r3 = cur.fetchall()

        # 2. manual patch (locks qty on row1) + manual insert
        p = manual.patch_row(conn, config.HUB_DB, table, r1["id"],
                             {"qty": 999}, "e2e patch")
        assert p["changed"] == 1, p
        ins = manual.insert_row(conn, config.HUB_DB, table,
                                {"note": "manual row", "qty": 7}, "e2e insert")
        assert ins["row"]["note"] == "manual row"

        # 3. bulk (locks qty on row2)
        b = manual.bulk_update(conn, config.HUB_DB, table,
                               {"note": "row2 a"}, {"qty": 555}, "e2e bulk")
        assert b["matched"] == 1 and b["changed"] == 1, b

        # 4. re-import with changed values -> locked fields survive, unlocked refresh
        rep2 = pipeline.upsert_dataset_rows(conn, dsrow(), mkrows("b"), column_map=cmap,
                                            source_file=f"{slug}-b.csv", sha256="y")
        assert rep2["rows_updated"] == 3, rep2
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT note, qty FROM `{config.HUB_DB}`.`{table}` ORDER BY id")
        rows = {row["note"].split()[0]: row for row in cur.fetchall()}
        assert rows["row1"]["qty"] == 999, "manual qty clobbered on re-import"
        assert rows["row1"]["note"] == "row1 b", "unlocked note did not refresh"
        assert rows["row2"]["qty"] == 555, "bulk qty clobbered on re-import"
        assert rows["row3"]["qty"] == 3, "unlocked row3 qty should refresh"
        cur.execute(f"SELECT qty FROM `{config.HUB_DB}`.`{table}` WHERE note='manual row'")
        assert cur.fetchone()["qty"] == 7, "manual row touched by import"

        # 5. delete: manual row ok (audit-based), imported row needs force
        d = manual.delete_row(conn, config.HUB_DB, table, ins["row_id"], "e2e cleanup")
        assert d["deleted"] is True
        try:
            manual.delete_row(conn, config.HUB_DB, table, r1["id"], "nope")
            raise AssertionError("expected PermissionError for imported row")
        except PermissionError:
            pass
        manual.delete_row(conn, config.HUB_DB, table, r1["id"], "e2e force", force=True)

        # 6. audit trail
        ed = manual.list_edits(conn, config.HUB_DB, table, limit=50)
        actions = {e["action"] for e in ed["edits"]}
        assert {"edit", "insert", "bulk", "delete"} <= actions, actions
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{config.HUB_DB}`.`{table}`")
        cur.execute("DELETE FROM datasets WHERE name=%s", (slug,))
        cur.execute("DELETE FROM manual_edits WHERE table_name=%s", (table,))
        cur.execute("DELETE FROM load_log WHERE source_file LIKE %s", (f"{slug}-%",))
        conn.commit()
        conn.close()
    print("E2E hub OK")


# --- API-level checks (route handlers, no server needed) --------------------------

def test_api_validation():
    from fastapi import HTTPException
    import server.main as main
    # bad db
    try:
        main._check_db("bogus")
        raise AssertionError("expected HTTPException for bad db")
    except HTTPException as e:
        assert e.status_code == 400
    # bad table name
    try:
        main._check_table("ds_x;drop")
        raise AssertionError("expected HTTPException for bad table name")
    except HTTPException as e:
        assert e.status_code == 400
    # list_tables returns the finance registry (hub degrades gracefully)
    out = main.list_tables()
    names = {t["table"] for t in out["tables"]}
    assert {"transactions", "tax_years"} <= names, names
    txn = next(t for t in out["tables"] if t["table"] == "transactions")
    assert "category_id" in txn["editable"]


def main():
    tests = [(k, v) for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as e:
            failed += 1
            import traceback
            print(f"FAIL {name}: {e}")
            traceback.print_exc()
    print(f"{len(tests) - failed}/{len(tests)} tests passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
"""Unit tests for the v2a generic CSV adapter + dataset upsert pipeline.

Run from the repo root:
    python -m tests.test_generic_csv        (or: python tests/test_generic_csv.py)

Pure unit tests need no database. The upsert idempotency test uses the hub
db when reachable (MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD env,
defaulting to the production values); otherwise it falls back to scratch
tables in the finance db (dropped immediately after, so finance ends
clean); otherwise it is skipped.
"""

import hashlib
import json
import os
import sys
import tempfile
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.adapters.generic_csv import (  # noqa: E402
    GenericCsvAdapter,
    content_hash,
    infer_type,
    normalize_value,
    parse_column_map,
    slugify,
)
from server.etl import pipeline  # noqa: E402


def _tmpfile(name: str, text: str) -> str:
    d = tempfile.mkdtemp(prefix="gtest-")
    p = os.path.join(d, name)
    with open(p, "w", newline="", encoding="utf-8") as f:
        f.write(text)
    return p


def _ds(**kw) -> dict:
    base = {
        "name": "t", "source_pattern": "t-*.csv", "target_table": "hub.ds_t",
        "delimiter": ",", "has_header": 1, "column_map": None, "key_columns": None,
    }
    base.update(kw)
    return base


# --- inference -----------------------------------------------------------------

def test_infer_types():
    assert infer_type(["1", "2", "3"]) == "BIGINT"
    assert infer_type(["-5", "3"]) == "BIGINT"
    assert infer_type(["1", "2.5"]) == "DECIMAL(18,4)"
    assert infer_type(["3.14"]) == "DECIMAL(18,4)"
    assert infer_type(["1e3", "2"]) == "DECIMAL(18,4)"
    assert infer_type(["2024-01-02", "2024-12-31"]) == "DATE"
    assert infer_type(["2024/01/02", "2024.01.03"]) == "DATE"
    assert infer_type(["hello", "world"]) == "VARCHAR(512)"
    assert infer_type(["1", "2", "x"]) == "VARCHAR(512)"
    assert infer_type(["2024-01-02", "2024-13-99"]) == "VARCHAR(512)"
    assert infer_type([]) == "VARCHAR(512)"  # all-empty column -> VARCHAR, NULLs


def test_normalize_value():
    assert normalize_value("BIGINT", "42") == 42
    assert normalize_value("BIGINT", "") is None
    assert normalize_value("BIGINT", "abc") is None
    assert normalize_value("DECIMAL(18,4)", "1.50") == Decimal("1.50")
    assert normalize_value("DECIMAL(18,4)", "") is None
    assert normalize_value("DATE", "2024-01-02") == date(2024, 1, 2)
    assert normalize_value("DATE", "nope") is None
    assert normalize_value("VARCHAR(512)", " hi ") == "hi"
    assert normalize_value("VARCHAR(512)", "") is None


def test_slugify():
    assert slugify("Revenue (USD)") == "revenue__usd_"
    assert slugify("Page Views") == "page_views"
    assert slugify("a-b/c") == "a_b_c"
    assert slugify("123") == "123"


# --- content hash ---------------------------------------------------------------

def test_content_hash_with_keys():
    h = content_hash("hub.ds_x", ["a", "b"], {"a": 1, "b": "x", "c": 5}, ["a", "b", "c"])
    assert h == hashlib.sha256("hub.ds_x\x1f1\x1fx".encode()).hexdigest()


def test_content_hash_without_keys():
    h = content_hash("hub.ds_x", None,
                     {"a": 1, "b": None, "c": date(2024, 1, 2), "d": Decimal("1.50")},
                     ["a", "b", "c", "d"])
    assert h == hashlib.sha256("1\x1f\x1f2024-01-02\x1f1.5000".encode()).hexdigest()


def test_content_hash_decimal_canonical():
    # 1.50 and 1.5 normalize to the same hash representation
    v1 = {"x": Decimal("1.50")}
    v2 = {"x": Decimal("1.5")}
    assert content_hash("t", None, v1, ["x"]) == content_hash("t", None, v2, ["x"])


# --- adapter: parsing / validation ----------------------------------------------

def test_detect():
    a = GenericCsvAdapter(_ds())
    assert a.detect("inbox/t-2024-01.csv")
    # fnmatch on basename is case-sensitive (per spec)
    assert not a.detect("inbox/t-2024-01.CSV")
    assert not a.detect("inbox/other.csv")
    assert not a.detect("inbox/t-2024-01.xlsx")
    assert not GenericCsvAdapter(_ds(source_pattern=None)).detect("inbox/t-1.csv")


def test_tsv_delimiter_and_inference():
    p = _tmpfile("t-1.tsv", "id\tname\twhen\n1\talpha\t2024-01-02\n2\tbeta\t2024-01-03\n")
    a = GenericCsvAdapter(_ds(source_pattern="t-*.tsv", delimiter="\t"))
    assert a.detect(p)
    res = a.load(p)
    assert not res.issues and not res.skipped, res.issues
    assert a.column_map == {"id": "col_id BIGINT", "name": "name VARCHAR(512)", "when": "when DATE"}
    assert res.rows[0].values == {"col_id": 1, "name": "alpha", "when": date(2024, 1, 2)}
    assert res.rows[1].values == {"col_id": 2, "name": "beta", "when": date(2024, 1, 3)}


def test_header_off():
    p = _tmpfile("t-1.csv", "1,alpha,2024-01-02\n2,beta,2024-01-03\n")
    a = GenericCsvAdapter(_ds(has_header=0))
    res = a.load(p)
    assert not res.issues, res.issues
    assert a.column_map == {
        "col_1": "col_1 BIGINT", "col_2": "col_2 VARCHAR(512)", "col_3": "col_3 DATE"}
    assert res.rows[0].values == {"col_1": 1, "col_2": "alpha", "col_3": date(2024, 1, 2)}


def test_column_count_mismatch_skipped():
    p = _tmpfile("t-1.csv", "a,b,c\n1,2,3\n4,5\n7,8,9\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert len(res.rows) == 2
    assert res.skipped == 1
    assert any("line 3" in i and "column count 2 != 3" in i for i in res.issues), res.issues


def test_empty_file():
    p = _tmpfile("t-1.csv", "")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert res.rows == [] and any("empty file" in i for i in res.issues)


def test_header_only():
    p = _tmpfile("t-1.csv", "a,b,c\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert res.rows == [] and any("header only" in i for i in res.issues)


def test_empty_values_become_null():
    p = _tmpfile("t-1.csv", "a,b\n1,\n,2\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert not res.issues, res.issues
    assert a.column_map == {"a": "a BIGINT", "b": "b BIGINT"}
    assert res.rows[0].values == {"a": 1, "b": None}
    assert res.rows[1].values == {"a": None, "b": 2}


def test_frozen_column_map_used():
    cmap = json.dumps({"rev": "revenue DECIMAL(18,4)", "d": "day DATE"})
    p = _tmpfile("t-1.csv", "rev,d\n1.50,2024-01-02\n2.25,2024-01-03\n")
    a = GenericCsvAdapter(_ds(column_map=cmap))
    res = a.load(p)
    assert not res.issues, res.issues
    assert a.column_map == {"rev": "revenue DECIMAL(18,4)", "d": "day DATE"}
    assert res.rows[0].values == {"revenue": Decimal("1.50"), "day": date(2024, 1, 2)}


def test_frozen_map_key_missing_in_file():
    cmap = json.dumps({"rev": "revenue DECIMAL(18,4)", "ghost": "g VARCHAR(512)"})
    p = _tmpfile("t-1.csv", "rev\n1.50\n")
    a = GenericCsvAdapter(_ds(column_map=cmap))
    res = a.load(p)
    assert any("ghost" in i for i in res.issues)
    assert res.rows[0].values == {"revenue": Decimal("1.50")}


def test_slug_collision_deduped():
    p = _tmpfile("t-1.csv", "Revenue,revenue\n1,2\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert not res.issues, res.issues
    # "Revenue" and "revenue" both slugify to "revenue" -> second gets _2
    assert a.column_map == {"Revenue": "revenue BIGINT", "revenue": "revenue_2 BIGINT"}
    assert res.rows[0].values == {"revenue": 1, "revenue_2": 2}


def test_inference_sample_capped_at_200():
    # A float in row 250 must not flip column 1 from BIGINT to DECIMAL.
    lines = ["x"] + [str(i) for i in range(1, 260)]
    lines[250] = "1.5"
    p = _tmpfile("t-1.csv", "\n".join(lines) + "\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert a.column_map == {"x": "x BIGINT"}
    assert len(res.rows) == 259  # row 250 (bad BIGINT) kept, value NULL + issue
    assert any("bad BIGINT" in i for i in res.issues)


def test_reserved_columns_sanitized():
    p = _tmpfile("t-1.csv", "id,content_hash,ingested_at,x\n1,2,3,4\n")
    a = GenericCsvAdapter(_ds())
    res = a.load(p)
    assert not res.issues, res.issues
    assert a.column_map == {"id": "col_id BIGINT",
                            "content_hash": "col_content_hash BIGINT",
                            "ingested_at": "col_ingested_at BIGINT",
                            "x": "x BIGINT"}
    assert res.rows[0].values == {"col_id": 1, "col_content_hash": 2,
                                  "col_ingested_at": 3, "x": 4}


# --- upsert idempotency (DB-backed) -----------------------------------------------

def _mysql_conn(database):
    import mysql.connector
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "thor.local"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "dh_app"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=database,
        autocommit=False,
    )


def _upsert_kwargs(dsets, llog, db):
    return dict(source_file="t-1.csv", sha256="ab" * 32,
                datasets_table=dsets, load_log_table=llog, db=db)


def test_upsert_idempotent():
    try:
        import mysql.connector  # noqa: F401
    except ImportError:
        print("SKIP test_upsert_idempotent: mysql-connector not installed")
        return
    p = _tmpfile("t-1.csv", "id,name,amount,day\n1,alpha,1.50,2024-01-02\n2,beta,2.75,2024-01-03\n")
    ds = _ds(name="gtest_ds", target_table=None, key_columns=json.dumps(["col_id"]))
    a = GenericCsvAdapter(ds)
    res = a.load(p)
    assert len(res.rows) == 2 and not res.issues, res.issues

    hub = False
    try:
        conn = _mysql_conn("hub")
        cur = conn.cursor()
        cur.execute("SHOW TABLES LIKE 'datasets'")
        if not cur.fetchone():
            raise RuntimeError("hub.datasets missing (migrations not applied)")
        db, dsets, llog, ttable = "hub", "datasets", "load_log", "hub.ds_gtest_ds"
        hub = True
    except Exception as e:
        conn = _mysql_conn("finance")
        db, dsets, llog = "finance", "gtest_datasets", "gtest_load_log"
        ttable = "finance.gtest_ds_gtest_ds"
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE gtest_datasets (id INT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(80) NOT NULL UNIQUE, "
            "source_pattern VARCHAR(255) NOT NULL DEFAULT '', "
            "target_table VARCHAR(191) NOT NULL DEFAULT '', "
            "column_map JSON NULL, key_columns JSON NULL)"
        )
        cur.execute(
            """CREATE TABLE gtest_load_log (
                 id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                 ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 source_file VARCHAR(255) NOT NULL,
                 sha256 CHAR(64) NOT NULL DEFAULT '',
                 adapter VARCHAR(40) NOT NULL,
                 dataset_name VARCHAR(80) NOT NULL DEFAULT '',
                 rows_total INT NOT NULL DEFAULT 0,
                 rows_new INT NOT NULL DEFAULT 0,
                 rows_updated INT NOT NULL DEFAULT 0,
                 rows_skipped INT NOT NULL DEFAULT 0,
                 date_min DATE NULL,
                 date_max DATE NULL,
                 status ENUM('ok','partial','error') NOT NULL DEFAULT 'ok',
                 issues JSON NULL)"""
        )
        conn.commit()
        print(f"  (hub unavailable: {e.__class__.__name__} — using finance scratch tables)")

    try:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO `{db}`.`{dsets}` (name, source_pattern, target_table, key_columns) "
            "VALUES (%s, %s, %s, %s)",
            ("gtest_ds", "t-*.csv", ttable, json.dumps(["col_id"])),
        )
        conn.commit()
        ds_row = dict(ds, target_table=ttable)

        r1 = pipeline.upsert_dataset_rows(
            conn, ds_row, res.rows, a.column_map,
            issues=res.issues, skipped=res.skipped, **_upsert_kwargs(dsets, llog, db))
        assert r1["rows_new"] == 2 and r1["rows_updated"] == 0, r1
        assert r1["status"] == "ok" and r1["date_min"] == date(2024, 1, 2) \
            and r1["date_max"] == date(2024, 1, 3), r1

        # Same rows again: 0 new / 2 updated (per spec E2E)
        r2 = pipeline.upsert_dataset_rows(
            conn, ds_row, res.rows, a.column_map, **_upsert_kwargs(dsets, llog, db))
        assert r2["rows_new"] == 0 and r2["rows_updated"] == 2, r2

        # Changed non-key value: still 0 new, matched rows counted updated
        res.rows[0].values["name"] = "alpha2"
        r3 = pipeline.upsert_dataset_rows(
            conn, ds_row, res.rows, a.column_map, **_upsert_kwargs(dsets, llog, db))
        assert r3["rows_new"] == 0 and r3["rows_updated"] == 2, r3
        cur.execute(f"SELECT `name` FROM {ttable} WHERE content_hash=%s",
                    (res.rows[0].content_hash,))
        assert cur.fetchone()[0] == "alpha2"
        cur.execute(f"SELECT COUNT(*) FROM {ttable}")
        assert cur.fetchone()[0] == 2

        # Inferred schema frozen into datasets.column_map on first import
        # (id column sanitized to col_id — reserved PK name)
        cur.execute(f"SELECT column_map FROM `{db}`.`{dsets}` WHERE name=%s", ("gtest_ds",))
        frozen = cur.fetchone()[0]
        assert frozen and json.loads(frozen) == a.column_map, frozen
        assert a.column_map == {"id": "col_id BIGINT", "name": "name VARCHAR(512)",
                                "amount": "amount DECIMAL(18,4)", "day": "day DATE"}

        # load_log rows written (adapter='generic', dataset_name set)
        cur.execute(
            f"SELECT rows_total, rows_new, rows_updated, status, adapter "
            f"FROM `{db}`.`{llog}` WHERE dataset_name=%s ORDER BY id",
            ("gtest_ds",))
        logs = cur.fetchall()
        assert len(logs) == 3, logs
        assert all(row[4] == "generic" and row[3] == "ok" for row in logs), logs
        assert logs[0][:3] == (2, 2, 0) and logs[1][:3] == (2, 0, 2), logs
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {ttable}")
        cur.execute(f"DELETE FROM `{db}`.`{dsets}` WHERE name=%s", ("gtest_ds",))
        cur.execute(f"DELETE FROM `{db}`.`{llog}` WHERE dataset_name=%s", ("gtest_ds",))
        if not hub:
            cur.execute("DROP TABLE IF EXISTS gtest_datasets")
            cur.execute("DROP TABLE IF EXISTS gtest_load_log")
        conn.commit()
        conn.close()


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
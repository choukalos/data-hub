"""Unit tests for the v2a hub-db readiness guard (hub db exists but
migrations not yet applied -> graceful 503 instead of 500).

Run from the repo root:
    python -m tests.test_hub_schema        (or: python tests/test_hub_schema.py)

The pipeline.hub_schema_ready tests are pure (fake conn, no DB, no
third-party deps). The server.main._hub_conn tests need fastapi +
mysql-connector (available in the data-hub container image); they are
skipped when those are not importable.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.etl import pipeline  # noqa: E402


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row
        self.closed = False

    def cursor(self, *a, **k):
        return _FakeCursor(self._row)

    def close(self):
        self.closed = True


def test_hub_schema_ready_true():
    assert pipeline.hub_schema_ready(_FakeConn((1,))) is True


def test_hub_schema_ready_false():
    assert pipeline.hub_schema_ready(_FakeConn(None)) is False


def _main_or_skip():
    """Import server.main + mysql.connector; return (main, mc) or skip."""
    try:
        import mysql.connector  # noqa: F401
        from fastapi import HTTPException  # noqa: F401
        import server.main as main
    except ImportError as e:
        print(f"SKIP: {e} (run inside the data-hub container)")
        return None
    return main, mysql.connector


def test_hub_conn_503_when_unmigrated():
    m = _main_or_skip()
    if m is None:
        return
    main, mc = m
    from fastapi import HTTPException
    fake = _FakeConn(None)  # db reachable, datasets table missing
    orig = main.db.get_conn
    main.db.get_conn = lambda db: fake
    try:
        try:
            main._hub_conn()
            raise AssertionError("expected HTTPException 503")
        except HTTPException as e:
            assert e.status_code == 503, e.status_code
            assert "not migrated" in e.detail, e.detail
        assert fake.closed, "conn must be closed on the 503 path"
    finally:
        main.db.get_conn = orig


def test_hub_conn_returns_conn_when_ready():
    m = _main_or_skip()
    if m is None:
        return
    main, mc = m
    fake = _FakeConn((1,))  # datasets table present
    orig = main.db.get_conn
    main.db.get_conn = lambda db: fake
    try:
        conn = main._hub_conn()
        assert conn is fake
        assert not fake.closed
    finally:
        main.db.get_conn = orig


def test_hub_conn_503_when_unreachable():
    m = _main_or_skip()
    if m is None:
        return
    main, mc = m
    from fastapi import HTTPException

    def _boom(db):
        raise mc.Error("1044 access denied")

    orig = main.db.get_conn
    main.db.get_conn = _boom
    try:
        try:
            main._hub_conn()
            raise AssertionError("expected HTTPException 503")
        except HTTPException as e:
            assert e.status_code == 503, e.status_code
            assert "unavailable" in e.detail, e.detail
    finally:
        main.db.get_conn = orig


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
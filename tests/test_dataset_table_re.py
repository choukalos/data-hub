"""Regression tests: _QUALIFIED_TABLE_RE must accept hyphenated table names.

Dataset slugs allow '-' (_DATASET_NAME_RE), so the default target table for
a dataset like 'mcp-e2e' is 'hub.ds_mcp-e2e'. The old regex rejected
hyphens, which made GET /api/datasets report row_count=0 for every
hyphen-named dataset and GET /api/datasets/{name}/preview return 500
(caught by the 2026-10-18 v2b production E2E).

Run from the repo root:
    python tests/test_dataset_table_re.py
The server.main import needs fastapi + mysql-connector (available in the
data-hub container image); skipped when not importable.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _main_or_skip():
    try:
        import server.main as main
    except ImportError as e:
        print(f"SKIP: {e} (run inside the data-hub container)")
        return None
    return main


def test_qualified_table_regex_hyphens():
    m = _main_or_skip()
    if m is None:
        return
    main = m
    # hyphenated target tables (default hub.ds_<slug>) must match
    assert main._QUALIFIED_TABLE_RE.match("hub.ds_mcp-e2e")
    assert main._QUALIFIED_TABLE_RE.match("hub.ds_workapp-2026")
    # plain identifiers still match
    assert main._QUALIFIED_TABLE_RE.match("hub.datasets")
    # no dot / extra dots / backticks must be rejected
    assert not main._QUALIFIED_TABLE_RE.match("ds_mcp-e2e")
    assert not main._QUALIFIED_TABLE_RE.match("hub.ds.a.b")
    assert not main._QUALIFIED_TABLE_RE.match("hub.`ds`")
    assert not main._QUALIFIED_TABLE_RE.match("`hub`.ds")
    # _qualify backticks both parts (what makes interpolation safe)
    assert main._qualify("hub.ds_mcp-e2e") == "`hub`.`ds_mcp-e2e`"


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
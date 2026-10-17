"""Phase 2 categorization: rules engine, AI suggestions (LiteLLM), review flow.

- apply_rules(): match the payee of NULL-category transactions against
  category_rules with exact -> contains -> prefix -> regex precedence,
  first match wins. Bad regexes are skipped + logged.
- suggest_categories(): for distinct payees with uncategorized txns and no
  pending suggestion, batch 25-50/call and ask LiteLLM for the best EXISTING
  category (strict JSON, parsed defensively). Inserts pending rows into
  category_suggestions, deduped to one pending suggestion per payee.
- review_suggestion(): approve -> create exact payee rule + backfill NULL
  txns + rebuild monthly_category_summary; reject -> mark rejected;
  edit -> change suggested category then approve.
- category_tree() / pending_suggestions(): read models for the API.

All DB helpers take an optional conn (caller-managed) and a commit flag so
they can run inside an existing transaction.
"""

import json
import logging
import re
import urllib.error
import urllib.request

from . import config
from .db import get_conn
from .etl.pipeline import _rebuild_summary

log = logging.getLogger("data-hub.categorize")

RULE_PRECEDENCE = ("exact", "contains", "prefix", "regex")
SUGGEST_BATCH_SIZE = 40  # 25-50 payees per LLM call
LLM_TIMEOUT_SECONDS = 120


# --- rules engine -------------------------------------------------------------

def _match_payee(payee: str, by_type: dict, compiled: dict):
    """Return category_id for the first matching rule (precedence order)."""
    for t in RULE_PRECEDENCE:
        for r in by_type[t]:
            p = r["pattern"]
            if not p:
                continue  # empty pattern would match everything
            if t == "exact":
                hit = p == payee
            elif t == "contains":
                hit = p in payee
            elif t == "prefix":
                hit = payee.startswith(p)
            else:  # regex
                rx = compiled.get(r["id"])
                hit = bool(rx and rx.search(payee))
            if hit:
                return r["category_id"]
    return None


def apply_rules(conn=None, commit=True) -> dict:
    """Categorize NULL-category transactions by payee rules.

    Matches each distinct uncategorized payee against category_rules with
    exact -> contains -> prefix -> regex precedence, first match wins.
    Returns a report: {matched, payees_matched, assignments,
    categories_touched, summary_rebuilt, ...}.
    """
    own = conn is None
    if own:
        conn = get_conn(config.FINANCE_DB)
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, pattern, match_type, category_id FROM category_rules ORDER BY id"
        )
        rules = cur.fetchall()
        by_type = {t: [] for t in RULE_PRECEDENCE}
        compiled = {}
        skipped = 0
        for r in rules:
            if r["match_type"] == "regex":
                try:
                    compiled[r["id"]] = re.compile(r["pattern"])
                except re.error as e:
                    skipped += 1
                    log.warning(
                        "skipping invalid regex rule id=%s pattern=%r: %s",
                        r["id"], r["pattern"], e,
                    )
                    continue
            by_type[r["match_type"]].append(r)

        cur.execute(
            """SELECT DISTINCT payee FROM transactions
               WHERE category_id IS NULL AND payee <> ''
               ORDER BY payee"""
        )
        payees = [row["payee"] for row in cur.fetchall()]

        assignments = {}
        for payee in payees:
            cat_id = _match_payee(payee, by_type, compiled)
            if cat_id is not None:
                assignments[payee] = cat_id

        matched = 0
        touched = {}
        for payee, cat_id in assignments.items():
            cur.execute(
                """UPDATE transactions SET category_id = %s
                   WHERE payee = %s AND category_id IS NULL""",
                (cat_id, payee),
            )
            matched += cur.rowcount
            touched[cat_id] = touched.get(cat_id, 0) + cur.rowcount

        summary_rebuilt = False
        if assignments:
            _rebuild_summary(cur)
            summary_rebuilt = True
        if commit:
            conn.commit()
        return {
            "rules_loaded": len(rules),
            "rules_skipped_bad_regex": skipped,
            "payees_scanned": len(payees),
            "payees_matched": len(assignments),
            "matched": matched,
            "assignments": assignments,
            "categories_touched": sorted(touched),
            "summary_rebuilt": summary_rebuilt,
        }
    finally:
        if own:
            conn.close()


# --- AI suggestions (LiteLLM) -------------------------------------------------

def _format_tree(cats: list[dict]) -> str:
    """Compact 'id: name' tree, sub-categories indented under their bucket."""
    by_parent: dict[int, list[dict]] = {}
    for c in cats:
        by_parent.setdefault(c["parent_id"], []).append(c)
    lines = []
    for c in by_parent.get(0, []):
        lines.append(f"{c['id']}: {c['name']}")
        for child in by_parent.get(c["id"], []):
            lines.append(f"  {child['id']}: {child['name']}")
    return "\n".join(lines)


def _build_messages(tree_text: str, payees: list[str]) -> list[dict]:
    numbered = "\n".join(f"{i}. {p}" for i, p in enumerate(payees, 1))
    system = (
        "You are a personal-finance bookkeeping assistant. You are given a "
        "list of bank transaction payees and the user's existing category "
        "tree. For each payee, choose the single best EXISTING category from "
        "the tree. Prefer leaf (sub)categories over top-level buckets. "
        "Reply with ONLY a JSON array - no markdown, no code fences, no "
        'commentary. Each element is an object: {"payee": <exact payee '
        'string from the input list>, "category_id": <integer id from the '
        'tree>, "confidence": <number between 0.0 and 1.0>, "rationale": '
        "<one short line explaining the choice>}."
    )
    user = (
        "Category tree (top-level buckets first, sub-categories indented, "
        f"format 'id: name'):\n{tree_text}\n\n"
        "Payees to categorize:\n" + numbered + "\n\nReturn the JSON array now."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _llm_chat(messages: list[dict], model: str | None = None) -> str:
    """One OpenAI-compatible chat completion against the LiteLLM proxy."""
    url = config.LITELLM_API_BASE.rstrip("/") + "/v1/chat/completions"
    body = json.dumps({
        "model": model or config.LITELLM_MODEL,
        "messages": messages,
        "temperature": 0,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.LITELLM_API_KEY}",
    })
    with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_SECONDS) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["choices"][0]["message"]["content"]


def _parse_llm_suggestions(text: str, valid_ids: set, payees: list[str]) -> list[dict]:
    """Defensively parse the LLM reply into suggestion dicts.

    Strips code fences, extracts the outermost JSON array, tolerates a
    wrapping object, and validates each entry (known payee, known category
    id, clamped confidence). Per-payee fallback: bad/missing entries are
    skipped, never fatal.
    """
    if not text:
        return []
    t = text.strip()
    m = re.match(r"^```[a-zA-Z0-9]*\s*(.*?)\s*```$", t, re.DOTALL)
    if m:
        t = m.group(1).strip()
    start, end = t.find("["), t.rfind("]")
    if start == -1 or end <= start:
        return []
    try:
        data = json.loads(t[start:end + 1])
    except (json.JSONDecodeError, ValueError):
        return []
    if isinstance(data, dict):
        for key in ("suggestions", "results", "data", "payees"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        return []
    by_payee = {p.strip().lower(): p for p in payees}
    out = []
    seen = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        raw_payee = item.get("payee")
        if not isinstance(raw_payee, str):
            continue
        key = raw_payee.strip().lower()
        if key not in by_payee or key in seen:
            continue
        cid = item.get("category_id", item.get("id"))
        try:
            cid = int(cid)
        except (TypeError, ValueError):
            continue
        if cid not in valid_ids:
            continue
        try:
            conf = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            conf = 0.0
        conf = min(1.0, max(0.0, conf))
        rationale = str(item.get("rationale", "") or "").strip()[:255]
        seen.add(key)
        out.append({
            "payee": by_payee[key],
            "category_id": cid,
            "confidence": conf,
            "rationale": rationale,
        })
    return out


def suggest_categories(conn=None, commit=True, batch_size: int = SUGGEST_BATCH_SIZE) -> dict:
    """Ask LiteLLM to categorize payees with uncategorized txns.

    Only payees with >=1 NULL-category txn and no pending suggestion are
    considered. Batches of `batch_size` payees per LLM call; strict JSON
    output, parsed defensively. Inserts pending rows into
    category_suggestions (one pending suggestion per payee). Returns a
    report including the LLM call log (calls, model, failures).
    """
    own = conn is None
    if own:
        conn = get_conn(config.FINANCE_DB)
    report = {
        "payees_found": 0,
        "batches": 0,
        "suggestions_inserted": 0,
        "suggestions": [],
        "llm": {
            "calls": 0,
            "failures": 0,
            "model": config.LITELLM_MODEL,
            "errors": [],
        },
    }
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """SELECT t.payee AS payee, COUNT(*) AS uncategorized_txns
               FROM transactions t
               WHERE t.category_id IS NULL AND t.payee <> ''
                 AND NOT EXISTS (
                   SELECT 1 FROM category_suggestions s
                   WHERE s.payee = t.payee AND s.status = 'pending')
               GROUP BY t.payee
               ORDER BY t.payee"""
        )
        payee_rows = cur.fetchall()
        report["payees_found"] = len(payee_rows)
        if not payee_rows:
            return report  # nothing to do; no LLM calls

        cur.execute(
            "SELECT id, name, parent_id FROM categories ORDER BY parent_id, sort_order, name"
        )
        cats = cur.fetchall()
        valid_ids = {c["id"] for c in cats}
        tree_text = _format_tree(cats)

        payees = [r["payee"] for r in payee_rows]
        for i in range(0, len(payees), batch_size):
            batch = payees[i:i + batch_size]
            report["batches"] += 1
            try:
                text = _llm_chat(_build_messages(tree_text, batch))
                report["llm"]["calls"] += 1
            except Exception as e:
                report["llm"]["failures"] += 1
                report["llm"]["errors"].append(f"batch {report['batches']}: {e}")
                log.warning("LLM suggestion batch %d failed: %s", report["batches"], e)
                continue
            for s in _parse_llm_suggestions(text, valid_ids, batch):
                # INSERT ... WHERE NOT EXISTS keeps one pending suggestion
                # per payee even if one appeared since the query above.
                cur.execute(
                    """INSERT INTO category_suggestions
                       (payee, suggested_category_id, confidence, rationale, status)
                       SELECT %s, %s, %s, %s, 'pending'
                       WHERE NOT EXISTS (
                         SELECT 1 FROM category_suggestions
                         WHERE payee = %s AND status = 'pending')""",
                    (s["payee"], s["category_id"], s["confidence"],
                     s["rationale"], s["payee"]),
                )
                if cur.rowcount:
                    report["suggestions_inserted"] += 1
                    report["suggestions"].append({
                        "payee": s["payee"],
                        "category_id": s["category_id"],
                        "confidence": s["confidence"],
                        "rationale": s["rationale"],
                    })
        if commit:
            conn.commit()
        return report
    finally:
        if own:
            conn.close()


# --- review flow ---------------------------------------------------------------

def review_suggestion(conn, suggestion_id: int, action: str,
                      category_id: int | None = None, commit=True) -> dict:
    """Review one pending suggestion.

    approve -> status='approved', create exact payee rule (source='ai'),
    backfill NULL-category txns for the payee, rebuild the summary.
    reject  -> status='rejected'.
    edit    -> update suggested_category_id, then behave like approve.
    """
    if action not in ("approve", "reject", "edit"):
        raise ValueError("action must be one of: approve, reject, edit")
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM category_suggestions WHERE id = %s", (suggestion_id,))
    s = cur.fetchone()
    if not s:
        raise LookupError(f"suggestion {suggestion_id} not found")
    if s["status"] != "pending":
        raise ValueError(f"suggestion {suggestion_id} is already {s['status']}")

    if action == "reject":
        cur.execute(
            "UPDATE category_suggestions SET status='rejected', reviewed_at=NOW() WHERE id=%s",
            (suggestion_id,),
        )
        if commit:
            conn.commit()
        return {
            "suggestion_id": suggestion_id, "action": "reject", "status": "rejected",
            "payee": s["payee"], "rule_id": None, "rows_backfilled": 0,
            "summary_rebuilt": False,
        }

    cat_id = s["suggested_category_id"]
    if action == "edit":
        if category_id is None:
            raise ValueError("action 'edit' requires category_id")
        cur.execute("SELECT id FROM categories WHERE id = %s", (category_id,))
        if not cur.fetchone():
            raise ValueError(f"category {category_id} does not exist")
        cur.execute(
            "UPDATE category_suggestions SET suggested_category_id=%s WHERE id=%s",
            (category_id, suggestion_id),
        )
        cat_id = category_id

    cur.execute(
        "UPDATE category_suggestions SET status='approved', reviewed_at=NOW() WHERE id=%s",
        (suggestion_id,),
    )
    # Exact payee rule (one per payee: drop conflicting exact rules first so
    # the rules engine stays unambiguous).
    cur.execute(
        "DELETE FROM category_rules WHERE pattern=%s AND match_type='exact' AND category_id <> %s",
        (s["payee"], cat_id),
    )
    cur.execute(
        """INSERT INTO category_rules (pattern, match_type, category_id, source)
           VALUES (%s, 'exact', %s, 'ai')
           ON DUPLICATE KEY UPDATE category_id = VALUES(category_id), source = 'ai'""",
        (s["payee"], cat_id),
    )
    cur.execute(
        "SELECT id FROM category_rules WHERE pattern=%s AND match_type='exact' AND category_id=%s",
        (s["payee"], cat_id),
    )
    rule_id = cur.fetchone()["id"]
    cur.execute(
        "UPDATE transactions SET category_id=%s WHERE payee=%s AND category_id IS NULL",
        (cat_id, s["payee"]),
    )
    rows_backfilled = cur.rowcount
    _rebuild_summary(cur)
    cur.execute("SELECT name FROM categories WHERE id=%s", (cat_id,))
    cat_name = cur.fetchone()["name"]
    if commit:
        conn.commit()
    return {
        "suggestion_id": suggestion_id, "action": action, "status": "approved",
        "payee": s["payee"], "category_id": cat_id, "category_name": cat_name,
        "rule_id": rule_id, "rows_backfilled": rows_backfilled,
        "summary_rebuilt": True,
    }


# --- read models ---------------------------------------------------------------

def category_tree(conn) -> dict:
    """Nested category tree with per-node txn counts + top-level totals."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT id, name, parent_id, bucket, sort_order FROM categories "
        "ORDER BY parent_id, sort_order, name"
    )
    cats = cur.fetchall()
    cur.execute(
        """SELECT category_id, COUNT(*) AS n FROM transactions
           WHERE category_id IS NOT NULL GROUP BY category_id"""
    )
    counts = {r["category_id"]: r["n"] for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) AS n FROM transactions WHERE category_id IS NULL")
    uncategorized = cur.fetchone()["n"]
    cur.execute("SELECT COUNT(*) AS n FROM transactions")
    total = cur.fetchone()["n"]

    nodes = {}
    for c in cats:
        nodes[c["id"]] = {
            "id": c["id"], "name": c["name"], "bucket": c["bucket"],
            "parent_id": c["parent_id"], "txn_count": counts.get(c["id"], 0),
            "children": [],
        }
    roots = []
    for c in cats:
        node = nodes[c["id"]]
        parent = nodes.get(c["parent_id"]) if c["parent_id"] != 0 else None
        if parent is not None:
            parent["children"].append(node)
        else:
            roots.append(node)
    return {
        "buckets": roots,
        "totals": {
            "categories": len(cats),
            "buckets": len(roots),
            "categorized_txns": total - uncategorized,
            "uncategorized_txns": uncategorized,
            "all_txns": total,
        },
    }


def pending_suggestions(conn) -> dict:
    """Pending AI suggestions with category name/bucket + affected txn count."""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """SELECT s.id, s.payee, s.suggested_category_id, s.confidence,
                  s.rationale, s.created_at,
                  c.name AS category_name, c.bucket,
                  (SELECT COUNT(*) FROM transactions t
                    WHERE t.payee = s.payee AND t.category_id IS NULL)
                    AS uncategorized_txn_count
           FROM category_suggestions s
           JOIN categories c ON c.id = s.suggested_category_id
           WHERE s.status = 'pending'
           ORDER BY s.created_at ASC, s.id ASC"""
    )
    rows = cur.fetchall()
    for r in rows:
        r["confidence"] = float(r["confidence"])
    return {"pending": rows, "count": len(rows)}
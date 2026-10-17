"""Starone (Star One) enriched transaction CSV adapter.

Format (11 columns, header row):
  Date,Description,Original Description,Amount,Type,Parent Category,
  Category,Account,Tags,Memo,Pending

Conventions (verified against 6 exports, 2022-2026):
- Date: M/D/YY (2-digit year, all 20xx in current data)
- Amount: always positive; sign from Type (Debit = outflow/negative,
  Credit = inflow/positive)
- Parent Category = bucket (Food & Dining, Transfer, Uncategorized, ...)
- Category = subcategory (Fast Food, Doctor, ...)
- Account: "Visa Signature" (credit), "Checking Account", "Money Market Savings"
- No transaction id -> dedupe via content_hash (see base.NormalizedTxn)
- Inter-account transfers appear twice (Debit on source + Credit on dest)
  under Parent Category "Transfer"; reports exclude that bucket from spend.
"""

import csv
import os

from .base import Adapter, LoadResult, NormalizedTxn

INSTITUTION = "Star One"

ACCOUNT_TYPES = {
    "Visa Signature": "credit",
    "Checking Account": "checking",
    "Money Market Savings": "savings",
}

REQUIRED_COLS = [
    "Date", "Description", "Original Description", "Amount", "Type",
    "Parent Category", "Category", "Account", "Pending",
]


def _parse_date(raw: str) -> str | None:
    """M/D/YY -> YYYY-MM-DD (2-digit year: <70 => 20xx, else 19xx)."""
    parts = raw.strip().split("/")
    if len(parts) != 3:
        return None
    try:
        m, d, yy = (int(p) for p in parts)
    except ValueError:
        return None
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return None
    year = 2000 + yy if yy < 70 else 1900 + yy
    return f"{year:04d}-{m:02d}-{d:02d}"


def _parse_cents(raw: str) -> int | None:
    try:
        return round(float(raw.strip().replace(",", "")) * 100)
    except ValueError:
        return None


class StaroneCsvAdapter(Adapter):
    name = "starone_csv"

    def detect(self, path: str) -> bool:
        if not path.lower().endswith(".csv"):
            return False
        try:
            with open(path, newline="") as f:
                header = next(csv.reader(f))
        except (OSError, StopIteration):
            return False
        return "Date" in header and "Parent Category" in header

    def load(self, path: str) -> LoadResult:
        result = LoadResult()
        with open(path, newline="") as f:
            rdr = csv.DictReader(f)
            if not rdr.fieldnames or any(c not in rdr.fieldnames for c in REQUIRED_COLS):
                raise ValueError(
                    f"{os.path.basename(path)}: not a Starone enriched CSV "
                    f"(missing expected columns); got {rdr.fieldnames}"
                )
            accounts = {}
            for i, r in enumerate(rdr, start=2):  # line numbers (1-based, header=1)
                date = _parse_date(r.get("Date") or "")
                if date is None:
                    result.issues.append(f"line {i}: bad date {r.get('Date')!r} — row skipped")
                    continue
                cents = _parse_cents(r.get("Amount") or "")
                if cents is None:
                    result.issues.append(f"line {i}: bad amount {r.get('Amount')!r} — row skipped")
                    continue
                ttype = (r.get("Type") or "").strip().lower()
                if ttype not in ("credit", "debit"):
                    result.issues.append(f"line {i}: unknown Type {r.get('Type')!r} — row skipped")
                    continue
                signed = -cents if ttype == "debit" else cents
                acct = (r.get("Account") or "").strip() or "Unknown"
                if acct not in accounts:
                    accounts[acct] = {
                        "name": acct,
                        "institution": INSTITUTION,
                        "type": ACCOUNT_TYPES.get(acct, "other"),
                    }
                payee = (r.get("Description") or "").strip()
                if not payee:
                    result.issues.append(f"line {i}: empty payee — kept as (no payee)")
                result.rows.append(
                    NormalizedTxn(
                        account_name=acct,
                        institution=INSTITUTION,
                        account_type=ACCOUNT_TYPES.get(acct, "other"),
                        txn_date=date,
                        amount_cents=signed,
                        payee=payee,
                        original_description=(r.get("Original Description") or "").strip(),
                        parent_category=(r.get("Parent Category") or "Uncategorized").strip() or "Uncategorized",
                        category=(r.get("Category") or "").strip(),
                        pending=(r.get("Pending") or "").strip().lower() == "true",
                    )
                )
        if any(t.pending for t in result.rows):
            n = sum(1 for t in result.rows if t.pending)
            result.issues.append(f"{n} pending (unsettled) rows included — may change on re-export")
        result.accounts = list(accounts.values())
        return result
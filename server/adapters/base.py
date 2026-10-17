"""Adapter base classes."""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class NormalizedTxn:
    """Canonical transaction row (bank sources)."""

    account_name: str
    institution: str
    account_type: str  # checking | savings | credit | other
    txn_date: str  # YYYY-MM-DD
    amount_cents: int  # signed: negative = outflow, positive = inflow
    payee: str = ""  # short description (Starone "Description" column)
    original_description: str = ""  # full bank statement line
    parent_category: str = ""  # bucket (Starone "Parent Category")
    category: str = ""  # subcategory (Starone "Category")
    pending: bool = False

    def content_hash(self, account_id: int) -> str:
        """Dedupe key: sha256(account_id|date|amount_cents|payee|original_description).

        Same transaction re-exported (even from overlapping files) = same hash.
        Verified 2026-10-17: all 1,962 cross-file duplicates in the Starone
        exports are byte-identical on these fields.
        """
        raw = f"{account_id}|{self.txn_date}|{self.amount_cents}|{self.payee}|{self.original_description}"
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class LoadResult:
    """Output of Adapter.load(): normalized rows + issues found while reading.

    rows: list[NormalizedTxn] for transaction adapters; list[dict] for
    non-transaction adapters (e.g. tax_xlsx).
    """

    rows: list = field(default_factory=list)
    issues: list = field(default_factory=list)  # human-readable strings
    accounts: list = field(default_factory=list)  # distinct accounts found


class Adapter(ABC):
    """One file format -> canonical rows."""

    name = "base"

    @abstractmethod
    def detect(self, path: str) -> bool:
        """Return True if this adapter handles the file at path."""
        ...

    @abstractmethod
    def load(self, path: str) -> LoadResult:
        """Read + normalize the file. Must not touch the database."""
        ...
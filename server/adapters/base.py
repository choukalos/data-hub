from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class NormalizedTxn:
    """Canonical transaction row (pre-DB)."""

    account_name: str
    institution: str
    txn_date: str  # YYYY-MM-DD
    amount_cents: int  # signed; negative = spend
    payee: str = ""
    description: str = ""
    external_txn_id: str | None = None

    def content_hash(self, account_id: int) -> str:
        """Dedupe key: sha256(account_id|txn_date|amount_cents|payee)."""
        import hashlib

        raw = f"{account_id}|{self.txn_date}|{self.amount_cents}|{self.payee}"
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class LoadResult:
    """Output of an adapter: normalized rows + issues found while reading."""

    rows: list[NormalizedTxn] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    accounts: list[dict] = field(default_factory=list)  # distinct accounts found


class Adapter(ABC):
    """A source adapter reads one file format into canonical rows."""

    name = "base"

    @abstractmethod
    def detect(self, path: str) -> bool:
        """Return True if this adapter can read the file at path."""
        ...

    @abstractmethod
    def load(self, path: str) -> LoadResult:
        """Read + normalize. Must not touch the DB (pure function of the file)."""
        ...
"""Bank CSV adapter — PHASE 1 (format pending sample files).

Column mapping + dedupe key will be defined once chuck's bank CSVs land in
/home/chuck/data/finance/bank (and bank-raw for the older historical files,
which have poor/missing categories — those load fine, categories are
assigned later via rules/AI suggestions).
"""

from .base import Adapter, LoadResult


class BankCsvAdapter(Adapter):
    name = "bank_csv"

    def detect(self, path: str) -> bool:
        return path.lower().endswith(".csv")

    def load(self, path: str) -> LoadResult:
        raise NotImplementedError(
            "bank_csv adapter lands in phase 1 — waiting on sample CSV formats"
        )
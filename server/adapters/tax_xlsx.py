"""Tax XLSX adapter — PHASE 1 (format pending sample file).

Expected: one sheet worth pulling, one row per year:
year, income, income tax paid, estimated savings (+ optional notes).
Upsert into tax_years keyed on year (newer file wins).
"""

from .base import Adapter, LoadResult


class TaxXlsxAdapter(Adapter):
    name = "tax_xlsx"

    def detect(self, path: str) -> bool:
        return path.lower().endswith((".xlsx", ".xls"))

    def load(self, path: str) -> LoadResult:
        raise NotImplementedError(
            "tax_xlsx adapter lands in phase 1 — waiting on sample XLSX"
        )
from .base import Adapter, LoadResult, NormalizedTxn
from .bank_csv import BankCsvAdapter
from .tax_xlsx import TaxXlsxAdapter

ADAPTERS: dict[str, Adapter] = {
    BankCsvAdapter.name: BankCsvAdapter(),
    TaxXlsxAdapter.name: TaxXlsxAdapter(),
}

__all__ = ["Adapter", "LoadResult", "NormalizedTxn", "ADAPTERS"]
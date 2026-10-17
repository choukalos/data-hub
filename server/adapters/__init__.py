from .base import Adapter, LoadResult, NormalizedTxn
from .starone_csv import StaroneCsvAdapter
from .tax_xlsx import TaxXlsxAdapter

ADAPTERS: dict[str, Adapter] = {
    StaroneCsvAdapter.name: StaroneCsvAdapter(),
    TaxXlsxAdapter.name: TaxXlsxAdapter(),
}

__all__ = ["Adapter", "LoadResult", "NormalizedTxn", "ADAPTERS"]
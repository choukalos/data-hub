"""ETL pipeline — PHASE 1.

Flow per ingested file:
  1. adapter.load(path)            -> LoadResult (normalized rows + issues)
  2. upsert accounts               (name+institution unique)
  3. compute content_hash per row
  4. INSERT ... ON DUPLICATE KEY UPDATE into transactions
     (newer export wins for description/external_txn_id/source_file)
  5. apply category rules to new/uncategorized rows
  6. queue AI suggestions for unmatched payees (phase 2)
  7. rebuild monthly_category_summary
  8. write load_log row (rows total/new/updated/skipped, date range, issues)

All of the above in one transaction per file.
"""
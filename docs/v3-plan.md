# v3 plan: multi-year finance review + report-hub dashboards over hub data

Status: **implemented 2026-10-18** (v3-a + v3-b; verified live on
thor.local; see §Status for the verification record).

Implements the v3 items from ARCHITECTURE.md §9 (Scope). v3 is a
**report-hub-centric** release: all new capability is dashboards/queries in
the `reports` database (the AI-managed read-only contract), plus one small
demo dataset in the `hub` db. No data-hub or report-hub engine code changes.

## 1. Goals (chuck's asks)

1. **Report-hub dashboards over hub generic data** — the v2 generic-dataset
   pipeline lands data in `hub.*` tables, but nothing displayed it. v3 makes
   generic data visible in report-hub: a static Data Hub dashboard (registry
   + ingest history) and a per-dataset dashboard pattern, demonstrated
   end-to-end with a demo dataset.
2. **Finance: review prior years** — the v1 dashboards are all
   trailing-12-month / latest-month windows. v3 adds a year-parameterized
   review dashboard covering the full history of the data
   (2021-01-02 → present; 9,810 txns as of 2026-10-18), so any prior year
   can be reviewed, not just the last year.

## 2. Scope

### v3-a — Yearly review dashboard (report-hub)

New dashboard `/d/yearly` (slug `yearly`) with a `year` param_nav
dropdown (years present in `finance.transactions`, newest first; default =
latest year in the data, currently 2026, partial).

| Card | Type | Notes |
|---|---|---|
| Spend in :year | label | currency; YoY delta vs prior calendar year (fractional change, v1 convention) |
| Transactions in :year | label | txn count |
| Monthly spend in :year | chart (column) | 12-month spine Jan–Dec for the selected year |
| Category mix in :year | chart (pie) | top-10 leaf categories + Other |
| Spend by year (all data) | chart (bar) | no param — full-history context |
| Category summary in :year | table | leaf category, bucket, txns, spend, share % |
| Top payees in :year | table | payee, category, txns, total, last seen (250/page) |
| Monthly totals in :year | table | month, txns, spend, avg txn |
| Transactions in :year | table | all txns in the year, date/payee/category/amount (250/page) |

Conventions (unchanged from v1): spend = SUM of negative amounts, excluding
Transfer + Income buckets; money in cents in SQL, dollars for charts;
`?year=` validated against `allowed_query` (distinct years in the data),
invalid/absent values fall back to the default year.

### v3-b — Dashboards over hub generic data (report-hub)

1. **Static Data Hub dashboard** `/d/datahub` (slug `datahub`) — always
   valid, no dynamic table references:
   - labels: active dataset count, rows loaded (all time)
   - table: dataset registry (`hub.datasets`: name, description,
     source_pattern, target_table, active, timestamps)
   - table: ingest history (`hub.load_log`, newest first, 250/page)
   - chart: rows loaded by day, trailing 90 days (`hub.load_log`)
2. **Per-dataset dashboard pattern** — the engine substitutes params as
   *escaped string literals only*, so a dashboard cannot reference a
   dynamic `hub.ds_<name>` table via a param. Per-dataset dashboards are
   therefore **generated per dataset** (the documented AI-managed workflow):
   static SQL against the concrete target table. Pattern documented in the
   report-hub README ("Dashboard over a hub dataset").
3. **Demo** — `demo-weather` dataset (14 daily readings: date, temp_c,
   humidity_pct; clearly labeled sample data) ingested through the normal
   v2 pipeline (register → inbox → watcher), with a `/d/demo-weather`
   dashboard (labels, dual-series chart, raw table) proving the full loop:
   CSV → `hub.ds_demo-weather` → report-hub dashboard.

### Out of v3 (still "later")

- alert push (n8n), finance/reports backups, more chart types (v2 "later"
  list, unchanged)
- real datasets for the hub (chuck's data; the demo stands in until then)
- automatic per-dataset dashboard generation (v3 documents the pattern;
  generating one is a one-shot AI task per dataset)

## 3. Design notes

- **No engine changes.** Everything v3 needs already exists: `:param`
  substitution with `allowed_query` validation + fallback (v1 `categories`
  dashboard), `param_nav` dropdowns, cross-database SELECTs (dh_reports has
  SELECT on `finance.*` + `hub.*`; card queries run on the finance
  connection with fully-qualified `hub.*` names), 250-row pagination.
- **Year param type.** `YEAR()` returns INT; the param machinery compares
  strings, so the nav/allowed queries `CAST(YEAR(txn_date) AS CHAR)`.
  Prior-year arithmetic uses `:year - 1` (string coerces in MySQL
  arithmetic) inside `BETWEEN`.
- **Partial latest year.** The default year (2026) is partial; monthly
  cards show the YTD shape (future months = 0), which is the honest view.
- **Seeds are idempotent** (delete-then-insert per slug, v1 convention),
  applied as `dh_reports` (the AI-managed writer; the UI never writes).

## 4. Verification (2026-10-18, live on thor.local)

- `/d/yearly` default (2026): all 9 cards render, no card errors.
- `/d/yearly?year=2024`, `?year=2021`: param substitution + nav dropdown
  verified; spend/monthly/category cards reflect the selected year.
- `/d/yearly?year=1999` (not in data): falls back to default year.
- `/d/datahub`: registry + load_log tables render real rows; 90-day chart
  has data.
- `/d/demo-weather`: `hub.ds_demo-weather` ingested via watcher (load_log
  row, status ok); chart + table render.
- Dashboard list page (`/`) shows the three new dashboards.

## 5. Files

- report-hub repo: `seed/v3_yearly_review.sql`, `seed/v3_datahub.sql`
  (idempotent seeds), README dashboard table + "Dashboard over a hub
  dataset" section.
- homelab repo: `data-hub/ARCHITECTURE.md` §9/§10 v3 rows.
- data-hub repo: this doc.
- `hub` db: `datasets` row `demo-weather` + table `hub.ds_demo-weather`
  (live data, not a migration — created via the v2 API/inbox pipeline).
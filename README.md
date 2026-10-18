# data-hub

Ingest → ETL → MySQL for the homelab. The **only writer** to the `finance`
database. Companion: [report-hub](../report-hub) (strictly read-only
reporting UI over the same data).

Architecture doc: `homelab/data-hub/ARCHITECTURE.md`

## What it does (v1)

- **Ingest doors**: `POST /api/ingest` (multipart upload) + watcher on
  `/home/chuck/data/finance/` subdirectories (see `sources.yaml`)
- **Source adapters** (one per file format): `bank_csv`, `tax_xlsx`
- **ETL**: staging → canonical, idempotent upserts
  (dedupe key `sha256(account|date|amount_cents|payee)`), overlapping files safe
- **Provenance**: `load_log` per file (sha256, counts, date range, issues)
- **Categorization**: rules + AI suggestions + human review flow
- **Migrations**: versioned SQL in `migrations/finance/`, auto-applied on start

## What it does (v2a — generic datasets)

Drop any CSV/TSV into `inbox/` (or `POST /api/ingest?source=inbox`) and it
lands idempotently in a table under the separate `hub` database — the
daily-ingest pattern for non-finance data (work-app analytics, blog
metrics, anything). Design: `docs/v2-generic-datasets.md` (v2a).

- **Registry** `hub.datasets`: name (slug), `source_pattern` filename glob,
  delimiter (`,`/tab), `has_header`, `column_map` JSON, `key_columns` JSON.
  Bank/tax adapters claim files first; files matching no active pattern stay
  pending (not ingested, not moved).
- **Adapter** `server/adapters/generic_csv.py` (stdlib csv, no new deps):
  column inference on first import (all-int → BIGINT, int/float →
  DECIMAL(18,4), ISO dates → DATE, else VARCHAR(512); empty → NULL), frozen
  into `datasets.column_map` afterwards.
- **Idempotent upsert** into `hub.ds_<name>`: `content_hash` UNIQUE +
  `ON DUPLICATE KEY UPDATE` — re-dropping a file = 0 new / N updated.
- **Provenance** `hub.load_log`: same shape as `finance.load_log` plus
  `dataset_name`.
- **API**: `GET/POST /api/datasets`, `DELETE /api/datasets/{name}`
  (deactivate, rows kept), `GET /api/datasets/{name}/preview?limit=50`, and
  `GET /api/loads?db=finance|hub` (default finance, unchanged).
- The `hub` db is created by `provision.sh` (root). Until it exists, hub
  endpoints return 503 and startup skips hub migrations with a warning —
  finance ingest is never affected.

## Data layout (host)

```
/home/chuck/data/finance/
  bank/        current bank CSVs (categorized)
  bank-raw/    older raw CSVs (historical, poor categories)
  tax/         tax XLSX (one row per year)
  inbox/       generic CSV/TSV datasets (v2a, resolved per file)
  processed/   data-hub moves ingested files here (auto-created)
```

## Setup (one-time, as root)

```sh
bash /home/chuck/homelab/data-hub/provision.sh
```

Creates `finance` + `reports` + `hub` DBs and the `dh_app` / `dh_reports`
users, and grants `SELECT finance.*` to the existing `ai` user (mcp_mysql
Q&A). `dh_reports` also gets `SELECT hub.*` for report-hub dashboards over
generic data.

## Run

```sh
cd /home/chuck/homelab/compose
docker compose -f compose.data-hub.yml up -d --build data-hub
curl -s http://thor.local:8100/api/health
```

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | liveness + counts + last load |
| POST | `/api/migrate` | apply pending migrations (finance + hub) |
| GET | `/api/loads?limit=&db=finance\|hub` | load log (default finance) |
| GET | `/api/loads/{id}` | load detail (incl. issues) |
| POST | `/api/ingest` | upload file for ingest (`?source=inbox` for generic) |
| GET | `/api/datasets` | generic dataset registry (with target-table row counts) |
| POST | `/api/datasets` | register a dataset `{name, source_pattern, ...}` |
| DELETE | `/api/datasets/{name}` | deactivate a dataset (rows kept) |
| GET | `/api/datasets/{name}/preview?limit=50` | latest rows from the target table |
| GET | `/api/categories` | category tree with per-node txn counts + totals |
| GET | `/api/categories/pending` | pending AI suggestions (category name/bucket + affected txn count) |
| POST | `/api/categories/review` | approve / reject / edit a suggestion |
| POST | `/api/categories/apply-rules` | re-run the rules engine over NULL-category txns |
| POST | `/api/categories/suggest` | run one AI suggestion pass now (LiteLLM) |

## Categorization loop (phase 2)

1. **Ingest** loads transactions; rows whose category can't be resolved get
   `category_id = NULL`.
2. **Rules engine** (`server/categorize.py::apply_rules`) matches the payee of
   NULL-category transactions against `category_rules` with
   exact → contains → prefix → regex precedence (first match wins; bad
   regexes are skipped + logged). Runs automatically after every ingest and
   can be re-run with `POST /api/categories/apply-rules`.
3. **AI suggestions** (`suggest_categories`) asks the LiteLLM proxy (cheap
   model, strict JSON) to pick the best *existing* category for each distinct
   uncategorized payee (batches of 40, category tree passed in, leaf
   preferred). Results land in `category_suggestions` as `pending`, one per
   payee. Runs automatically after every ingest (best-effort — failures are
   logged to the load's issues and never fail the ingest) and on demand via
   `POST /api/categories/suggest` (response includes an LLM call log).
4. **Human review** via `GET /api/categories/pending` +
   `POST /api/categories/review`:
   - `approve` → creates an exact-payee rule (`source='ai'`), backfills the
     payee's NULL-category transactions, rebuilds `monthly_category_summary`
   - `reject` → marks the suggestion rejected
   - `edit` → change the suggested category, then behaves like approve

## v1 dashboards + ingest/review loop (phase 4)

**Dashboards.** The read-only [report-hub](../report-hub) (:8101) ships
five v1 finance dashboards, seeded from
`report-hub/seed/v1_finance_dashboards.sql` (money = integer cents; spend =
debits, Transfer/Income excluded):

| Dashboard | Answers |
|---|---|
| `/d/overview` | Spend last month / 12mo with MoM/YoY deltas, savings rate (latest tax year), kid share of spend, monthly trend + top-10 categories |
| `/d/categories?category=<Bucket>` | One top-level bucket: 12mo spend + MoM, 24mo trend, top payees, recent transactions, all-21-bucket comparison (default = top-spend bucket) |
| `/d/kids` | Kid spend (Kids + Education buckets): 12mo, share of total, YoY, by year, top payees, recent transactions |
| `/d/tax-savings` | Income / income tax paid / est. savings / savings rate per tax year, with prior-year deltas |
| `/d/data-health` | Ingest history, uncategorized count, pending suggestions, per-bucket MoM/YoY spend watch (ALERT when > 30%) |

Open them at `http://thor.local:8101/d/<slug>`. To refresh the seeded
static bits (e.g. after the `?category=` default or the kid definition
changes), re-run the idempotent seed as `dh_reports`:
`mysql -h thor.local -u dh_reports -p reports <
report-hub/seed/v1_finance_dashboards.sql`.

**Ingest new files.** Drop a bank CSV / tax XLSX into a watched
subdirectory of `/home/chuck/data/finance/` (see `sources.yaml` —
currently `starone/` and `tax/`; ingested files move to `processed/`) or
upload via `POST /api/ingest` (multipart). The watcher and the endpoint
run the same pipeline: rules engine → AI suggestions → `load_log`
provenance. Check `GET /api/loads` afterwards. Generic CSV/TSV files go to
`inbox/` (v2a): bank/tax adapters claim first, then active
`hub.datasets.source_pattern` globs; unmatched files stay pending. Check
`GET /api/datasets` + `GET /api/loads?db=hub` afterwards.

**Categorization review loop.** Uncategorized payees get AI suggestions
(`category_suggestions`, status `pending`) after each ingest. Review them
with `GET /api/categories/pending` + `POST /api/categories/review`
(approve → creates a rule + backfills the payee's transactions; reject;
or edit the category first). Re-run the rules engine over NULL-category
txns with `POST /api/categories/apply-rules`, and force an AI suggestion
pass with `POST /api/categories/suggest`. Approved suggestions become
rules, so the same payee is never re-suggested.

## Security

- LAN only, no auth (homelab-only by design).
- `dh_app` has ALL on `finance.*` and `hub.*` only. It is the sole writer
  to finance and hub data.
- Bank data never leaves the homelab.

## Repo layout

```
migrations/finance/   versioned schema migrations (0001_init.sql, ...)
migrations/hub/       v2a hub schema (datasets + load_log)
server/
  main.py             FastAPI app
  config.py           env config
  db.py               connections + migration runner
  adapters/           per-format file adapters (starone_csv, tax_xlsx,
                      generic_csv)
  etl/                staging → canonical pipeline + dataset upsert
sources.yaml          inbox subdir -> adapter registry
tests/                unit tests (python3 tests/test_generic_csv.py)
```
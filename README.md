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

## Data layout (host)

```
/home/chuck/data/finance/
  bank/        current bank CSVs (categorized)
  bank-raw/    older raw CSVs (historical, poor categories)
  tax/         tax XLSX (one row per year)
  processed/   data-hub moves ingested files here (auto-created)
```

## Setup (one-time, as root)

```sh
bash /home/chuck/homelab/data-hub/provision.sh
```

Creates `finance` + `reports` DBs and the `dh_app` / `dh_reports` users,
and grants `SELECT finance.*` to the existing `ai` user (mcp_mysql Q&A).

## Run

```sh
cd /home/chuck/homelab/compose
docker compose -f compose.data-hub.yml up -d --build data-hub
curl -s http://thor.local:8100/api/health
```

## API (v1)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | liveness + counts + last load |
| POST | `/api/migrate` | apply pending migrations |
| GET | `/api/loads?limit=` | load log |
| GET | `/api/loads/{id}` | load detail (incl. issues) |
| POST | `/api/ingest` | upload file for ingest |
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

## Security

- LAN only, no auth (homelab-only by design).
- `dh_app` has ALL on `finance.*` only. It is the sole writer to finance data.
- Bank data never leaves the homelab.

## Repo layout

```
migrations/finance/   versioned schema migrations (0001_init.sql, ...)
server/
  main.py             FastAPI app
  config.py           env config
  db.py               connections + migration runner
  adapters/           per-format file adapters (bank_csv, tax_xlsx)
  etl/                staging → canonical pipeline
sources.yaml          inbox subdir -> adapter registry
```
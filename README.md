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
| POST | `/api/ingest` | upload file (phase 1) |
| GET | `/api/categories` | category tree (phase 2) |
| GET | `/api/categories/pending` | AI suggestions (phase 2) |
| POST | `/api/categories/review` | approve/reject/edit (phase 2) |

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
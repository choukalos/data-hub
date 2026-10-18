# v2 design: generic dataset ETL + mcp_datahub

Status: v2a implemented (2026-10-18; production E2E pending `hub` db
provisioning — `provision.sh` needs root; unit tests + scratch-MySQL E2E
passed). v2b (mcp_datahub) remains spec.
Implements the v2 items from ARCHITECTURE.md sections 9/10:
(1) generic-dataset ETL adapter, (2) MCP wrapper for data-hub.

## v2a — Generic dataset adapter (data-hub)

Goal: drop any CSV/TSV into the inbox (or POST it) and have it land
idempotently in a MySQL table under a new `hub` database — the
daily-ingest-into-MySQL pattern for non-finance data (work-app analytics,
blog metrics, anything).

### Database: `hub` (new; migrations in `migrations/hub/`)

- `hub.datasets` — registry:
  - `id`, `name` UNIQUE (slug, e.g. `workapp`), `description`
  - `source_pattern` — filename glob the watcher matches (e.g. `workapp-*.csv`)
  - `target_table` — fully qualified (default `hub.ds_<name>`)
  - `delimiter` — `,` | `\t` (default `,`)
  - `has_header` — tinyint default 1
  - `column_map` JSON — `{"csv_col": "target_col TYPE", ...}`; when NULL,
    schema is inferred on first import and frozen into this column
  - `key_columns` JSON — list of target columns forming the dedupe key
    (NULL = all columns)
  - `is_active` tinyint default 1, timestamps
- `hub.load_log` — same schema as `finance.load_log` (file, sha256,
  adapter='generic', dataset_name, rows_total/new/updated/skipped,
  date_range, status, issues JSON).

MySQL users: `dh_app` gets the same grants on `hub` as on `finance`
(provision.sh + a note; the container already connects with dh_app).
`dh_reports` gets SELECT on `hub.*` so report-hub can build dashboards
over generic data.

### Adapter: `server/adapters/generic_csv.py`

- `GenericCsvAdapter` implements the existing `Adapter` interface
  (name, can_handle(path), normalize(path) -> rows + report).
- Reads with the csv module (delimiter from registry), header row per
  config.
- Column inference (only when `column_map` is NULL): sample up to 200
  rows; per column: all-integer -> BIGINT, integer-or-float ->
  DECIMAL(18,4), ISO-ish dates -> DATE, else VARCHAR(512) (empty -> NULL).
  Target column names: lowercased, non-alnum -> `_`.
- `content_hash` = sha256(target_table | key column values joined by \x1f)
  when key_columns set, else sha256 of all normalized values.
- Upsert: `INSERT ... ON DUPLICATE KEY UPDATE` on the mutable columns,
  preserving `ingested_at`; target table created with
  `CREATE TABLE IF NOT EXISTS` (id BIGINT PK AUTO_INCREMENT, content_hash
  CHAR(64) UNIQUE, ingested_at TIMESTAMP, inferred columns).
- Validation: row count, header presence, all-empty file, bad row
  (column count mismatch) -> skipped + issue in load report.

### Pipeline integration

- Watcher: after the bank/tax adapters fail to claim a file, scan
  `hub.datasets` (active) for a matching `source_pattern` (fnmatch on
  basename) and run the generic adapter. Files matching nothing stay
  `pending` (existing behavior).
- `ingest_file(conn, path, adapter)` in `pipeline.py` already
  genericizes staging/upsert per adapter — extend the canonical-upsert
  step: bank/tax keep their dedicated upserts; generic gets
  `upsert_dataset_rows(conn, dataset, rows)`.
- load_log rows for generic loads go to `hub.load_log`; `GET /api/loads`
  gains `?db=finance|hub` (default finance, existing behavior unchanged).

### API (data-hub)

- `GET  /api/datasets` — registry list (with target table row counts)
- `POST /api/datasets` — register `{name, description?, source_pattern,
  delimiter?, has_header?, column_map?, key_columns?}`; validates name
  slug, pattern non-empty, column_map keys sane.
- `DELETE /api/datasets/{name}` — deactivate (rows kept).
- `GET  /api/datasets/{name}/preview?limit=50` — latest rows from the
  target table (read via dh_app).
- Existing `POST /api/ingest` (manual single-file) works unchanged —
  adapter resolution picks generic when the pattern matches.

### Tests / verification

- Unit: inference (int/float/date/str/empty), hash with/without keys,
  delimiter, header off.
- E2E: register a scratch dataset, drop a CSV into the inbox, watch the
  watcher ingest it, re-drop the same file (0 new / N updated), drop a
  changed file (updated), verify `hub.load_log` + `/api/loads?db=hub` +
  preview endpoint. Clean up scratch dataset + table after.

## v2b — mcp_datahub (MCP server)

New MCP server `mcp_datahub` (same pattern as `mcp_mysql` in
`/home/chuck/homelab/mcp/`): FastMCP, HTTP transport, LAN-only, wraps the
data-hub REST API (http://192.168.4.54:8100) so any LLM client can drive
ingest + categorization review:

- `ingest_file(filename)` — POST a file from the host inbox path
  (`/home/chuck/data/finance/inbox/<filename>`) to data-hub; returns the
  load report.
- `list_loads(db="finance", limit=20)` — recent loads.
- `load_detail(load_id)` — full load report + issues.
- `pending_categories()` — uncategorized payees with 12-month totals.
- `suggest_categories()` — trigger the LiteLLM suggestion pass.
- `review_category(suggestion_id, action, category_id?)` —
  approve/reject/edit via the review API.
- `list_datasets()` / `dataset_preview(name)` — v2a registry.

Compose: add `mcp_datahub` service to the homelab compose (or its own
file), env: DATAHUB_URL, INBOX_DIR. Tool descriptions written for LLM
consumption (what each returns, when to use). Verify with an MCP client
handshake + one tool call each (read-only ones; do not trigger real
ingests of real files during verification — use a scratch file).

## Out of scope (v2)

n8n alert push, backups, extra chart types (tracked in ARCHITECTURE v2
list; separate follow-ups).
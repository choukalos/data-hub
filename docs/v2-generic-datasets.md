# v2 design: generic dataset ETL + mcp_datahub

Status: v2a implemented (2026-10-18; production E2E pending `hub` db
provisioning — `provision.sh` needs root; unit tests + scratch-MySQL E2E
passed). v2b (mcp_datahub) implemented 2026-10-18 (10 tools, MCP handshake
+ read-only tool calls verified; ingest_file verified with a scratch file).
Both pending the `hub` db provisioning + data-hub restart for the full
production E2E.
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

Implemented 2026-10-18 at `homelab/mcp/servers/datahub/` (server.py,
Dockerfile, pyproject.toml, README.md); compose service `mcp_datahub` in
`homelab/compose/compose.mcp.yml`; registered in `homelab/litellm/config.yml`
(SSE, allow_all_keys, like the other MCP servers).

New MCP server `mcp_datahub` (same pattern as `mcp_mysql` in
`/home/chuck/homelab/mcp/`): FastMCP, SSE transport, LAN-only, wraps the
data-hub REST API (http://data-hub:8100) so any LLM client can drive
ingest + categorization review:

- `health()` — liveness + counts + last load + pending files.
- `ingest_file(filename)` — POST a file from the mounted inbox
  (`/home/chuck/data/finance/inbox` -> `/inbox:ro`) to data-hub
  (`?source=inbox`); returns the load report. Filename sanitized
  (alnum/./-/_) — also blocks path traversal.
- `list_loads(db="finance", limit=20)` — recent loads (finance | hub).
- `load_detail(load_id)` — full finance load report + issues (hub loads
  carry the full report in list_loads(db="hub")).
- `pending_categories()` — suggestions awaiting review.
- `list_categories()` — category tree with ids (needed for edit reviews).
- `suggest_categories()` — trigger the LiteLLM suggestion pass.
- `review_category(suggestion_id, action, category_id?)` —
  approve/reject/edit via the review API.
- `list_datasets()` / `dataset_preview(name, limit)` — v2a registry.

Env: DATAHUB_URL, INBOX_DIR, HTTP_TIMEOUT (60s), INGEST_TIMEOUT (600s —
LLM passes are slow). Tool descriptions written for LLM consumption (what
each returns, when to use). Verified with an MCP client handshake + one
tool call each (read-only ones; ingest_file with a scratch file — with the
hub db unprovisioned it can only come back status=pending, so no real
data moves).

## Out of scope (v2)

n8n alert push, backups, extra chart types (tracked in ARCHITECTURE v2
list; separate follow-ups).
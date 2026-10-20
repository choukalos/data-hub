-- 0002_manual_edits.sql — audit trail for manual row edits (v2b)
-- Applied by the data-hub migration runner (tracked in hub.schema_migrations).
-- Convention: no semicolons inside string literals (runner splits on the
-- semicolon character).
--
-- Same shape as finance.manual_edits. One row per manually changed row
-- (PATCH/POST/DELETE/bulk via /api/tables). `bulk` writes one row per
-- affected row so re-import protection can find locked fields by
-- (table_name, row_id). row_id references the table's primary key
-- (ds_*.id, ...). `changes` shape: {field: {"old": ..., "new": ...}}.

CREATE TABLE IF NOT EXISTS manual_edits (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  table_name   VARCHAR(128) NOT NULL,
  row_id       BIGINT UNSIGNED NOT NULL,
  action       ENUM('edit','insert','delete','bulk') NOT NULL,
  changes      JSON NOT NULL,
  reason       VARCHAR(512) NOT NULL DEFAULT '',
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_me_table_row (table_name, row_id),
  KEY idx_me_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
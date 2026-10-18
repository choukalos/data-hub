-- 0001_init.sql — hub schema v1 (v2a generic datasets)
-- Applied by the data-hub migration runner (tracked in hub.schema_migrations).
-- Convention: no semicolons inside string literals (runner splits on the
-- semicolon character).

-- Registry of generic CSV/TSV datasets. name is a slug (e.g. workapp).
-- target_table is fully qualified (e.g. hub.ds_workapp). The hub.ds_<name>
-- default is applied by the API at registration time because MySQL cannot
-- default a column from a sibling column.
CREATE TABLE IF NOT EXISTS datasets (
  id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name            VARCHAR(80) NOT NULL,
  description     VARCHAR(512) NOT NULL DEFAULT '',
  source_pattern  VARCHAR(255) NOT NULL,
  target_table    VARCHAR(191) NOT NULL,
  `delimiter`     CHAR(1) NOT NULL DEFAULT ',',
  has_header      TINYINT(1) NOT NULL DEFAULT 1,
  column_map      JSON NULL,
  key_columns     JSON NULL,
  is_active       TINYINT(1) NOT NULL DEFAULT 1,
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_datasets_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Provenance for generic dataset loads. Same shape as finance.load_log
-- plus dataset_name.
CREATE TABLE IF NOT EXISTS load_log (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_file   VARCHAR(255) NOT NULL,
  sha256        CHAR(64) NOT NULL DEFAULT '',
  adapter       VARCHAR(40) NOT NULL,
  dataset_name  VARCHAR(80) NOT NULL DEFAULT '',
  rows_total    INT NOT NULL DEFAULT 0,
  rows_new      INT NOT NULL DEFAULT 0,
  rows_updated  INT NOT NULL DEFAULT 0,
  rows_skipped  INT NOT NULL DEFAULT 0,
  date_min      DATE NULL,
  date_max      DATE NULL,
  status        ENUM('ok','partial','error') NOT NULL DEFAULT 'ok',
  issues        JSON NULL,
  KEY idx_load_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
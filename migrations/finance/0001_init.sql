-- 0001_init.sql — finance schema v1
-- Applied by the data-hub migration runner (tracked in finance.schema_migrations).
-- Convention: no semicolons inside string literals (runner splits on ';').

CREATE TABLE IF NOT EXISTS accounts (
  id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name          VARCHAR(120) NOT NULL,
  institution   VARCHAR(120) NOT NULL DEFAULT '',
  type          ENUM('checking','savings','credit','other') NOT NULL DEFAULT 'other',
  currency      CHAR(3) NOT NULL DEFAULT 'USD',
  is_active     TINYINT(1) NOT NULL DEFAULT 1,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_accounts_name_inst (name, institution)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS categories (
  id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name          VARCHAR(80) NOT NULL,
  -- 0 = top-level (bucket). NOT NULL (not NULL) so the unique key below
  -- dedupes correctly (MySQL unique keys treat NULLs as distinct).
  parent_id     INT UNSIGNED NOT NULL DEFAULT 0,
  bucket        VARCHAR(40) NOT NULL DEFAULT 'Other',
  sort_order    INT NOT NULL DEFAULT 0,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_categories_name_parent (name, parent_id),
  KEY idx_categories_bucket (bucket)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dedupe key: content_hash = sha256(account_id|txn_date|amount_cents|payee|description).
-- Same transaction re-exported (even from overlapping files) = same hash
-- (verified 2026-10-17: all 1,962 cross-file dups byte-identical).
-- Upsert semantics: ON DUPLICATE KEY UPDATE description/category_id/source_file
-- (newer export wins for mutable fields), imported_at preserved.
CREATE TABLE IF NOT EXISTS transactions (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  account_id      INT UNSIGNED NOT NULL,
  txn_date        DATE NOT NULL,
  amount_cents    BIGINT NOT NULL,
  payee           VARCHAR(255) NOT NULL DEFAULT '',
  description     VARCHAR(512) NOT NULL DEFAULT '',
  category_id     INT UNSIGNED NULL,
  external_txn_id VARCHAR(120) NULL,
  content_hash    CHAR(64) NOT NULL,
  source_file     VARCHAR(255) NOT NULL DEFAULT '',
  imported_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_txn_content (content_hash),
  KEY idx_txn_date (txn_date),
  KEY idx_txn_category (category_id),
  KEY idx_txn_payee (payee(64)),
  KEY idx_txn_ext (external_txn_id),
  CONSTRAINT fk_txn_account FOREIGN KEY (account_id) REFERENCES accounts (id),
  CONSTRAINT fk_txn_category FOREIGN KEY (category_id) REFERENCES categories (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS category_rules (
  id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  pattern       VARCHAR(255) NOT NULL,
  match_type    ENUM('exact','contains','prefix','regex') NOT NULL DEFAULT 'contains',
  category_id   INT UNSIGNED NOT NULL,
  source        ENUM('manual','ai') NOT NULL DEFAULT 'manual',
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_rules_pattern (pattern, match_type, category_id),
  CONSTRAINT fk_rules_category FOREIGN KEY (category_id) REFERENCES categories (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- AI-suggested payee->category mappings awaiting human review.
CREATE TABLE IF NOT EXISTS category_suggestions (
  id                     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  payee                  VARCHAR(255) NOT NULL,
  suggested_category_id  INT UNSIGNED NOT NULL,
  confidence             DECIMAL(4,3) NOT NULL DEFAULT 0,
  rationale              VARCHAR(255) NOT NULL DEFAULT '',
  status                 ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
  created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  reviewed_at            TIMESTAMP NULL,
  KEY idx_sugg_status (status),
  KEY idx_sugg_payee (payee(64)),
  CONSTRAINT fk_sugg_category FOREIGN KEY (suggested_category_id) REFERENCES categories (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- From the tax XLSX: one row per year (income, income tax paid, estimated savings).
CREATE TABLE IF NOT EXISTS tax_years (
  year                   SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
  income_cents           BIGINT NOT NULL DEFAULT 0,
  income_tax_paid_cents  BIGINT NOT NULL DEFAULT 0,
  savings_est_cents      BIGINT NOT NULL DEFAULT 0,
  savings_rate           DECIMAL(6,4) NULL,
  notes                  VARCHAR(512) NOT NULL DEFAULT '',
  updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Provenance: one row per ingested file.
CREATE TABLE IF NOT EXISTS load_log (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  ts            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_file   VARCHAR(255) NOT NULL,
  sha256        CHAR(64) NOT NULL DEFAULT '',
  adapter       VARCHAR(40) NOT NULL,
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

-- Materialized rollup, rebuilt after each ingest (fast dashboards).
CREATE TABLE IF NOT EXISTS monthly_category_summary (
  year           SMALLINT UNSIGNED NOT NULL,
  month          TINYINT UNSIGNED NOT NULL,
  category_id    INT UNSIGNED NOT NULL,
  amount_cents   BIGINT NOT NULL DEFAULT 0,
  txn_count      INT NOT NULL DEFAULT 0,
  PRIMARY KEY (year, month, category_id),
  CONSTRAINT fk_mcs_category FOREIGN KEY (category_id) REFERENCES categories (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
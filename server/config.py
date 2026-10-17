import os

# --- MySQL (data-hub is the ONLY writer to the finance db) ---
MYSQL_HOST = os.environ.get("MYSQL_HOST", "thor.local")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "dh_app")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
FINANCE_DB = os.environ.get("FINANCE_DB", "finance")

# --- Ingestion ---
# INBOX_ROOT is the finance data dir (host: /home/chuck/data/finance).
# Subdirectories are mapped to adapters in sources.yaml.
INBOX_ROOT = os.environ.get("INBOX_ROOT", "/data")
SOURCES_FILE = os.environ.get("SOURCES_FILE", "/app/sources.yaml")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "10"))
PROCESSED_DIRNAME = os.environ.get("PROCESSED_DIRNAME", "processed")

# --- LiteLLM (AI category suggestions, phase 2) ---
LITELLM_API_BASE = os.environ.get("LITELLM_API_BASE", "http://litellm-proxy:4000")
LITELLM_API_KEY = os.environ.get("LITELLM_API_KEY", "")
LITELLM_MODEL = os.environ.get("LITELLM_MODEL", "matrix-coder")
LITELLM_DISABLE_THINKING = os.environ.get("LITELLM_DISABLE_THINKING", "true").lower() in ("true", "1", "yes")
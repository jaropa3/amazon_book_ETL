-- schema.sql — pierwotne postawienie bazy (ang. initial standup).
-- Jedyne źródło prawdy dla DDL całej bazy: bronze (dane) + logs (rejestr runów).
-- Idempotentne: bezpiecznie uruchamiać wielokrotnie (ang. single source of truth).
--
-- Uruchomienie (jednorazowo, po utworzeniu bazy):
--   python scripts/init_db.py

-- bronze przechowuje surowe dane z JEDNEJ sesji scrapowania (TRUNCATE+INSERT w ingest.py).
-- Wszystkie kolumny TEXT — typowanie robi dbt staging (::numeric, ::timestamp),
-- dzięki czemu bronze nigdy nie odrzuca wiersza za niezgodny typ (ang. schema-on-read).
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.books (
    asin       TEXT,
    title      TEXT,
    author     TEXT,
    price      TEXT,
    rating     TEXT,
    scraped_at TEXT
);

-- logs: rejestr przebiegów pipeline'u — jeden wiersz na dag_run (UPSERT po dag_run_id
-- w logging_db.py). Pipeline tylko zapisuje i czyta; struktury nie tworzy.
CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.pipeline_runs (
    run_id                SERIAL PRIMARY KEY,
    run_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_run_id            TEXT UNIQUE,   -- wymagane przez ON CONFLICT w upsert_pipeline_run
    dag_id                TEXT,
    status                TEXT,
    run_type              TEXT,
    scraped_count         INT,
    gold_inserted_count   INT,
    registry_new_count    INT,
    failed_task           TEXT,
    error_message         TEXT,
    duration_seconds      NUMERIC
);

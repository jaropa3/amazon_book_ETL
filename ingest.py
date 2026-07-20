import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

from config import CONFIG
from connection import connection_db
from logger import setup_logger

logger = setup_logger("ingest")
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"), encoding="utf-8-sig")

RAW_DATA_DIR = os.path.join(PROJECT_DIR, CONFIG.storage.raw_data_dir)
DB_SCHEMA = CONFIG.database.db_schema
DB_TABLE = CONFIG.database.table


def _insert(cur: psycopg.Cursor, schema: str, table: str, df: pd.DataFrame) -> None:
    # Tylko przeładowanie DANYCH — struktura tabeli żyje w sql/schema.sql.
    # Nieznana kolumna w CSV → INSERT padnie od razu (Fail Fast), zamiast po cichu
    # rozszerzyć tabelę i rozjechać kontrakt z dbt staging.
    cur.execute(f"TRUNCATE TABLE {schema}.{table}")

    cols = ", ".join(f'"{col}"' for col in df.columns)
    # Bulk load przez COPY FROM STDIN — jeden strumień zamiast INSERT po wierszu
    # (ang. bulk load). write_row sam robi adaptację typów i NULL (None → \N).
    with cur.copy(f"COPY {schema}.{table} ({cols}) FROM STDIN") as copy:
        for row in df.itertuples(index=False):
            copy.write_row(tuple(None if pd.isna(v) else str(v) for v in row))


def _pick_oldest_file(raw_dir: Path, table: str) -> Path:
    """Najstarszy plik {table}_*.csv (FIFO). Fail-fast: wyjątek gdy brak plików."""
    files = sorted(raw_dir.glob(f"{table}_*.csv"))
    if not files:
        raise FileNotFoundError(f"Brak plików {table}_*.csv w {raw_dir}")
    return files[0]


def _validate_raw(df: pd.DataFrame, source: Path) -> None:
    """Fail Fast: waliduj PRZED dotknięciem bazy. Bez tego pusty/uszkodzony plik
    TRUNCATE'uje bronze i ląduje w processed/, a task pada dopiero na .iloc[0] —
    czyli bronze wyczyszczony, plik skonsumowany, dane sesji stracone bezpowrotnie.
    """
    if df.empty:
        raise ValueError(f"Pusty plik CSV (0 wierszy): {source}")
    if "scraped_at" not in df.columns:
        raise ValueError(f"Brak kolumny scraped_at w {source}")
    if df["scraped_at"].isna().all():
        raise ValueError(f"Kolumna scraped_at pusta w {source}")


def ingest_books() -> str:
    file_to_process = _pick_oldest_file(Path(RAW_DATA_DIR), DB_TABLE)
    books_raw = pd.read_csv(file_to_process, dtype=str)
    _validate_raw(books_raw, file_to_process)

    with connection_db() as con, con.cursor() as cur:
        _insert(cur, DB_SCHEMA, DB_TABLE, books_raw)
    logger.info("%s.%s: %d wierszy z %s", DB_SCHEMA, DB_TABLE, len(books_raw), file_to_process)

    processed_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # partycja po dacie UTC
    processed_dir = os.path.join(RAW_DATA_DIR, "processed", processed_day)
    os.makedirs(processed_dir, exist_ok=True)
    shutil.move(
        file_to_process, os.path.join(processed_dir, os.path.basename(file_to_process))
    )
    logger.info("przeniesiono %s → processed/%s/", os.path.basename(file_to_process), processed_day)

    return books_raw["scraped_at"].iloc[0]  # scraped_at tej sesji → XCom

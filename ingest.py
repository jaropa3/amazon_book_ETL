import os
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from config import CONFIG
from connection import connection_db

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"), encoding="utf-8-sig")

RAW_DATA_DIR = os.path.join(PROJECT_DIR, CONFIG["storage"]["raw_data_dir"])
DB_SCHEMA = CONFIG["database"]["schema"]
DB_TABLE = CONFIG["database"]["table"]


def _insert(cur, schema: str, table: str, df: pd.DataFrame) -> None:
    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ()")
    for col in df.columns:
        cur.execute(
            f'ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS "{col}" TEXT'
        )
    cur.execute(f"TRUNCATE TABLE {schema}.{table}")

    cols = ", ".join(f'"{col}"' for col in df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    rows = [
        tuple(None if pd.isna(v) else str(v) for v in row)
        for row in df.itertuples(index=False)
    ]
    cur.executemany(
        f"INSERT INTO {schema}.{table} ({cols}) VALUES ({placeholders})",
        rows,
    )


def _init_schemas(cur) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
    # cur.execute("CREATE SCHEMA IF NOT EXISTS rejected")


def _pick_oldest_file(raw_dir: Path, table: str) -> Path:
    """Najstarszy plik {table}_*.csv (FIFO). Fail-fast: wyjątek gdy brak plików."""
    files = sorted(raw_dir.glob(f"{table}_*.csv"))
    if not files:
        raise FileNotFoundError(f"Brak plików {table}_*.csv w {raw_dir}")
    return files[0]


def ingest_books() -> str:
    file_to_process = _pick_oldest_file(Path(RAW_DATA_DIR), DB_TABLE)
    books_raw = pd.read_csv(file_to_process, dtype=str)

    with connection_db() as con, con.cursor() as cur:
        _init_schemas(cur)
        _insert(cur, DB_SCHEMA, DB_TABLE, books_raw)
    print(f"{DB_SCHEMA}.{DB_TABLE}: {len(books_raw)} wierszy z {file_to_process}")

    processed_day = datetime.now().strftime("%Y-%m-%d")
    processed_dir = os.path.join(RAW_DATA_DIR, "processed", processed_day)
    os.makedirs(processed_dir, exist_ok=True)
    shutil.move(
        file_to_process, os.path.join(processed_dir, os.path.basename(file_to_process))
    )
    print(f"przeniesiono {os.path.basename(file_to_process)} → processed/{processed_day}/")

    return books_raw["scraped_at"].iloc[0]  # scraped_at tej sesji → XCom

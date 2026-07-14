"""Standup bazy — wykonuje sql/schema.sql (DDL poza pipeline'em, ang. deploy-time).

Uruchamiany RAZ przy stawianiu bazy lub po zmianie schematu — NIE w pipeline
(ingest.py zakłada, że tabela już istnieje). Reużywa connection_db() → jedno źródło
prawdy dla parametrów połączenia (ang. single source of truth), bez wymogu psql
w środowisku (ważne pod Docker/AWS).

    python scripts/init_db.py
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))  # uruchamiane jako skrypt — root nie jest na sys.path
load_dotenv(PROJECT_DIR / ".env", encoding="utf-8-sig")

from connection import connection_db  # noqa: E402 — po sys.path.insert i load_dotenv
from logger import setup_logger  # noqa: E402

logger = setup_logger("init_db")
SCHEMA_SQL = PROJECT_DIR / "sql" / "schema.sql"


def init_db(schema_sql: Path = SCHEMA_SQL) -> None:
    ddl = schema_sql.read_text(encoding="utf-8")  # brak pliku → FileNotFoundError (Fail Fast)
    with connection_db() as con, con.cursor() as cur:
        cur.execute(ddl)
    logger.info("zastosowano %s", schema_sql.name)


if __name__ == "__main__":
    init_db()

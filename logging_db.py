import os
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

from connection import connection_db

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"), encoding="utf-8-sig")

BACKLOG_BRANCH = "skip_fetch"


def _init_log_schema(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS logs")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs.pipeline_runs (
            run_id                SERIAL PRIMARY KEY,
            run_at                TIMESTAMP NOT NULL DEFAULT now(),
            dag_run_id            TEXT,
            dag_id                TEXT,
            status                TEXT,
            run_type              TEXT,
            scraped_count         INT,
            gold_inserted_count   INT,
            registry_new_count    INT,
            failed_task           TEXT,
            error_message         TEXT,
            duration_seconds      NUMERIC
        )
    """)


def gold_rows_affected(scraped_at: str) -> int:
    try:
        with connection_db() as con, con.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM public_marts.fct_books_history
                WHERE scraped_at = %s
            """,
                (scraped_at,),
            )
            count = cur.fetchone()[0]
        return count
    except Exception:
        traceback.print_exc()
        return 0


def registry_new_rows_count(scraped_at: str) -> int:
    try:
        with connection_db() as con, con.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT asin)
                FROM public_marts.fct_books_history
                WHERE scraped_at = %s
                  AND asin NOT IN (
                      SELECT asin FROM public_marts.fct_books_history
                      WHERE scraped_at < %s
                  )
            """,
                (scraped_at, scraped_at),
            )
            count = cur.fetchone()[0]
        return count
    except Exception:
        traceback.print_exc()
        return 0


def upsert_pipeline_run(
    dag_run_id: str,
    dag_id: str,
    status: str = None,
    run_type: str = None,
    scraped_count: int = None,
    gold_inserted_count: int = None,
    registry_new_count: int = None,
    failed_task: str = None,
    error_message: str = None,
    duration_seconds: float = None,
    overwrite_status: bool = True,
) -> None:
    """Jedyny writer do logs.pipeline_runs — UPSERT po dag_run_id.

    Wołane z trzech niezależnych miejsc w pipeline.py, w nieznanej z góry kolejności,
    z których każde zna tylko część danych:
    - log_run_task (task, zawsze ALL_DONE) — scraped_count/gold_inserted_count/
      registry_new_count/duration_seconds, status="pending_result" jako placeholder
      tylko na wypadek, gdyby wykonał się jako pierwszy (overwrite_status=False —
      nie ma nadpisać już ustalonego "failed"/"success", jeśli wykona się później).
    - task_failure_logger (per-task on_failure_callback) — failed_task i error_message
      z żywego context["exception"] (jedyne miejsce, gdzie ten obiekt istnieje —
      patrz error-reporting-theory.md, sekcja 6), status="failed" (overwrite_status=True).
    - dag_failure_alert / dag_success_alert (DAG-level callbacki) — tylko status,
      jako siatka bezpieczeństwa (overwrite_status=True).

    Pola poza status aktualizowane przez COALESCE(nowa_wartość, stara_wartość), żeby
    częściowe wywołanie nie zerowało danych zapisanych przez inny writer. status ma
    odwrotny kierunek COALESCE gdy overwrite_status=False — patrz wyżej.
    Pojedyncza, wspólna funkcja zamiast osobnego INSERT-u i osobnego UPDATE-u — dwie
    niezależne ścieżki zapisu nieuchronnie tworzyły dwa wiersze dla tego samego runu.
    """
    status_sql = (
        "status=COALESCE(%s, status)"
        if overwrite_status
        else "status=COALESCE(status, %s)"
    )
    with connection_db() as con, con.cursor() as cur:
        _init_log_schema(cur)
        cur.execute(
            f"""UPDATE logs.pipeline_runs
               SET {status_sql},
                   run_type=COALESCE(%s, run_type),
                   scraped_count=COALESCE(%s, scraped_count),
                   gold_inserted_count=COALESCE(%s, gold_inserted_count),
                   registry_new_count=COALESCE(%s, registry_new_count),
                   failed_task=COALESCE(%s, failed_task),
                   error_message=COALESCE(%s, error_message),
                   duration_seconds=COALESCE(%s, duration_seconds)
               WHERE dag_run_id=%s""",
            (
                status,
                run_type,
                scraped_count,
                gold_inserted_count,
                registry_new_count,
                failed_task,
                error_message,
                duration_seconds,
                dag_run_id,
            ),
        )
        if cur.rowcount == 0:
            cur.execute(
                """INSERT INTO logs.pipeline_runs
                   (dag_run_id, dag_id, status, run_type, scraped_count, gold_inserted_count,
                    registry_new_count, failed_task, error_message, duration_seconds)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    dag_run_id,
                    dag_id,
                    status,
                    run_type,
                    scraped_count,
                    gold_inserted_count,
                    registry_new_count,
                    failed_task,
                    error_message,
                    duration_seconds,
                ),
            )


def log_run_task(**context) -> None:
    ti = context["ti"]
    dag_run = context["dag_run"]

    branch = ti.xcom_pull(task_ids="check_pending_start")
    run_type = "backlog" if branch == BACKLOG_BRANCH else "normal"
    scraped_count = ti.xcom_pull(task_ids="fetch_book") or 0
    scraped_at = ti.xcom_pull(task_ids="ingest_books")
    gold_inserted_count = gold_rows_affected(scraped_at) if scraped_at else 0
    registry_new_count = registry_new_rows_count(scraped_at) if scraped_at else 0

    start = dag_run.start_date
    duration_seconds = (
        round((datetime.now(timezone.utc) - start).total_seconds(), 1)
        if start
        else None
    )

    upsert_pipeline_run(
        dag_run_id=dag_run.run_id,
        dag_id=dag_run.dag_id,
        status="pending_result",
        run_type=run_type,
        scraped_count=scraped_count,
        gold_inserted_count=gold_inserted_count,
        registry_new_count=registry_new_count,
        duration_seconds=duration_seconds,
        overwrite_status=False,
    )

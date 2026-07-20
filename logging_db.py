import os
import traceback
from datetime import datetime, timezone
from typing import Any

import psycopg
from dotenv import load_dotenv

from connection import connection_db
from logger import setup_logger

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(PROJECT_DIR, ".env"), encoding="utf-8-sig")

logger = setup_logger("Amazon_books_ETL_DB_connection")

BACKLOG_BRANCH = "skip_fetch"


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


def registry_new_rows_count(scraped_at: str) -> int | None:
    """Liczba ASIN-ów widzianych po raz pierwszy w sesji `scraped_at`.

    None oznacza „nie wiem" (gold jeszcze nie zbudowany) — w odróżnieniu od 0,
    które znaczy „wiem, że żadnych nowych". Ta różnica trafia do logs.pipeline_runs.
    """
    try:
        with connection_db() as con, con.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT h.asin)
                FROM public_marts.fct_books_history h
                WHERE h.scraped_at = %(scraped_at)s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM public_marts.fct_books_history prev
                      WHERE prev.asin = h.asin
                        AND prev.scraped_at < %(scraped_at)s
                  )
                """,
                {"scraped_at": scraped_at},
            )
            return cur.fetchone()[0]
    except psycopg.errors.UndefinedTable:
        logger.warning(
            "brak public_marts.fct_books_history — metryka registry_new dla sesji %s nieznana",
            scraped_at,
        )
        return None


def upsert_pipeline_run(
    dag_run_id: str,
    dag_id: str,
    status: str | None = None,
    run_type: str | None = None,
    scraped_count: int | None = None,
    gold_inserted_count: int | None = None,
    registry_new_count: int | None = None,
    failed_task: str | None = None,
    error_message: str | None = None,
    duration_seconds: float | None = None,
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
    # status ma dwa kierunki COALESCE (patrz overwrite_status w docstringu);
    # reszta pól zawsze COALESCE(nowa, stara), żeby częściowy zapis nie zerował
    # danych innego writera. EXCLUDED = wiersz, który próbowaliśmy wstawić.
    status_update = (
        "COALESCE(EXCLUDED.status, logs.pipeline_runs.status)"
        if overwrite_status
        else "COALESCE(logs.pipeline_runs.status, EXCLUDED.status)"
    )
    # Atomowy UPSERT jednym zapytaniem — check-then-act (UPDATE, potem INSERT gdy
    # rowcount=0) miał race: dwa równoległe callbacki widziały brak wiersza i oba
    # robiły INSERT → dwa wiersze na jeden dag_run. ON CONFLICT tego nie dopuszcza.
    with connection_db() as con, con.cursor() as cur:
        cur.execute(
            f"""INSERT INTO logs.pipeline_runs
                   (dag_run_id, dag_id, status, run_type, scraped_count, gold_inserted_count,
                    registry_new_count, failed_task, error_message, duration_seconds)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (dag_run_id) DO UPDATE SET
                   status={status_update},
                   run_type=COALESCE(EXCLUDED.run_type, logs.pipeline_runs.run_type),
                   scraped_count=COALESCE(EXCLUDED.scraped_count, logs.pipeline_runs.scraped_count),
                   gold_inserted_count=COALESCE(EXCLUDED.gold_inserted_count, logs.pipeline_runs.gold_inserted_count),
                   registry_new_count=COALESCE(EXCLUDED.registry_new_count, logs.pipeline_runs.registry_new_count),
                   failed_task=COALESCE(EXCLUDED.failed_task, logs.pipeline_runs.failed_task),
                   error_message=COALESCE(EXCLUDED.error_message, logs.pipeline_runs.error_message),
                   duration_seconds=COALESCE(EXCLUDED.duration_seconds, logs.pipeline_runs.duration_seconds)""",
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


def log_run_task(**context: Any) -> None:
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


def append_run_summary(dag_run_id: str) -> None:
    """Dopisuje podsumowanie runu do logs/pipeline_runs.log (trwały rejestr na dysku).

    Źródłem jest gotowy wiersz z logs.pipeline_runs (SSOT) — dokładnie te dane, które trafiają
    do tabeli i do Slacka. Nic nie liczymy ponownie; plik to tylko dodatkowy sink. Wołane z
    callbacków sukcesu i porażki, po sfinalizowaniu `status` w tabeli.
    """
    with connection_db() as con, con.cursor() as cur:
        cur.execute(
            """SELECT status, run_type, scraped_count, gold_inserted_count,
                      registry_new_count, duration_seconds, failed_task, error_message
               FROM logs.pipeline_runs WHERE dag_run_id = %s""",
            (dag_run_id,),
        )
        row = cur.fetchone()

    if row is None:
        return

    status, run_type, scraped, gold, registry, duration, failed_task, error = row
    summary = (
        f"run={dag_run_id} | status={status} | type={run_type} | "
        f"scraped={scraped} | gold={gold} | registry={registry} | duration={duration}s"
    )
    if failed_task:
        summary += f" | failed_task={failed_task} | error={error}"

    # logger o nazwie "pipeline_runs" → linia trafia do wspólnego dziennego etl_<data>.log
    # (obok szczegółów przebiegu); grep "| pipeline_runs |" wyciąga same podsumowania.
    setup_logger("pipeline_runs").info(summary)

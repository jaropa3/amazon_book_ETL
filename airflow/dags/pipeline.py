import os
import sys
from datetime import datetime, timedelta

PROJECT_DIR = "/home/mycka/projects/amazon_books_ETL"
sys.path.insert(0, PROJECT_DIR)
os.environ.setdefault("AIRFLOW_HOME", os.path.join(PROJECT_DIR, "airflow"))

DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_project"
DBT_BIN = f"{PROJECT_DIR}/.venv/bin/dbt"
DBT_OPTS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir ~/.dbt"

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.sdk.definitions.context import Context
from main import fetch_books
from ingest import ingest_books, RAW_DATA_DIR, DB_TABLE
from connection import connection_db
from logging_db import (
    log_run_task,
    upsert_pipeline_run,
    append_run_summary,
    BACKLOG_BRANCH,
)


def _has_pending_files() -> bool:
    return bool(
        [
            f
            for f in os.listdir(RAW_DATA_DIR)
            if f.startswith(f"{DB_TABLE}_") and f.endswith(".csv")
        ]
    )


def _branch_start() -> str:
    return BACKLOG_BRANCH if _has_pending_files() else "fetch_book"


def task_failure_logger(context: Context) -> None:
    """Per-task on_failure_callback (przez default_args — działa dla każdego taska).

    W przeciwieństwie do DAG-level on_failure_callback, ten uruchamia się w tym samym
    procesie co failujący task, zaraz po błędzie — to jedyne miejsce, gdzie context
    zawiera żywy obiekt wyjątku (context["exception"]). DAG-level callback wykonuje się
    w osobnym procesie (DAG file processor) z odtworzonym kontekstem bez tego pola.
    """
    exception = context.get("exception")
    upsert_pipeline_run(
        context["dag_run"].run_id,
        context["dag_run"].dag_id,
        status="failed",
        failed_task=context["ti"].task_id,
        error_message=str(exception) if exception else None,
    )


DEF_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=3),
    "on_failure_callback": task_failure_logger,
}


def dag_failure_alert(context: Context) -> None:
    dag_run_id = context["dag_run"].run_id
    SlackWebhookNotifier(
        slack_webhook_conn_id="slack_webhook_default",
        text=(
            f":red_circle: DAG `amazon_books_pipeline` nie powiódł się\n"
            f"Run: `{dag_run_id}`"
        ),
    )(context)
    upsert_pipeline_run(
        context["dag_run"].run_id,
        context["dag_run"].dag_id,
        status="failed",
    )
    append_run_summary(dag_run_id)


def dag_success_alert(context: Context) -> None:
    dag_run_id = context["dag_run"].run_id
    with connection_db() as con, con.cursor() as cur:
        cur.execute(
            """
            SELECT scraped_count, gold_inserted_count, registry_new_count
            FROM logs.pipeline_runs WHERE dag_run_id = %s
        """,
            (dag_run_id,),
        )
        row = cur.fetchone()
    scraped, gold, registry = row or (0, 0, 0)

    SlackWebhookNotifier(
        slack_webhook_conn_id="slack_webhook_default",
        text=(
            ":large_green_circle: DAG `amazon_books_pipeline` zakończony sukcesem\n"
            f"Run: `{dag_run_id}`\n"
            f"zescrapowano: {scraped} | dodano do gold: {gold} | nowe w rejestrze: {registry}"
        ),
    )(context)
    upsert_pipeline_run(dag_run_id, context["dag_run"].dag_id, status="success")
    append_run_summary(dag_run_id)


with (
    DAG(
        dag_id="amazon_books_pipeline",
        default_args=DEF_ARGS,
        description="fetch and store amazon books info",
        start_date=datetime(2024, 1, 1),
        schedule="*/30 * * * *",
        catchup=False,
        max_active_runs=1,
        on_failure_callback=dag_failure_alert,  # odpala się raz, gdy cały DAG run zakończy się porażką
        is_paused_upon_creation=True,
        max_active_tasks=2,
    ) as dag
):
    check_pending_start = BranchPythonOperator(
        task_id="check_pending_start",
        python_callable=_branch_start,
    )

    skip_fetch = EmptyOperator(task_id=BACKLOG_BRANCH)

    fetch_from_API = PythonOperator(
        task_id="fetch_book",
        python_callable=fetch_books,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_books",
        python_callable=ingest_books,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"{DBT_BIN} run {DBT_OPTS} --select staging",
    )  # wykona /home/mycka/projects/amazon_books_ETL/.venv/bin/dbt run --project-dir /home/mycka/projects/amazon_books_ETL/dbt_project --profiles-dir ~/.dbt --select staging
    # .venv/bin/dbt — pełna ścieżka do dbt z virtualenv. Nie używamy dbt z PATH bo Airflow uruchamia bash bez aktywowanego venv — musimy wskazać dokładnie gdzie jest binarka.
    # --project-dir — gdzie szukać dbt_project.yml, modeli, testów. Bez tego dbt szukałby w katalogu bieżącym, który w kontekście Airflow jest losowy.
    # --profiles-dir ~/.dbt — gdzie szukać profiles.yml z danymi do połączenia z bazą (host, port, user, password). Plik jest poza repozytorium bo ma credentials.
    # --select staging — uruchom tylko modele z folderu staging/. Bez tego dbt budowałby wszystkie modele naraz.

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"{DBT_BIN} run {DBT_OPTS} --select int_books",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test {DBT_OPTS}",
    )

    dbt_fct_books_history = BashOperator(
        task_id="dbt_fct_books_history",
        bash_command=f"{DBT_BIN} run {DBT_OPTS} --select fct_books_history",
    )

    dbt_books_registry = BashOperator(
        task_id="dbt_books_registry",
        bash_command=f"{DBT_BIN} run {DBT_OPTS} --select books_registry",
    )

    dbt_rejected_books = BashOperator(
        task_id="dbt_rejected_books",
        bash_command=f"{DBT_BIN} run {DBT_OPTS} --select rejected_books",
    )

    log_run = PythonOperator(
        task_id="log_run",
        python_callable=log_run_task,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # "Watcher task" (standardowy wzorzec Airflow): log_run ma trigger_rule=ALL_DONE i jest jedynym
    # innym liściem grafu, więc bez tego tasku Airflow zawsze oceniałby cały DAG run jako "success".
    # pipeline_succeeded celowo wymaga ALL_SUCCESS na realnych taskach biznesowych — staje się drugim
    # liściem, który faktycznie odzwierciedla porażkę (upstream_failed), gdy coś wcześniej padnie.
    pipeline_succeeded = EmptyOperator(
        task_id="pipeline_succeeded",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=dag_success_alert,
    )

    check_pending_files = ShortCircuitOperator(
        task_id="check_pending_files",
        python_callable=_has_pending_files,
    )

    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="amazon_books_pipeline",
        wait_for_completion=False,
    )

    check_pending_start >> [fetch_from_API, skip_fetch]
    [fetch_from_API, skip_fetch] >> ingest_bronze
    ingest_bronze >> dbt_staging >> [dbt_intermediate, dbt_rejected_books]
    (
        dbt_intermediate
        >> dbt_test
        >> dbt_fct_books_history
        >> dbt_books_registry
        >> log_run
    )
    dbt_rejected_books >> log_run
    [dbt_books_registry, dbt_rejected_books] >> pipeline_succeeded
    log_run >> pipeline_succeeded
    pipeline_succeeded >> check_pending_files >> trigger_next_run

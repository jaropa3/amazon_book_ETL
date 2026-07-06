"""Test integralności DAG-a — łapie błędy importu/składni i literówki w task_id
zanim zobaczy je scheduler. Wymaga Airflow (jest w requirements-dev.txt)."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_FOLDER = str(Path(__file__).resolve().parents[1] / "airflow" / "dags")
DAG_ID = "amazon_books_pipeline"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_laduje_sie_bez_bledow(dagbag):
    assert dagbag.import_errors == {}


def test_dag_istnieje(dagbag):
    assert DAG_ID in dagbag.dags


def test_dag_ma_kluczowe_taski(dagbag):
    task_ids = {t.task_id for t in dagbag.dags[DAG_ID].tasks}
    oczekiwane = {"check_pending_start", "fetch_book", "ingest_books", "log_run"}
    assert oczekiwane <= task_ids

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All Python commands use the project virtualenv:

```bash
source .venv/bin/activate
```

**Run full pipeline (scrape → ingest → ...):**
```bash
python main.py
```

**dbt — run all layers:**
```bash
dbt --project-dir dbt_project --profiles-dir ~/.dbt run
dbt --project-dir dbt_project --profiles-dir ~/.dbt test
```

**dbt — single layer:**
```bash
dbt --project-dir dbt_project --profiles-dir ~/.dbt run --select staging
dbt --project-dir dbt_project --profiles-dir ~/.dbt run --select intermediate
dbt --project-dir dbt_project --profiles-dir ~/.dbt run --select gold
```

**dbt — pełne przebudowanie modelu incremental (np. po zmianie schematu gold):**
```bash
dbt --project-dir dbt_project --profiles-dir ~/.dbt run --full-refresh --select fct_books_history
```

**Airflow (uruchomienie lokalne):**
```bash
export AIRFLOW_HOME=~/projects/amazon_books_ETL/airflow
airflow db migrate
airflow webserver -p 8080 &
airflow scheduler &
```

## Architecture

Pipeline jest podzielony na warstwy. Bronze zawiera dane z **jednej sesji** (TRUNCATE przed każdym ingestem). Historia kumuluje się w gold i w plikach CSV.

```
Amazon.com → main.py (scraper) → data/raw_data/books_YYYYMMDD_HHMMSS.csv
                                        ↓
                    ingest.py → bronze.books (TRUNCATE+insert, FIFO — najstarszy plik)
                             → data/raw_data/processed/ (plik po udanym commicie)
                                        ↓
                         dbt staging → stg_books (typuje kolumny, NULL handling)
                                        ↓
                     dbt intermediate → int_books (deduplicates per asin+scraped_at session)
                                        ↓
                              dbt test (testy jakości przed gold)
                                        ↓
                            dbt gold → fct_books_history (incremental, unique_key=book_sk)
                                        ↓
                         logging_db.py → logs.pipeline_runs
```

### Kluczowe decyzje architektoniczne

- **bronze jest TRUNCATE'owany** przy każdym ingecie — zawiera tylko dane z jednej sesji. Historia jest tylko w gold i w plikach CSV.
- **Late-arriving data (FIFO + self-trigger):** `ingest_books()` bierze zawsze najstarszy plik z `raw_data/` i przenosi go do `raw_data/processed/` po commicie. DAG sprawdza na początku (`check_pending_start`, `BranchPythonOperator`) czy są zaległe pliki — jeśli tak, pomija scraping i przetwarza tylko backlog. Na końcu (`check_pending_files`, `ShortCircuitOperator`) triggeruje kolejny run jeśli zostały pliki.
- **int_books deduplicuje po `(asin, scraped_at)`** — usuwa duplikaty z tej samej sesji, preferując wiersz z kompletniejszymi danymi. Unikalność `asin` per sesja jest testowana przez singular test.
- **gold jest incremental** z `unique_key='book_sk'`, gdzie `book_sk = md5(asin || '_' || scraped_at::text)`. Kumuluje każdą sesję scrapowania. Brak filtra `WHERE scraped_at > MAX(scraped_at)` — celowa decyzja chroniąca late-arriving data.
- **staging i intermediate są widokami** (`materialized: view`) — nie ma kosztownych tabel pośrednich.
- **Wszystkie kolumny w bronze są TEXT** — dbt staging robi typowanie przez `::numeric`, `::timestamp`.
- **Scraper szuka autora** przez `href` zawierający `/e/ASIN` — nie przez klasę CSS (która łapała też "Paperback", "Kindle Edition" itp.).
- **`run_type` w logach** — `logs.pipeline_runs.run_type` = `'normal'` (scrape + ingest) lub `'backlog'` (tylko ingest zaległego pliku). Odczytywany z XCom `check_pending_start`. Porównanie odbywa się przez stałą `BACKLOG_BRANCH = "skip_fetch"` zdefiniowaną w `logging_db.py` i importowaną w `pipeline.py` (task_id `skip_fetch` EmptyOperatora) — zmiana nazwy gałęzi wymaga edycji jednego miejsca.

### Pliki konfiguracyjne

- `config.yaml` — parametry scrapera (URL, keyword, liczba stron, retry), ścieżki, schemat/tabela w DB. Ładowany przez `config.py` jako dict `CONFIG`.
- `.env` — dane do połączenia z PostgreSQL (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`). Baza działa w Dockerze, host `host.docker.internal`.
- `~/.dbt/profiles.yml` — profil dbt (poza repozytorium).

### Airflow DAG

`airflow/dags/pipeline.py` — DAG `amazon_books_pipeline`, uruchamiany co 30 minut (`*/30 * * * *`). Ścieżka do dbt i projektu jest hardcodowana jako `/home/mycka/projects/amazon_books_ETL`. DAG używa `.venv/bin/dbt` przez BashOperator.

Przepływ XCom:
- `fetch_book` → `log_run` (scraped_count)
- `ingest_books` → `log_run` (scraped_at przetworzonej sesji)
- `check_pending_start` → `log_run` (branch: `'fetch_book'` lub `'skip_fetch'` → run_type)

Kluczowe taski dodatkowe:
- `check_pending_start` (`BranchPythonOperator`) — sprawdza `raw_data/` na początku; wybiera gałąź `fetch_book` (brak zaległych) lub `skip_fetch` (jest backlog)
- `skip_fetch` (`EmptyOperator`) — pusta gałąź gdy backlog; `ingest_books` ma `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS`
- `check_pending_files` (`ShortCircuitOperator`) — sprawdza `raw_data/` na końcu; short-circuits gdy brak plików
- `trigger_next_run` (`TriggerDagRunOperator`) — odpala kolejny run gdy są zaległe pliki

Callbacki alertów:
- `task_failure_logger` (per-task `on_failure_callback` przez `default_args`) — jedyne miejsce z żywym `context["exception"]` i `context["ti"]`; zapisuje `failed_task` i `error_message`
- `dag_failure_alert` (DAG-level `on_failure_callback`) — **nie ma** `context["ti"]`; tylko wysyła Slack i ustawia `status="failed"` przez UPSERT; nie powtarza `failed_task` (już zapisany przez `task_failure_logger`)
- `dag_success_alert` (`on_success_callback` na `pipeline_succeeded`) — buduje notifier inline z f-stringiem (nie Jinja template); odpytuje DB po statystyki

### Testy dbt

Testy są w dwóch miejscach:
- `dbt_project/models/*/schema.yml` — standardowe testy (not_null, unique)
- `dbt_project/tests/` — singular testy SQL (zwracają wiersze naruszające regułę):
  - `assert_gold_price_positive.sql`, `assert_gold_rating_max_5.sql`
  - `assert_int_books_unique_per_session.sql` — unikalność `(asin, scraped_at)` w `int_books`

### Zasady
- jest projekt do nauki. zawsze zapytaj zanim coś faktycznie zmienisz w kodzie.

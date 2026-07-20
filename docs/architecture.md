# Architektura projektu

```mermaid
flowchart TD
    subgraph SRC["Źródło danych"]
        Amazon["Amazon.com\n(wyszukiwanie: 'data engineering')"]
    end

    subgraph LANDING["Landing zone"]
        RAW["data/raw_data/\nbooks_YYYYMMDD_HHMMSS.csv"]
        PROC["data/raw_data/processed/\n(po udanym ingeście)"]
    end

    subgraph ING["Ingestion — Python"]
        MAIN["main.py\nscrape_to_csv()\n= fetch_books() + save_books_to_csv()"]
        INGEST["ingest.py\ningest_books()\n_validate_raw() przed DB\nFIFO — najstarszy plik\nzwraca scraped_at → XCom"]
    end

    subgraph PG["PostgreSQL — DDL w sql/schema.sql (scripts/init_db.py, poza pipeline'em)"]
        BRONZE[(bronze.books\nTEXT, TRUNCATE+COPY)]
        STAGING[(public_staging.stg_books\nwidok)]
        INTERMEDIATE[(public_intermediate.int_books\nwidok)]
        REJECTED[(public_intermediate.rejected_books\nincremental — AKUMULUJE\nunique_key=rejected_sk)]
        FCT[(public_marts.fct_books_history\nincremental)]
        REGISTRY[(public_marts.books_registry\ntabela)]
        LOGS[(logs.pipeline_runs\ndag_run_id UNIQUE\nrun_type: normal/backlog)]
    end

    subgraph DBT["dbt_project/models/"]
        STG["staging/stg_books\nTypowanie kolumn\nfiltr asin not null"]
        INT["intermediate/int_books\nDeduplicacja per\n(asin, scraped_at)"]
        REJ["intermediate/rejected_books\nincremental — akumuluje historię\nbrak asin + duplikaty sesji"]
        FCT_M["marts/fct_books_history\nincremental\nunique_key = book_sk\n= md5(asin + scraped_at)"]
        REG["marts/books_registry\nDISTINCT asin+title+author"]
    end

    subgraph TESTS["dbt testy"]
        SCHEMA_TESTS["schema.yml\nnot_null, unique"]
        SINGULAR["tests/\nassert_gold_price_positive.sql\nassert_gold_rating_max_5.sql\nassert_int_books_unique_per_session.sql"]
    end

    subgraph ORCH["Airflow — DAG: amazon_books_pipeline\nschedule: co 30 minut"]
        CHECK_START[BranchPythonOperator\ncheck_pending_start\nsprawdza raw_data/ przed scrapem]
        SKIP[EmptyOperator\nskip_fetch\ngałąź gdy backlog]
        FETCH[PythonOperator\nfetch_book]
        INGEST_OP[PythonOperator\ningest_books\nNONE_FAILED_MIN_ONE_SUCCESS]
        DBT_STG[BashOperator\ndbt_staging]
        DBT_INT[BashOperator\ndbt_intermediate\nint_books]
        DBT_REJ[BashOperator\ndbt_rejected_books]
        DBT_TST[BashOperator\ndbt_test]
        DBT_FCT[BashOperator\ndbt_fct_books_history]
        DBT_REG[BashOperator\ndbt_books_registry]
        LOG_RUN[PythonOperator\nlog_run\ntrigger_rule=ALL_DONE]
        WATCHER[EmptyOperator\npipeline_succeeded\ntrigger_rule=ALL_SUCCESS\nwatcher task]
        CHECK_END[ShortCircuitOperator\ncheck_pending_files\nsprawdza raw_data/ po pipeline]
        TRIGGER[TriggerDagRunOperator\ntrigger_next_run\nwait_for_completion=False]
    end

    subgraph ALERT["Alerty — Slack (Incoming Webhook)"]
        FAIL_CB["on_failure_callback\n(poziom DAG-a)"]
        SUCC_CB["on_success_callback\n(na pipeline_succeeded)\nze statystykami z DB"]
    end

    Amazon --> FETCH
    CHECK_START -->|brak zaległych| FETCH
    CHECK_START -->|jest backlog| SKIP
    FETCH --> MAIN --> RAW
    SKIP --> INGEST_OP
    FETCH --> INGEST_OP
    INGEST_OP --> INGEST --> BRONZE
    RAW -->|FIFO| INGEST
    INGEST --> PROC

    BRONZE --> DBT_STG
    DBT_STG --> STG --> STAGING

    STAGING --> DBT_INT & DBT_REJ
    DBT_INT --> INT --> INTERMEDIATE
    DBT_REJ --> REJ --> REJECTED

    INTERMEDIATE --> DBT_TST
    DBT_TST --> SCHEMA_TESTS & SINGULAR

    SCHEMA_TESTS & SINGULAR --> DBT_FCT
    DBT_FCT --> FCT_M --> FCT
    FCT --> DBT_REG --> REG --> REGISTRY

    DBT_REG --> LOG_RUN
    DBT_REJ --> LOG_RUN
    LOG_RUN --> LOGS
    LOG_RUN --> WATCHER

    DBT_REG --> WATCHER
    DBT_REJ --> WATCHER
    WATCHER --> CHECK_END --> TRIGGER
    WATCHER -. success .-> SUCC_CB
    SUCC_CB -. update status .-> LOGS
    ORCH -. dag run failed .-> FAIL_CB
    FAIL_CB -. update status .-> LOGS
```

## Przepływ danych

1. **Sprawdzenie backlogu** — `check_pending_start` (`BranchPythonOperator`) sprawdza `data/raw_data/` przed scrapingiem. Jeśli są zaległe pliki (late-arriving data) → gałąź `skip_fetch`, scraping pominięty. Jeśli nie ma → gałąź `fetch_book`, normalny flow.

2. **Scraping** — task `fetch_book` wywołuje `scrape_to_csv()` z `main.py`. Funkcja jest **brzegiem systemu** (ang. *edge*): składa `fetch_books()` (pobiera i parsuje, **bez I/O** — czysta i testowalna) z `save_books_to_csv()` (jedyny efekt uboczny). Scraper odpytuje Amazon.com (keyword: `data engineering`, domyślnie 5 stron), rotuje User-Agenty, obsługuje retry przy challenge page / HTTP 429 / 503 (z poszanowaniem `Retry-After`). Zero książek → `RuntimeError` (Fail Fast), nie pusty CSV. Zwraca liczbę wierszy przez XCom. `scraped_at` powstaje **raz na sesję** jako `datetime.now(timezone.utc)` — wszystkie wiersze sesji dzielą ten sam znacznik, co jest warunkiem dedupu w `int_books`. Autor scrapowany przez href z `/e/ASIN`.

3. **Bronze** — `ingest_books` bierze **najstarszy** plik z `data/raw_data/` (FIFO). Najpierw `_validate_raw()` sprawdza plik (niepusty, kolumna `scraped_at` obecna i niepusta) — **przed** dotknięciem bazy (Fail Fast). Potem ładuje do `bronze.books` przez `TRUNCATE + COPY FROM STDIN` (bulk load jednym strumieniem, nie `INSERT` po wierszu). Wszystkie kolumny są tekstowe (`TEXT`). Po udanym commicie plik trafia do `data/raw_data/processed/<data UTC>/`. Zwraca `scraped_at` przetworzonej sesji przez XCom.

4. **Staging** (`stg_books`, widok) — typuje kolumny: `price::numeric`, `rating::numeric`, `scraped_at::timestamptz`. Czas jest w **UTC end-to-end** — od `datetime.now(timezone.utc)` w scraperze po `TIMESTAMPTZ` w bazie. Filtruje wiersze bez `asin`.

5. **Intermediate** — dwa modele uruchamiane równolegle po staging:
   - `int_books` (widok) — deduplikuje po `(asin, scraped_at)` przez `ROW_NUMBER()`. Gdy ten sam ASIN pojawia się wielokrotnie w jednej sesji, zostaje wiersz z kompletniejszymi danymi (`price` i `rating` not null mają priorytet).
   - `rejected_books` (**incremental**, `unique_key=rejected_sk`) — rejestr odrzuconych rekordów, **akumulujący historię wszystkich sesji**: brak `asin` w bronze + duplikaty sesji odfiltrowane przez `int_books`. `rejected_sk` to hash treści wiersza → ponowne przetworzenie tego samego pliku nie dubluje wpisów (idempotencja).

6. **Testy dbt** — uruchamiane po `int_books`, przed marts:
   - `schema.yml`: `book_sk` (unique, not_null), `asin` (not_null), `scraped_at` (not_null), `title` (not_null)
   - Singular testy: `price > 0`, `rating ∈ [0, 5]`, `(asin, scraped_at)` unique w `int_books`

7. **Marts** — dwa modele sekwencyjne:
   - `fct_books_history` (incremental) — kumuluje historię wszystkich sesji. `unique_key = book_sk = md5(asin || '_' || scraped_at)`. Każda para `(asin, scraped_at)` trafia do tabeli tylko raz. Brak filtra `WHERE scraped_at > MAX(scraped_at)` — chroni late-arriving data.
   - `books_registry` (tabela) — unikalny rejestr książek: `DISTINCT asin + title + author` z całego `fct_books_history`.

8. **Logi** — `log_run` uruchamia się zawsze (`trigger_rule=ALL_DONE`). Zapisuje do `logs.pipeline_runs` przez **atomowy UPSERT** (`INSERT … ON CONFLICT (dag_run_id) DO UPDATE`):
   - `run_type` — `'normal'` lub `'backlog'` (z XCom `check_pending_start`)
   - `scraped_count` — z XCom `fetch_book` (0 przy run_type=backlog)
   - `gold_inserted_count`, `registry_new_count` — liczone dla `scraped_at` z XCom `ingest_books`
   - `duration_seconds`, `status="pending_result"` (placeholder)

9. **Wykrywanie wyniku i alerty** — `pipeline_succeeded` (`EmptyOperator`, `trigger_rule=ALL_SUCCESS`, zależny od `log_run`) kończy się sukcesem tylko gdy realne taski biznesowe przeszły (watcher task pattern). `on_success_callback` odpytuje `logs.pipeline_runs` i wysyła Slack ze statystykami. `on_failure_callback` (DAG-level) wysyła alert o porażce.

10. **Kolejny run** — `check_pending_files` (`ShortCircuitOperator`) sprawdza czy w `data/raw_data/` zostały pliki. Jeśli tak — `trigger_next_run` (`TriggerDagRunOperator`) odpala kolejny run natychmiast bez czekania na harmonogram. `max_active_runs=1` zapobiega równoległemu uruchomieniu.

## ⚠️ Pułapka: `--full-refresh` na `fct_books_history` kasuje całą historię

```bash
# NIE RÓB TEGO:
dbt run --full-refresh --select fct_books_history
```

`fct_books_history` jest incremental i czyta z `int_books` ← `bronze`, a **bronze jest
TRUNCATE'owany co run** (jedna sesja). `--full-refresh` = `DROP` + odbudowa z tego, co
akurat widać, czyli **z jednej bieżącej sesji** → cała skumulowana historia gold znika.

Odzyskanie jest możliwe tylko przez ręczny replay wszystkich plików z `data/raw_data/processed/`
przez FIFO — dlatego warstwa CSV jest niemutowalnym źródłem prawdy (ang. *immutable raw layer*),
a nie plikiem tymczasowym.

**Zamiast tego:** zmiana schematu gold → dopisz kolumnę migracją albo przebuduj przez
kontrolowany replay. `--full-refresh` jest bezpieczny na `rejected_books` i `books_registry`
(oba dają się odtworzyć), ale **nigdy** na `fct_books_history`.

## Kluczowe decyzje architektoniczne

| Decyzja | Uzasadnienie |
|---|---|
| Bronze = TRUNCATE | Staging i intermediate zawsze widzą tylko bieżący scrape — bez historycznych śmieci |
| FIFO + self-trigger dla late data | Jeden run = jedna sesja = czyste statystyki; backlog oczyszczany bez czekania na harmonogram |
| `check_pending_start` przed scrapem | Scraping pomijany gdy jest backlog — bez tego każdy run tworzy nowy plik i kolejka nigdy nie maleje |
| Staging i int_books jako widoki | Brak kosztownych tabel pośrednich; dane materializują się tylko w marts |
| Surrogate key `book_sk = md5(asin + scraped_at)` | Jeden klucz zamiast composite key; łatwiejsze joiny |
| Brak filtra `is_incremental()` w gold | Filtr `WHERE scraped_at > MAX(scraped_at)` wykluczałby late-arriving data — celowa decyzja |
| `run_type` w logach | Odróżnia runy normalne od backlogowych — bez tego `scraped_count=0` wygląda jak błąd |
| `BACKLOG_BRANCH` stała w `logging_db.py` | Task_id gałęzi backlogowej w jednym miejscu — `pipeline.py` importuje stałą zamiast powielać string `"skip_fetch"` |
| `dag_failure_alert` bez `context["ti"]` | DAG-level callback nie ma `TaskInstance` w kontekście; `failed_task` zapisuje tylko `task_failure_logger` (per-task callback, jedyne miejsce z żywym wyjątkiem) |
| Slack alert budowany inline z f-stringiem | Jinja template `{{ run_id }}` może nie być renderowany w kontekście callbacka — f-string z `context["dag_run"].run_id` jest zawsze bezpieczny |
| `ingest_books` zwraca `scraped_at` przez XCom | `log_run` odpytuje gold dla konkretnej sesji, nie dla `MAX(scraped_at)` z bronze (które może być już nadpisane) |
| `rejected_books` w intermediate | Czyta tylko z bronze i stg_books — nie zależy od marts |
| `rejected_books` jako **incremental**, nie `table` | Bronze trzyma tylko bieżącą sesję, więc `table` (przebudowywana co run) kasowałaby odrzuty poprzednich runów — „rejestr" bez historii. `unique_key=rejected_sk` (hash treści wiersza) daje idempotentny merge. Brak filtra po `scraped_at` — świadomie, jak w gold: filtr `> MAX(scraped_at)` gubiłby late-arriving data |
| `books_registry` jako tabela (nie incremental) | Zawsze przebudowywana — odzwierciedla aktualny stan `fct_books_history` |
| `trigger_rule=ALL_DONE` na log_run | Logi zawsze zapisywane — również przy błędach pipeline'u |
| `pipeline_succeeded` jako watcher task (`ALL_SUCCESS`) | `log_run` (`ALL_DONE`) jako jedyny liść grafu maskowałby każdą porażkę |
| `log_run >> pipeline_succeeded` | `dag_success_alert` odpytuje DB po statystyki — musi się wykonać po commicie `log_run` |
| `status` w `pipeline_runs` ustawiany przez callback, nie przez `log_run` | Airflow 3 blokuje bezpośredni dostęp do bazy metadanych z poziomu taska |
| UPSERT w `upsert_pipeline_run` | `log_run` i callback to równoległe ścieżki — kolejność nie jest gwarantowana |
| **Atomowy** UPSERT: `INSERT … ON CONFLICT (dag_run_id) DO UPDATE` | Poprzednie „UPDATE, a jak `rowcount=0` to INSERT" to *check-then-act race*: przy `max_active_tasks=2` dwa równoległe callbacki widziały brak wiersza i **oba** robiły INSERT → dwa wiersze na jeden run → `fetchone()` w alertach brał losowy z nich. Wymaga `UNIQUE (dag_run_id)` w `sql/schema.sql` |
| `TRUNCATE` + `COPY FROM STDIN` zamiast `INSERT` po wierszu | Bulk load jednym strumieniem — dużo szybszy; `write_row` sam robi adaptację typów i `None → NULL`. Bronze nie ma indeksów ani constraintów, więc COPY jest bezpieczny |
| Czas w **UTC** end-to-end (`TIMESTAMPTZ`) | `scraped_at` powstaje jako `datetime.now(timezone.utc)` → `isoformat()` z offsetem `+00:00` → `::timestamptz` w dbt. Bez tego dev (Europe/Warsaw) i chmura (UTC) produkowałyby niespójne znaczniki tej samej sesji |
| `_validate_raw()` **przed** połączeniem z bazą | Bez tego pusty/uszkodzony CSV najpierw TRUNCATE'ował bronze i lądował w `processed/`, a task padał dopiero na `.iloc[0]` — czyli bronze wyczyszczony, plik „skonsumowany", sesja stracona. Guard clause na wejściu = Fail Fast |
| `fetch_books()` bez I/O, zapis w `scrape_to_csv()` | Single Responsibility: pobieranie jest czyste i deterministyczne (testowalne bez dysku), efekt uboczny żyje na brzegu systemu. Dzięki temu `fetch_books` w ogóle dało się pokryć testami |
| `logger.propagate = False` | Pod Airflow root logger ma własne handlery — bez tego każdy rekord byłby emitowany dwa razy (double logging) |
| DDL w `sql/schema.sql`, uruchamiane przez `scripts/init_db.py` | Schemat żyje w migracjach, nie w pipelinie. `CREATE TABLE IF NOT EXISTS` w każdym runie ukrywał rozjazd kontraktu z dbt; teraz nieznana kolumna → Fail Fast |
| Jeden wspólny dziennik: `logs/etl_<data>.log` | Pełny przebieg pipeline'u (retry/error/„zapisano N"/ingest) **oraz** podsumowanie runu trafiają na stdout i do **jednego** dziennego pliku (append) — świadomie zrezygnowano z osobnego `pipeline_runs.log`, żeby całość runu była w jednym miejscu. Podsumowanie loguje logger o nazwie `pipeline_runs` (grep `\| pipeline_runs \|` wyciąga same podsumowania); dane bierze z `append_run_summary()`, który czyta **gotowy wiersz z `logs.pipeline_runs`** (SSOT) — nic nie liczy ponownie, wołany z obu callbacków po sfinalizowaniu `status`. Czytelny, ciągły zapis na dysku, w przeciwieństwie do logów Airflow rozbitych per task w JSON (`airflow/logs/.../task_id=.../attempt=N.log`). Kluczowe: **stabilna nazwa dzienna + `delay=True`** eliminują dawną eksplozję pustych plików (poprzednio `now()` w nazwie + eager open tworzyły dziesiątki plików przy parsowaniu DAG-a co ~30 s). Świadome powielenie logów Airflow w zamian za wygodę przeglądania; pod AWS (MWAA/ECS) stdout zbierze CloudWatch bez zmian w kodzie |

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
dbt run --project-dir dbt_project --profiles-dir ~/.dbt
dbt test --project-dir dbt_project --profiles-dir ~/.dbt
```

**dbt — single layer:**
```bash
dbt run --project-dir dbt_project --profiles-dir ~/.dbt --select staging
dbt run --project-dir dbt_project --profiles-dir ~/.dbt --select intermediate
dbt run --project-dir dbt_project --profiles-dir ~/.dbt --select marts
```

**⚠️ NIE rób `--full-refresh` na `fct_books_history`.** To model incremental czytający
z `int_books` ← `bronze`, a bronze jest TRUNCATE'owany co run (jedna sesja). `--full-refresh`
= `DROP` + odbudowa z bieżącej sesji → **cała historia gold ginie** (nieodwracalne poza
ręcznym replayem wszystkich CSV z `processed/`). Zmiana schematu gold = dopisz kolumnę
migracją albo przebuduj przez kontrolowany replay, nie przez `--full-refresh`.

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
- **Wszystkie kolumny w bronze są TEXT** — dbt staging robi typowanie przez `::numeric`, `::timestamptz` (czas w UTC end-to-end: `scraped_at` powstaje jako `datetime.now(timezone.utc)`).
- **Scraper szuka autora** przez `href` zawierający `/e/ASIN` — nie przez klasę CSS (która łapała też "Paperback", "Kindle Edition" itp.).
- **`run_type` w logach** — `logs.pipeline_runs.run_type` = `'normal'` (scrape + ingest) lub `'backlog'` (tylko ingest zaległego pliku). Odczytywany z XCom `check_pending_start`. Porównanie odbywa się przez stałą `BACKLOG_BRANCH = "skip_fetch"` zdefiniowaną w `logging_db.py` i importowaną w `pipeline.py` (task_id `skip_fetch` EmptyOperatora) — zmiana nazwy gałęzi wymaga edycji jednego miejsca.

### Pliki konfiguracyjne

- `config.yaml` — parametry scrapera (URL, keyword, liczba stron, retry), ścieżki, schemat/tabela w DB. Ładowany i **walidowany schematem Pydantic** w `config.py` → obiekt `CONFIG` z dostępem atrybutowym (`CONFIG.scraper.num_pages`, nie `CONFIG["scraper"]`). Błędny/niekompletny config → wyjątek na starcie (Fail Fast).
- `.env` — dane do połączenia z PostgreSQL (`POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`). PostgreSQL 16 natywnie pod Windows (usługa `postgresql-x64-16`, PGDATA w `C:\Program Files\PostgreSQL\16\data`) — **nie w Dockerze**; z WSL dostępny przez `host.docker.internal` (IP hosta Windows wstrzykiwane przez Docker Desktop do `/etc/hosts`). Znana kruchość: odinstalowanie Docker Desktop zabiera ten wpis i zrywa połączenie, a IP zmienia się z DHCP.
- `~/.dbt/profiles.yml` — profil dbt (poza repozytorium).
- `sql/schema.sql` — DDL całej bazy: `bronze.books` (dane, kolumny TEXT) + `logs.pipeline_runs` (rejestr runów, `dag_run_id` z `UNIQUE`). Uruchamiane **raz** przez `scripts/init_db.py` przy standupie bazy — poza pipeline'em. `ingest.py` robi tylko `TRUNCATE`+`COPY` (bulk load), `logging_db.py` tylko atomowy UPSERT (`INSERT … ON CONFLICT (dag_run_id) DO UPDATE`); struktury nie tworzą, zakładają że istnieje (nieznana kolumna → Fail Fast).

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

### Rules
- na początku każdej sesji przypominaj o pliku NOTEBOOK.md
- Opisuj mi jakich narzędzi urzyłeś do debugowania i testowania. Chodzi mi głównie o bashe jak curl, ss czy ps
- jest to projekt do nauki, ale chce żeby się nadawał na produkcje i do pokazania seniorowi DE.
  - bierz pod uwagą najnowsze koncepty data engineeringu i stacka pod oferty pracy w 2026. 
- uczę się angielskiego. Dodawaj w nawiasach kluczowe słowa, metody, funkcje lub koncepcy po angielsku (ang. TEKST). Zależy mi na tym !!!
- zawsze zapytaj zanim coś faktycznie zmienisz w kodzie.
- sprawdzaj reguły clean code (nie zmieniaj sam, tylko podpowiadaj)
  - DRY
  - świadomy zakres (KISS/right-sized): disaster recovery, formalne SLO/error budget, feature flags, testy wydajnościowe, exactly-once — poza zakresem małego projektu; zapisz to zdaniem w README zamiast wdrażać (umiejętność nie-wdrażania to też decyzja architektoniczna)
  - Funkcja robi jedną rzecz (Single Responsibility).
  - Type hints.
  - logging zamiast print. logger tak (zawsze), własny plik pod orkiestratorem nie (powielanie)
  - Errors should never pass silently — logowanie i wyjątki zamiast `except: pass`.
  - Fail Fast — program zgłasza błąd od razu (walidacja na wejściu), nie po godzinie liczenia; guard clauses to mikro-wersja tej zasady.
  - Single Source of Truth — jedna informacja zdefiniowana w jednym miejscu: np. `DATABASE_URL` nie występuje w 15 plikach (→ config), definicja metryki w jednym modelu dbt 
  - Convention over Configuration — ustalone konwencje (`tests/`, `src/`, `__init__.py`) zamiast setek opcji; dlatego wszystkie projekty pythonowe wyglądają podobnie.
  - Nazwy opisują przeznaczenie: zmienne rzeczownikami (`user_name`), funkcje czasownikami (`load_data()`, nie `data()`).
  - snake_case dla funkcji/zmiennych/plików, PascalCase dla klas, UPPER_CASE dla stałych.
  - pathlib.Path zamiast ścieżek-stringów.
  - Konfiguracja poza kodem (env/.env/Secret Manager — nigdy hasło w źródle).
  - i inne kluczowe reguły.
- Decyzje architektoniczne
  - idempotencja to kluczowa zasada
  - Walidacja konfiguracji na starcie. walidacja przez schemat, przez Pydantic
  - pilnuj pytest'ów
  - projekt lokalny docelowo ma być mapowany pod AWS
  - Pilnować mechaniki late arrive data. w data/ rozdzieli foldery na pliki przetworzone (processed) i nie przetworzone.
- Pilnuj architektury projektu. A jeśli projekt jest świeży to podpowiedz mi żeby stworzyć to czego brakuje poniżej
  - .github/workflows
  - Linter — 'ruff'.
  - podpowiadaj mi o ruff check, uv i pytest co jakis czas.
  - .env
  - README.md odpowiada na 3 pytania **w tej kolejności**: co to i po co → jak uruchomić → jak działa; obcy rozumie projekt w 30 s i uruchamia w 5 min
    - Quick start testowany „na czysto": komendy działają po świeżym `git clone`, bez rzeczy, które masz tylko lokalnie (`.env.example` pokazuje, co ustawić)
    - obowiązkowa sekcja **„Decisions & trade-offs"**: każdy istotny wybór z uzasadnieniem („DuckDB zamiast Postgres, bo X, kosztem Y") + sekcja „Co bym poprawił" (szczerość > udawana perfekcja)
  - docs/architecture.md. Tu jest pokazany przepływ w całym projekcie
  - pyproject.toml
  - scripts/ miejsce na bashe
  - requirement.txt
- Airflow
  - "Watcher task" (standardowy wzorzec Airflow): log_run ma trigger_rule=ALL_DONE
  - `catchup=False` domyślnie (chyba że świadomie przeliczasz historię)
  - `max_active_runs=1` wszędzie, gdzie runy dzielą stan (TRUNCATE staging, self-trigger, backlog FIFO) — inaczej race condition kasuje dane bez błędu
  - XCom tylko na metadane (ścieżka, batch_id, count) — dane w S3/DWH, przekazujesz wskaźnik
  - zero ciężkiego kodu top-level w pliku DAG (`Variable.get()`, requesty, odczyty plików) — scheduler parsuje plik co ~30 s; pobieraj w środku `@task` lub przez Jinja
  - DAG orkiestruje, nie oblicza — logika w `src/`/`include/`, testowalna pytestem bez Airflow; `dags/` cienkie
  - sensory: `mode="reschedule"` (lub deferrable) + obowiązkowy `timeout` — sensor bez timeoutu wisi wiecznie i nie alertuje
- Różne
  - obowiązkowy timeout w requests.get()
  - retry z exponential backoff + jitter (`tenacity`); przy `429` szanuj header `Retry-After` — zwolnij, nie próbuj mocniej
  - funkcje transformujące czyste i deterministyczne — bez I/O i side-effectów (mail, zapis) w środku; efekty na brzegach systemu
  - pliki > RAM: generatory / `chunksize` / streaming; dane > ~1–4 GB → Polars/DuckDB zamiast pandas
  - paginacja: cursor/token zamiast offset/limit (offset drift gubi/dubluje rekordy na żywych danych); jeśli musi być offset → `ORDER BY` po stabilnym kluczu
  - dedup po kluczu biznesowym / hashu payloadu, nie `drop_duplicates()` po całych wierszach
  - czas przechowuj w UTC jako `TIMESTAMPTZ`; konwersja na strefę dopiero przy prezentacji
  - bulk load przez `COPY`, nie `INSERT` po wierszu; staging bez indeksów i constraintów
  - gdy tabela ma downstream (widoki): przeładowuj dane (`TRUNCATE`+`INSERT`), nie strukturę (`DROP`+`CREATE`)
  - surrogate key jako PK; unikalność biznesową wymuszaj osobnym `UNIQUE` 
- Storage, chmura, koszty
  - format kolumnowy (Parquet) + partycjonowanie po dacie/kolumnie niskokardynalnej; nie partycjonuj po `user_id` (small files) — wysokokardynalne klucze bucketuj
  - docelowy rozmiar pliku ~100–128 MB; kompakcja przed zapisem (`coalesce`/`OPTIMIZE`)
  - dostęp usług przez role IAM, nigdy klucze w kodzie/repo; least privilege — bez `Resource: "*"`; przy SSE-KMS pamiętaj `kms:Decrypt`/`GenerateDataKey`
  - `SELECT` konkretnych kolumn, nie `SELECT *` (płacisz za skan); `LIMIT` nie zmniejsza skanu; filtr zawsze po kolumnie partycyjnej bez funkcji na niej
  - compute i storage w tym samym regionie (egress to najdroższy transfer)
  - lifecycle policy na buckety + `AbortIncompleteMultipartUpload`; retencja logów CloudWatch jawnie (default „never expire" = rosnący rachunek)
  - Snowflake: auto-suspend 60 s, nie default 600 s
  - billing alert / budżet pierwszego dnia; tagi `Environment`/`Owner`/`Project` na każdym zasobie
  - dobieraj silnik do skali: DuckDB/Polars single-node zanim Spark; Lambda ma limit 15 min — nie dla ETL; klaster transient per job, nie 24/7
- Testy i CI/CD
  - pytest testuje **kod** (wyzwalacz: PR/push → CI), dbt test testuje **dane** (wyzwalacz: scheduler → DAG) — nie mieszaj: pytest nie jest taskiem w DAG-u
  - pre-commit: `ruff format` → `ruff check` → mypy → pytest (od najtańszego do najdroższego); te same checki jako brama w CI
  - testy nie wołają prawdziwego API — mocki/fixtures (nagrane odpowiedzi); realny kontakt tylko w osobnym smoke teście
  - testy integracyjne izolowane: świeży schemat/kontener per run, kolejność wykonania bez znaczenia
  - pierwszy `pre-commit run --all-files` osobnym commitem (szum formatowania oddzielony od logiki)
  - test wygenerowany przez AI weryfikuj mutacją: zepsuj funkcję celowo — test ma paść
- Bazy i SQL
  - Standardowe rozwiązanie w hurtowni to nie DISTINCT, tylko świadomy wybór jednej wersji na klucz biznesowy (ang. business key) — najczęściej najnowszej, przez okno row_number()
  - DDL (schemat) żyje w migracjach, nie w pipelinie — `CREATE TABLE IF NOT EXISTS` w każdym runie to zapach niedojrzałości. Docelowo należy używać narzędzia do migracji.
  - Dobrym rozwiązaniem jest skrypt init_db.py, pomagający zainicjować schema.sql

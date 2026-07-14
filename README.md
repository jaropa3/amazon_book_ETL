# amazon_books_ETL

![tests](https://github.com/jaropa3/amazon_book_ETL/actions/workflows/tests.yml/badge.svg)

Pipeline ETL scrapujący książki z Amazona i ładujący je do PostgreSQL w architekturze
warstwowej (bronze → staging → intermediate → gold) z transformacjami w dbt i orkiestracją
w Apache Airflow.

## Stack

**Python 3.14** · **PostgreSQL** · **dbt** · **Apache Airflow** · **pandas** · **BeautifulSoup** · **pytest** · **GitHub Actions (CI)**

## Architektura

Warstwy: `scraper → CSV → bronze → dbt (staging → intermediate → gold) → logi`.
Bronze zawiera dane z **jednej sesji** (TRUNCATE przed każdym ingestem); historia kumuluje się
w warstwie gold i w plikach CSV.

📐 **Pełny diagram przepływu i decyzje architektoniczne: [docs/architecture.md](docs/architecture.md)**

## Wymagania

- **Python 3.14** + virtualenv (`.venv`)
- **Docker** — PostgreSQL działa w kontenerze (host `host.docker.internal`)
- **PostgreSQL** — baza docelowa pipeline'u

## Konfiguracja

**1. Zmienne środowiskowe** — utwórz plik `.env` w katalogu głównym (jest w `.gitignore`, nie trafia do repo):

```env
POSTGRES_DB=amazon_books
POSTGRES_HOST=host.docker.internal
POSTGRES_USER=postgres
POSTGRES_PASSWORD=twoje_haslo
POSTGRES_PORT=5432
```

**2. Parametry pipeline'u** — `config.yaml` (scraper, ścieżki, schemat DB):

```yaml
scraper:
  base_url: "https://www.amazon.com/s"
  keyword: "data engineering"
  num_pages: 5
  max_retries: 4
  backoff_base: 2
  delay_between_pages: { min: 2, max: 4 }

storage:
  raw_data_dir: "data/raw_data"

database:
  schema: bronze
  table: books
```

**3. Profil dbt** — `~/.dbt/profiles.yml` (poza repozytorium).

**4. Schemat bazy** — postaw schemat i tabelę bronze **przed pierwszym runem** (DDL żyje poza pipeline'em, w [sql/schema.sql](sql/schema.sql); komenda idempotentna, uruchom raz):

```bash
python scripts/init_db.py
```

## Uruchomienie

```bash
source .venv/bin/activate

# pełny pipeline (scrape → ingest)
python main.py

# transformacje dbt
dbt --project-dir dbt_project --profiles-dir ~/.dbt run
dbt --project-dir dbt_project --profiles-dir ~/.dbt test
```

Airflow (uruchomienie lokalne):

```bash
export AIRFLOW_HOME=~/projects/amazon_books_ETL/airflow
airflow db migrate
airflow webserver -p 8080 &
airflow scheduler &
```

## Testy i CI

Testy kodu (pytest) — czyste funkcje scrapera, bez sieci i bazy:

```bash
pytest            # albo: pytest -v
```

Testy uruchamiają się **automatycznie po każdym push i pull requeście** przez GitHub Actions
([.github/workflows/tests.yml](.github/workflows/tests.yml)). Testy danych (jakość) są osobno,
w warstwie dbt (`dbt test`) i odpalają się w każdym runie pipeline'u.

## Decisions & trade-offs

- **DDL poza pipeline'em ([sql/schema.sql](sql/schema.sql) + [scripts/init_db.py](scripts/init_db.py)), nie w `ingest.py`.** Schemat bronze jest stabilny i znany downstreamowi (dbt), więc definicja tabeli żyje w jednym miejscu i jest stawiana raz — a nie odtwarzana przez `ALTER TABLE ... ADD COLUMN` przy każdym runie. Zysk: nieznana kolumna w CSV pada od razu (Fail Fast) zamiast po cichu rozjechać kontrakt z dbt. Koszt: trzeba pamiętać o kroku standupu po `git clone` (punkt 4 w Konfiguracji).
- **Jeden `schema.sql`, bez narzędzia migracyjnego (Alembic/yoyo).** Przy jednej stabilnej tabeli narzędzie migracyjne to narzut bez zysku (ang. YAGNI). Świadomie odłożone do chwili, gdy schemat zacznie ewoluować na żywej bazie albo dojdzie wiele tabel/środowisk — wtedy `schema.sql` staje się `sql/migrations/0001_init.sql` + runner z tabelą `schema_migrations`.

### Co bym poprawił

- **`COPY` zamiast `executemany`** w `ingest.py` — szybszy bulk load.
- **`csv → parquet`** w warstwie raw (format kolumnowy, mniejszy skan).

## Dokumentacja

- [Architektura](docs/architecture.md)


# amazon_books_ETL

![tests](https://github.com/jaropa3/amazon_book_ETL/actions/workflows/tests.yml/badge.svg)

Pipeline ETL scrapujący książki z Amazona i ładujący je do PostgreSQL w architekturze
warstwowej (bronze → staging → intermediate → gold) z transformacjami w dbt i orkiestracją
w Apache Airflow.

## Przepływ

```
Amazon.com → main.py (scraper) → data/raw_data/*.csv
                                       ↓
                          ingest.py → bronze.books (TRUNCATE + insert, FIFO)
                                       ↓
                        dbt staging → intermediate → test → gold (incremental)
                                       ↓
                         logging_db.py → logs.pipeline_runs
```

## Szybki start

```bash
source .venv/bin/activate

# pełny pipeline (scrape → ingest)
python main.py

# transformacje dbt
dbt --project-dir dbt_project --profiles-dir ~/.dbt run
dbt --project-dir dbt_project --profiles-dir ~/.dbt test

# testy kodu
pytest
```

## Dokumentacja

- [Architektura](docs/architecture.md)
- [Przetwarzanie wsadowe](docs/batch-processing-patterns.md)
- [Late-arriving data](docs/late-arriving-data.md)
- [Raportowanie błędów — teoria](docs/error-reporting-theory.md)
- [Raportowanie błędów — implementacja](docs/error-reporting-implementation.md)

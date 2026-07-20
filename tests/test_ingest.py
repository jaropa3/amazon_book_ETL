"""Testy ingest — wybór pliku FIFO i brak DDL w _insert. Bez bazy: mock kursora."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingest import _insert, _pick_oldest_file, _validate_raw


def test_fifo_wybiera_najstarszy(tmp_path):
    # kolejność tworzenia celowo inna niż chronologiczna nazw
    for name in [
        "books_20240103_000000.csv",
        "books_20240101_000000.csv",
        "books_20240102_000000.csv",
    ]:
        (tmp_path / name).write_text("asin,title\n")
    result = _pick_oldest_file(tmp_path, "books")
    assert result.name == "books_20240101_000000.csv"


def test_fifo_ignoruje_obce_pliki(tmp_path):
    (tmp_path / "books_20240101_000000.csv").write_text("x")
    (tmp_path / "notatka.txt").write_text("x")
    (tmp_path / "other_20200101_000000.csv").write_text("x")
    result = _pick_oldest_file(tmp_path, "books")
    assert result.name == "books_20240101_000000.csv"


def test_brak_plikow_zglasza_wyjatek(tmp_path):
    with pytest.raises(FileNotFoundError):
        _pick_oldest_file(tmp_path, "books")


def test_insert_przeladowuje_dane_przez_copy_bez_ddl():
    """_insert dotyka tylko DANYCH (TRUNCATE + COPY) — struktura żyje w sql/schema.sql.

    Regresja (ang. mutation guard): gdyby ktoś przywrócił CREATE/ALTER TABLE do
    pipeline'u albo wrócił do INSERT po wierszu (executemany) zamiast bulk COPY,
    ten test pada.
    """
    cur = MagicMock()
    df = pd.DataFrame({"asin": ["A1", "A2"], "title": ["Book", "Book2"]})

    _insert(cur, "bronze", "books", df)

    executed = " ".join(str(call.args[0]).upper() for call in cur.execute.call_args_list)
    assert "TRUNCATE" in executed
    assert "CREATE TABLE" not in executed
    assert "ALTER TABLE" not in executed
    # bulk load przez COPY, nie INSERT po wierszu
    cur.executemany.assert_not_called()
    cur.copy.assert_called_once()
    assert "COPY" in cur.copy.call_args.args[0].upper()
    # jeden write_row na wiersz DataFrame
    copy_writer = cur.copy.return_value.__enter__.return_value
    assert copy_writer.write_row.call_count == len(df)


# ── _validate_raw (Fail Fast przed dotknięciem bazy) ──────────────────

SRC = Path("books_20240101_000000.csv")


def test_validate_przepuszcza_poprawny_plik():
    df = pd.DataFrame({"asin": ["A1"], "scraped_at": ["2024-01-01T00:00:00+00:00"]})
    _validate_raw(df, SRC)  # nie rzuca


def test_validate_pusty_plik_rzuca():
    with pytest.raises(ValueError, match="Pusty plik"):
        _validate_raw(pd.DataFrame({"asin": [], "scraped_at": []}), SRC)


def test_validate_brak_kolumny_scraped_at_rzuca():
    with pytest.raises(ValueError, match="scraped_at"):
        _validate_raw(pd.DataFrame({"asin": ["A1"]}), SRC)


def test_validate_scraped_at_same_nulle_rzuca():
    with pytest.raises(ValueError, match="scraped_at"):
        _validate_raw(pd.DataFrame({"asin": ["A1"], "scraped_at": [None]}), SRC)

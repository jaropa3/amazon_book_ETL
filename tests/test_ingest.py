"""Testy wyboru pliku FIFO — bez bazy danych, na tymczasowym katalogu pytest."""

import pytest

from ingest import _pick_oldest_file


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

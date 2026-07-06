"""Testy parsowania HTML → lista książek. Działają na zapisanym fixture, bez sieci."""

from pathlib import Path

import pytest

from main import parse_books

FIXTURE = Path(__file__).parent / "fixtures" / "search_page.html"


@pytest.fixture
def sample_html() -> bytes:
    return FIXTURE.read_bytes()


def test_parse_wyciaga_ksiazki_z_autorem(sample_html):
    # trzeci wynik nie ma linku autora /e/ASIN → powinien zostać pominięty
    books = parse_books(sample_html)
    assert len(books) == 2


def test_parse_wypelnia_wszystkie_pola(sample_html):
    first = parse_books(sample_html)[0]
    assert first == {
        "asin": "B001ABCDEF",
        "title": "Clean Code",
        "author": "Robert C. Martin",
        "price": "$29.99",
        "rating": "4.7 out of 5 stars",
    }


def test_autor_nie_jest_formatem_wydania(sample_html):
    """Regresja: autor z linku /e/ASIN, a nie z klasy CSS (łapała 'Paperback'/'Kindle Edition')."""
    formaty = {"Paperback", "Kindle Edition", "Hardcover", "Audible Audiobook"}
    authors = {b["author"] for b in parse_books(sample_html)}
    assert authors.isdisjoint(formaty)


def test_pusty_html_zwraca_pusta_liste():
    assert parse_books("<html><body></body></html>") == []

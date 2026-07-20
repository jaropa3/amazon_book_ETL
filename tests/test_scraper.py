"""Testy jednostkowe scrapera — bez sieci i bez bazy danych."""

from pathlib import Path

import pytest
import requests

import main
from main import (
    USER_AGENTS,
    _get_with_retry,
    _is_challenge_page,
    build_headers,
    fetch_books,
    scrape_to_csv,
)


class FakeResponse:
    """Atrapa obiektu requests.Response — tylko pola używane przez testowane funkcje."""

    def __init__(
        self,
        text: str = "normalna strona",
        content: bytes = b"x" * 10000,
        status_code: int = 200,
    ):
        self.text = text
        self.content = content
        self.status_code = status_code


# ── _is_challenge_page ────────────────────────────────────────────────

def test_challenge_gdy_bm_verify_w_tekscie():
    resp = FakeResponse(text="strona z bm-verify w środku", content=b"x" * 10000)
    assert _is_challenge_page(resp) is True


def test_challenge_gdy_strona_za_krotka():
    resp = FakeResponse(text="ok", content=b"x" * 100)  # < 5000 bajtów
    assert _is_challenge_page(resp) is True


def test_normalna_strona_nie_jest_challenge():
    resp = FakeResponse(text="normalna treść strony", content=b"x" * 10000)
    assert _is_challenge_page(resp) is False


# ── build_headers ─────────────────────────────────────────────────────

def test_build_headers_zawiera_user_agent():
    headers = build_headers()
    assert "User-Agent" in headers


def test_build_headers_user_agent_z_puli():
    headers = build_headers()
    assert headers["User-Agent"] in USER_AGENTS


def test_build_headers_ma_wymagane_naglowki():
    headers = build_headers()
    assert set(headers) == {"User-Agent", "Accept", "Accept-Language"}


# ── _get_with_retry ───────────────────────────────────────────────────


def _patch_get(monkeypatch, responses):
    """Podstawia requests.get sekwencją odpowiedzi (ostatnia powtarzana) i wycisza sleep."""
    state = {"n": 0}

    def fake_get(url, headers=None, timeout=None):
        i = min(state["n"], len(responses) - 1)
        state["n"] += 1
        item = responses[i]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(main.requests, "get", fake_get)
    monkeypatch.setattr(main.time, "sleep", lambda *_: None)  # bez realnych opóźnień
    return state


def test_retry_zwraca_od_razu_przy_sukcesie(monkeypatch):
    calls = _patch_get(monkeypatch, [FakeResponse(status_code=200)])
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert result.status_code == 200
    assert calls["n"] == 1  # zero ponowień


def test_retry_ponawia_przy_503_potem_sukces(monkeypatch):
    calls = _patch_get(
        monkeypatch, [FakeResponse(status_code=503), FakeResponse(status_code=200)]
    )
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert result.status_code == 200
    assert calls["n"] == 2  # jedna nieudana + jedna udana


def test_retry_ponawia_przy_challenge_page(monkeypatch):
    challenge = FakeResponse(text="... bm-verify ...", status_code=200)
    calls = _patch_get(monkeypatch, [challenge, FakeResponse(status_code=200)])
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert _is_challenge_page(result) is False
    assert calls["n"] == 2


def test_retry_poddaje_sie_po_max_retries(monkeypatch):
    calls = _patch_get(monkeypatch, [FakeResponse(status_code=503)])  # zawsze 503
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert result.status_code == 503
    assert calls["n"] == 4  # max_retries + 1 prób


def test_retry_ponawia_po_bledzie_sieci_potem_sukces(monkeypatch):
    # wyjątek sieciowy ma być traktowany jak nieudana próba, nie wysadzać scrapingu
    calls = _patch_get(
        monkeypatch,
        [requests.ConnectionError("boom"), FakeResponse(status_code=200)],
    )
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert result.status_code == 200
    assert calls["n"] == 2


def test_retry_zwraca_none_gdy_stale_bledy_sieci(monkeypatch):
    calls = _patch_get(monkeypatch, [requests.Timeout("timed out")])  # zawsze wyjątek
    result = _get_with_retry("http://x", max_retries=3, backoff_base=2)
    assert result is None
    assert calls["n"] == 4  # max_retries + 1 prób


# ── fetch_books / scrape_to_csv ───────────────────────────────────────

FIXTURE = Path(__file__).parent / "fixtures" / "search_page.html"


def _search_page_response() -> FakeResponse:
    """Strona wyników z fixture (2 książki), dopchana spacjami do >5000 B.

    Poniżej tego progu _is_challenge_page uznałby ją za blokadę Amazona i
    fetch_books pominąłby stronę. BeautifulSoup padding ignoruje.
    """
    return FakeResponse(content=FIXTURE.read_bytes().ljust(6000))


def test_fetch_books_zwraca_ksiazki_ze_wszystkich_stron(monkeypatch):
    _patch_get(monkeypatch, [_search_page_response()])
    books = fetch_books(num_pages=3)
    assert len(books) == 6  # 3 strony × 2 książki z fixture


def test_fetch_books_nie_zapisuje_na_dysk(monkeypatch):
    """Regresja (ang. mutation guard): zapis to efekt uboczny na brzegu systemu
    (scrape_to_csv). Gdyby ktoś wrócił z save_books_to_csv() do fetch_books, test pada.
    """
    _patch_get(monkeypatch, [_search_page_response()])
    zapisy = []
    monkeypatch.setattr(main, "save_books_to_csv", lambda *a, **k: zapisy.append(1))

    fetch_books(num_pages=1)

    assert zapisy == []


def test_fetch_books_rzuca_gdy_zadna_strona_sie_nie_udala(monkeypatch):
    """Fail Fast: same błędy → wyjątek, a nie cichy pusty CSV."""
    _patch_get(monkeypatch, [FakeResponse(status_code=503)])  # zawsze 503
    with pytest.raises(RuntimeError):
        fetch_books(num_pages=2)


def test_scrape_to_csv_zapisuje_raz_i_zwraca_liczbe(monkeypatch):
    """Brzeg systemu: pobiera + zapisuje dokładnie raz, zwraca licznik → XCom."""
    _patch_get(monkeypatch, [_search_page_response()])
    zapisane = []
    monkeypatch.setattr(main, "save_books_to_csv", lambda books: zapisane.append(books))

    count = scrape_to_csv(num_pages=2)

    assert count == 4  # 2 strony × 2 książki
    assert len(zapisane) == 1  # zapis dokładnie raz
    assert len(zapisane[0]) == 4

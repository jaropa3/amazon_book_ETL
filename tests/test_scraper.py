"""Testy jednostkowe scrapera — bez sieci i bez bazy danych."""

from main import USER_AGENTS, _is_challenge_page, build_headers


class FakeResponse:
    """Atrapa obiektu requests.Response — tylko pola używane przez testowane funkcje."""

    def __init__(self, text: str, content: bytes):
        self.text = text
        self.content = content


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

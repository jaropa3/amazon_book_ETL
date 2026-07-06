"""Walidacja konfiguracji (fail-fast).

config._validate zgłasza brakujące klucze od razu przy wczytaniu — zamiast pozwolić,
by zły config.yaml wybuchł godzinę później jako mętny KeyError w środku pipeline'u.
"""

import pytest

from config import CONFIG, REQUIRED_KEYS, _validate


def _resolve(cfg: dict, path: tuple[str, ...]):
    node = cfg
    for key in path:
        node = node[key]
    return node


@pytest.mark.parametrize("path", REQUIRED_KEYS, ids=lambda p: ".".join(p))
def test_config_ma_wymagany_klucz(path):
    assert _resolve(CONFIG, path) is not None


def test_validate_przepuszcza_poprawny_config():
    _validate(CONFIG)  # realny config — nie powinno rzucić


def test_validate_zglasza_brak_kluczy():
    broken = {"scraper": {"base_url": "x"}}  # brakuje prawie wszystkiego
    with pytest.raises(ValueError):
        _validate(broken)


def test_validate_komunikat_wskazuje_brakujacy_klucz():
    broken = {
        "scraper": {
            "base_url": "x",
            "keyword": "y",
            "num_pages": 1,
            "max_retries": 1,
            "backoff_base": 2,
            "delay_between_pages": {"min": 1, "max": 2},
        },
        "storage": {"raw_data_dir": "d"},
        "database": {"schema": "s"},  # brak database.table
    }
    with pytest.raises(ValueError, match="database.table"):
        _validate(broken)


def test_config_wartosci_numeryczne_maja_wlasciwy_typ():
    # kod robi range(num_pages + 1) i backoff_base ** attempt — typy muszą się zgadzać
    scraper = CONFIG["scraper"]
    assert isinstance(scraper["num_pages"], int)
    assert isinstance(scraper["max_retries"], int)
    assert isinstance(scraper["backoff_base"], (int, float))
    assert isinstance(scraper["delay_between_pages"]["min"], (int, float))
    assert isinstance(scraper["delay_between_pages"]["max"], (int, float))

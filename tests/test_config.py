"""Walidacja konfiguracji przez Pydantic.

Zły config.yaml (brak klucza, ujemny num_pages, zły typ) jest odrzucany od razu
przy wczytaniu — ValidationError zamiast mętnego błędu godzinę później w pipeline.
"""

import pytest
from pydantic import ValidationError

from config import CONFIG, Config


def _valid_dict() -> dict:
    """Kompletny, poprawny słownik konfiguracji — baza do psucia w testach."""
    return {
        "scraper": {
            "base_url": "https://x",
            "keyword": "y",
            "num_pages": 5,
            "max_retries": 3,
            "backoff_base": 2,
            "delay_between_pages": {"min": 1, "max": 4},
        },
        "storage": {"raw_data_dir": "data/raw_data"},
        "database": {"schema": "bronze", "table": "books"},
    }


def test_realny_config_ma_kluczowe_pola():
    assert CONFIG.scraper.base_url
    assert CONFIG.database.db_schema      # alias: w YAML "schema"
    assert CONFIG.storage.raw_data_dir


def test_poprawny_dict_przechodzi():
    Config(**_valid_dict())  # nie powinno rzucić


def test_odrzuca_brak_klucza():
    dane = _valid_dict()
    del dane["database"]["table"]
    with pytest.raises(ValidationError):
        Config(**dane)


def test_odrzuca_ujemne_num_pages():
    dane = _valid_dict()
    dane["scraper"]["num_pages"] = -1  # Field(gt=0)
    with pytest.raises(ValidationError):
        Config(**dane)


def test_odrzuca_zly_typ_num_pages():
    dane = _valid_dict()
    dane["scraper"]["num_pages"] = "pięć"  # nie da się skonwertować na int
    with pytest.raises(ValidationError):
        Config(**dane)

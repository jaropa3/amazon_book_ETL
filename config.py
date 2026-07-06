import os

import yaml

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")

# Ścieżki kluczy, na których opiera się kod (main.py / ingest.py) — jedno źródło prawdy.
REQUIRED_KEYS: tuple[tuple[str, ...], ...] = (
    ("scraper", "base_url"),
    ("scraper", "keyword"),
    ("scraper", "num_pages"),
    ("scraper", "max_retries"),
    ("scraper", "backoff_base"),
    ("scraper", "delay_between_pages", "min"),
    ("scraper", "delay_between_pages", "max"),
    ("storage", "raw_data_dir"),
    ("database", "schema"),
    ("database", "table"),
)


def _validate(config: dict) -> None:
    """Fail-fast: zgłasza brakujące klucze od razu przy wczytaniu, z jasnym komunikatem."""
    missing = []
    for path in REQUIRED_KEYS:
        node = config
        for key in path:
            if not isinstance(node, dict) or key not in node:
                missing.append(".".join(path))
                break
            node = node[key]
    if missing:
        raise ValueError(f"config.yaml — brak wymaganych kluczy: {', '.join(missing)}")


def _load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    _validate(config)
    return config


CONFIG = _load_config(CONFIG_PATH)

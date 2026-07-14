import os

import yaml
from pydantic import BaseModel, ConfigDict, Field


class DelayConfig(BaseModel):
    min: float = Field(ge=0)
    max: float = Field(ge=0)


class ScraperConfig(BaseModel):
    base_url: str
    keyword: str
    num_pages: int = Field(gt=0)
    max_retries: int = Field(ge=0)
    backoff_base: float = Field(gt=1)
    delay_between_pages: DelayConfig


class StorageConfig(BaseModel):
    raw_data_dir: str


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    # 'schema' koliduje z atrybutem BaseModel — pole nazywamy db_schema,
    # ale w config.yaml klucz zostaje "schema" (alias).
    db_schema: str = Field(alias="schema")
    table: str


class Config(BaseModel):
    scraper: ScraperConfig
    storage: StorageConfig
    database: DatabaseConfig


CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")

with open(CONFIG_PATH, encoding="utf-8") as f:
    CONFIG = Config.model_validate(yaml.safe_load(f))  # wczytuje config.yaml i waliduje schematem

import pandas as pd
import psycopg
from dotenv import load_dotenv
import os

def connection_db():
    con = psycopg.connect(
        host="host.docker.internal",
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    return con
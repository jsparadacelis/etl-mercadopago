import os

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class Settings:
    
    db_user = os.getenv('DB_USER', 'postgres')
    db_pass = os.getenv('DB_PASS', 'prueba_meli')
    db_name = os.getenv('DB_NAME', 'postgres')

_settings = Settings()


def get_db_conn():
    db_user = _settings.db_user
    db_pass = _settings.db_pass
    return psycopg2.connect(f"host=localhost user={db_user} password={db_pass} port=5432")


def create_engine_conn() -> Engine:
    db_user = _settings.db_user
    db_pass = _settings.db_pass
    db_name = _settings.db_name
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@localhost:5432/{db_name}")
    return engine

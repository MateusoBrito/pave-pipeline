import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

load_dotenv()


def get_postgres_url() -> str:
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(get_postgres_url(), pool_pre_ping=True)
    return _engine


def get_session() -> Session:
    SessionLocal = sessionmaker(bind=get_engine())
    return SessionLocal()
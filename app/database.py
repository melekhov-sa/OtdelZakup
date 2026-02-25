"""SQLite database engine, session management, and bootstrap."""

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DB_PATH = Path(os.environ.get("OTDELZAKUP_DB_PATH", "./data/readiness.db"))


def _make_engine(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        echo=False,
    )


engine = _make_engine(DB_PATH)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db_session():
    """Create a new database session. Caller must close it."""
    return SessionLocal()


def init_db():
    """Create all tables if they do not exist."""
    import app.models  # noqa: F401 — ensure models are registered
    Base.metadata.create_all(bind=engine)

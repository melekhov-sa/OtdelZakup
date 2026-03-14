"""SQLite database engine, session management, and bootstrap."""

import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DB_PATH = Path(os.environ.get("OTDELZAKUP_DB_PATH", "./data/readiness.db"))


def _make_engine(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    eng = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 30},
        echo=False,
    )

    @event.listens_for(eng, "connect")
    def _on_connect(dbapi_conn, _record):
        # WAL mode: readers don't block the writer and vice versa.
        # NORMAL synchronous: safe with WAL and much faster than FULL.
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()

    return eng


engine = _make_engine(DB_PATH)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db_session():
    """Create a new database session. Caller must close it."""
    return SessionLocal()


def get_catalog_version() -> int:
    """Return the current catalog version counter (0 if never set)."""
    from app.models import SystemSetting  # noqa: PLC0415
    session = SessionLocal()
    try:
        row = session.get(SystemSetting, "catalog_version")
        return int(row.value) if row and row.value else 0
    finally:
        session.close()


def increment_catalog_version() -> int:
    """Increment catalog_version counter and return the new value."""
    from app.models import SystemSetting  # noqa: PLC0415
    session = SessionLocal()
    try:
        row = session.get(SystemSetting, "catalog_version")
        new_val = (int(row.value) + 1) if (row and row.value) else 1
        if row:
            row.value = str(new_val)
        else:
            session.add(SystemSetting(key="catalog_version", value=str(new_val)))
        session.commit()
        return new_val
    finally:
        session.close()


def init_db():
    """Create all tables if they do not exist."""
    import app.models  # noqa: F401 — ensure models are registered
    import app.order_models  # noqa: F401 — register order & quote models
    import app.quality_models  # noqa: F401 — register quality monitoring models
    import app.benchmark_models  # noqa: F401 — register benchmark models
    Base.metadata.create_all(bind=engine)
    # Ensure WAL mode is active for this database file (survives restarts).
    with engine.connect() as conn:
        conn.execute(__import__("sqlalchemy").text("PRAGMA journal_mode=WAL"))
        conn.execute(__import__("sqlalchemy").text("PRAGMA synchronous=NORMAL"))

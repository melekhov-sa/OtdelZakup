"""Tests for the product type directory and matcher."""

import pytest

from app.product_type_matcher import match_product_type


# ── Lightweight mock so tests don't require the DB ─────────────────────────────

class _MockProductType:
    """Minimal stand-in for the ProductType ORM object."""

    def __init__(self, name: str, aliases: list[str]):
        self.name = name
        self._aliases = aliases

    @property
    def aliases(self) -> list[str]:
        return self._aliases


@pytest.fixture()
def sample_types():
    return [
        _MockProductType("болт",   ["болта", "болты", "болтов"]),
        _MockProductType("гайка",  ["гайки", "гаек", "гайке"]),
        _MockProductType("шайба",  ["шайбы", "шайб"]),
    ]


# ── Unit tests (no DB needed) ──────────────────────────────────────────────────


def test_match_exact_word(sample_types):
    """Exact whole-word match returns the primary type name."""
    assert match_product_type("Болт М12x80 ГОСТ 7798-70", types=sample_types) == "болт"


def test_match_alias(sample_types):
    """An alias match returns the primary type name."""
    assert match_product_type("гайки М10 DIN 934", types=sample_types) == "гайка"


def test_match_no_match_returns_empty(sample_types):
    """When no type matches, an empty string is returned."""
    assert match_product_type("Шуруп 4.2x16", types=sample_types) == ""


def test_match_case_insensitive(sample_types):
    """Matching is case-insensitive."""
    assert match_product_type("БОЛТ СТРОИТЕЛЬНЫЙ М10", types=sample_types) == "болт"


# ── DB-backed tests (require isolation fixture) ────────────────────────────────


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir  = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR",  str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR  = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH     = db_path
    db_mod.engine      = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    db_mod.SessionLocal = sessionmaker(
        bind=db_mod.engine, autoflush=False, expire_on_commit=False
    )
    db_mod.init_db()


def test_seed_creates_default_types():
    """seed_default_product_types() populates all 16 default types."""
    from app.seed import seed_default_product_types
    from app.database import get_db_session
    from app.models import ProductType

    seed_default_product_types()
    db = get_db_session()
    try:
        names = {pt.name for pt in db.query(ProductType).all()}
    finally:
        db.close()

    assert "болт"    in names
    assert "гайка"   in names
    assert "шайба"   in names
    assert "саморез" in names
    assert "герметик" in names
    assert len(names) >= 16


def test_seed_is_idempotent():
    """Calling seed_default_product_types() twice does not create duplicates."""
    from app.seed import seed_default_product_types
    from app.database import get_db_session
    from app.models import ProductType

    seed_default_product_types()
    seed_default_product_types()
    db = get_db_session()
    try:
        count = db.query(ProductType).count()
    finally:
        db.close()

    assert count == 16

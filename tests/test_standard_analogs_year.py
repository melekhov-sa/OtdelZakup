"""Analog lookup must ignore the edition year of a standard key."""
import pytest

from app.models import StandardEquivalent


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


def _seed(src, dst):
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def test_exact_key_still_works():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]


def test_key_without_year_finds_analog():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-7798") == ["DIN-933"]


def test_reverse_direction_without_year():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("DIN-933") == ["GOST-7798-70"]


def test_unknown_key_returns_empty():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-9999") == []


def test_result_is_a_copy():
    """Caller must not be able to corrupt the shared cache."""
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    got = get_standard_analogs("GOST-7798-70")
    got.append("JUNK")
    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]

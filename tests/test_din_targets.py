"""din_targets_for_row: which DIN keys a row must be searched by."""
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


@pytest.fixture
def seeded():
    """ГОСТ 7798-70 ↔ DIN 933, DIN 933 ↔ ISO 4017, ISO 4014 ↔ DIN 931."""
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    for src, dst in [
        ("GOST-7798-70", "DIN-933"),
        ("DIN-933", "ISO-4017"),
        ("ISO-4014", "DIN-931"),
    ]:
        session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def _row(**kw):
    row = {"name": "", "name_raw": "", "gost": "", "iso": "", "din": ""}
    row.update(kw)
    return row


@pytest.mark.parametrize("row,expected", [
    (_row(din="DIN 933"),                        []),
    (_row(gost="ГОСТ 7798-70"),                  ["DIN-933"]),
    (_row(gost="ГОСТ 7798"),                     ["DIN-933"]),
    (_row(iso="ISO 4017"),                       ["DIN-933"]),
    (_row(gost="ГОСТ Р ИСО 4014"),               ["DIN-931"]),
    (_row(gost="ГОСТ 9999"),                     []),
    (_row(),                                     []),
    (_row(name_raw="Болт М12х60 без стандарта"), []),
    (_row(name_raw="Болт ГОСТ 7798-70 М12х60"),  ["DIN-933"]),
    (_row(name_raw="Болт DIN 933 М12х60"),       []),
])
def test_din_targets(seeded, row, expected):
    from app.matching.standard_analogs import din_targets_for_row
    assert din_targets_for_row(row) == expected


def test_din_wins_over_gost_in_the_same_text(seeded):
    """A row that already names a DIN stays on it, even if a ГОСТ is written too."""
    from app.matching.standard_analogs import din_targets_for_row
    assert din_targets_for_row(_row(name_raw="Болт DIN 933 (аналог ГОСТ 7798-70)")) == []


def test_only_din_analogs_are_returned(seeded):
    """ISO 4017 has both a DIN and a GOST analog; only DIN comes back."""
    from app.database import get_db_session
    from app.matching.standard_analogs import (
        din_targets_for_row,
        invalidate_standard_analogs_cache,
    )
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical="ISO-4017", dst_canonical="GOST-7805-70", is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()
    assert din_targets_for_row(_row(iso="ISO 4017")) == ["DIN-933"]


def test_analog_queries_unfiltered(seeded):
    """Without a filter, ГОСТ 7798-70 is rewritten to its DIN analog."""
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт ГОСТ 7798-70 М12х60")
    assert [q.analog_canonical for q in queries] == ["DIN-933"]
    assert queries[0].rewritten_text == "Болт DIN 933 М12х60"


def test_analog_queries_filtered_to_din(seeded):
    """DIN 933 has both an ISO and a GOST analog; the filter keeps neither."""
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт DIN 933 М12х60", allowed_analogs={"DIN-931"})
    assert queries == []


def test_analog_queries_filter_keeps_allowed(seeded):
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт ГОСТ 7798-70 М12х60", allowed_analogs={"DIN-933"})
    assert [q.analog_canonical for q in queries] == ["DIN-933"]

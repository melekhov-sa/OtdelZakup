"""Seeding the analog reference book from the published DIN tables."""
import re

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


def _count():
    from app.database import get_db_session
    session = get_db_session()
    try:
        return session.query(StandardEquivalent).count()
    finally:
        session.close()


# ── The pair list itself ──────────────────────────────────────────────────────

def test_pairs_are_canonical_keys():
    """Every key must be in the form the matcher produces, or lookups miss."""
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    shape = re.compile(r"^(DIN|GOST|ISO)-[\w.\-]+$")
    for src, dst in DIN_GOST_PAIRS:
        assert shape.match(src), src
        assert shape.match(dst), dst


def test_every_pair_starts_from_a_din():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    assert all(src.startswith("DIN-") for src, _ in DIN_GOST_PAIRS)


def test_no_letter_suffixes_survived():
    """DIN 125 А and DIN 127 B collapse onto the bare number the catalog uses."""
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    for src, _ in DIN_GOST_PAIRS:
        tail = src[len("DIN-"):]
        assert re.match(r"^\d+(-\d+)?$", tail), src


def test_no_cyrillic_left_in_keys():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    for src, dst in DIN_GOST_PAIRS:
        for key in (src, dst):
            assert not any("Ѐ" <= ch <= "ӿ" for ch in key), key


def test_no_duplicate_pairs():
    """The table has a unique constraint — a duplicate would break the seed."""
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    assert len(set(DIN_GOST_PAIRS)) == len(DIN_GOST_PAIRS)


def test_no_self_pairs():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    assert all(src != dst for src, dst in DIN_GOST_PAIRS)


def test_known_pairs_are_present():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS
    pairs = set(DIN_GOST_PAIRS)
    assert ("DIN-933", "GOST-7798-70") in pairs
    assert ("DIN-934", "GOST-5927-70") in pairs
    assert ("DIN-125", "GOST-11371-78") in pairs


# ── Seeding behaviour ─────────────────────────────────────────────────────────

def test_seed_fills_an_empty_table():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS, seed_standard_equivalents
    assert _count() == 0
    seed_standard_equivalents()
    assert _count() == len(DIN_GOST_PAIRS)


def test_seed_keeps_out_of_a_curated_table():
    """A pair deleted by hand must not come back on the next restart."""
    from app.database import get_db_session
    from app.seed_standard_equivalents import seed_standard_equivalents

    session = get_db_session()
    session.add(StandardEquivalent(
        src_canonical="GOST-7798-70", dst_canonical="DIN-933", is_active=True,
    ))
    session.commit()
    session.close()

    seed_standard_equivalents()

    assert _count() == 1


def test_seed_is_idempotent():
    from app.seed_standard_equivalents import DIN_GOST_PAIRS, seed_standard_equivalents
    seed_standard_equivalents()
    seed_standard_equivalents()
    assert _count() == len(DIN_GOST_PAIRS)


def test_seeded_pairs_are_found_by_the_matcher():
    """End of the chain: a seeded pair must be visible to analog lookup."""
    from app.matching.standard_analogs import get_standard_analogs
    from app.seed_standard_equivalents import seed_standard_equivalents

    seed_standard_equivalents()

    analogs = get_standard_analogs("GOST-7798-70")
    assert "DIN-933" in analogs


def test_din_targets_work_for_a_gost_row():
    """The whole point: a ГОСТ row now has DIN targets to search by."""
    from app.matching.standard_analogs import din_targets_for_row
    from app.seed_standard_equivalents import seed_standard_equivalents

    seed_standard_equivalents()

    targets = din_targets_for_row({"gost": "ГОСТ 7798-70", "iso": "", "din": "",
                                   "name": "", "name_raw": ""})
    assert "DIN-933" in targets

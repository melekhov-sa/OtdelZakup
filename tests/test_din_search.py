"""End-to-end matching in the "Poisk po DIN" mode."""
import pytest

from app.matcher import add_internal_matches
from app.match_settings import MatchSettings
from app.matching.minhash_index import rebuild_index
from app.models import InternalItem, StandardEquivalent


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


def _settings(**kw):
    defaults = dict(
        enable_minhash=True,
        lsh_threshold=0.05,
        num_perm=64,
        minhash_top_k=20,
        ngram_n=4,
        use_type_buckets=False,
        min_candidates_before_fallback=1,
        auto_apply_enabled=True,
        auto_apply_jaccard_threshold=0.0,
        always_require_confirmation=False,
        use_standard_analogs_in_main_match=False,
        analogs_only=False,
        din_only=False,
        min_display_score=0,
    )
    defaults.update(kw)
    return MatchSettings(**defaults)


def _seed_catalog(items_data):
    from datetime import datetime, timezone
    from app.database import get_db_session
    session = get_db_session()
    for d in items_data:
        session.add(InternalItem(
            name=d["name"],
            item_type=d.get("item_type", "болт"),
            size=d.get("size", "M12x60"),
            standard_text=d.get("standard_text", ""),
            standard_key=d.get("standard_key"),
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))
    session.commit()
    items = session.query(InternalItem).all()
    session.close()
    rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)
    return items


def _seed_equiv(pairs):
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    for src, dst in pairs:
        session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def _df(rows):
    import pandas as pd
    return pd.DataFrame(rows)


def _row(name, gost="", din="", iso="", size="M12x60", item_type="болт"):
    return {
        "name": name, "name_raw": name, "item_type": item_type, "size": size,
        "gost": gost, "iso": iso, "din": din,
        "diameter": "", "length": "", "strength": "", "coating": "",
    }


DIN_ITEM = {"name": "Болт М12x60 DIN 933", "standard_text": "DIN 933", "standard_key": "DIN-933"}
GOST_ITEM = {"name": "Болт М12x60 ГОСТ 7798-70", "standard_text": "ГОСТ 7798-70", "standard_key": "GOST-7798-70"}


def test_gost_row_finds_din_item():
    """A ГОСТ row is matched to the DIN item through the analog."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None
    assert "DIN 933" in results[0]["name"]


def test_gost_row_standard_only_in_column():
    """The standard lives in a column, not in the name — the DIN search still works."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None


def test_gost_item_not_substituted_for_gost_row():
    """Следствие 1: DIN-аналог есть, но по нему в каталоге пусто — подбора нет."""
    _seed_catalog([GOST_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is None


def test_din_row_stays_on_din():
    """A DIN row keeps its own standard and is not led onto the ГОСТ item."""
    _seed_catalog([DIN_ITEM, GOST_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 DIN 933", din="DIN 933")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert "DIN 933" in results[0]["name"]


def test_row_without_standard_matches_as_usual():
    """No standard in the row — the mode must not change anything for it."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60")])
    off = add_internal_matches(_df([_row("Болт М12x60")]), settings=_settings())[1]
    din = add_internal_matches(df, settings=_settings(din_only=True))[1]

    assert din[0]["internal_item_id"] == off[0]["internal_item_id"]


def test_row_without_din_analog_falls_back_to_own_standard():
    """No DIN counterpart in the reference book — search by the row's own standard."""
    _seed_catalog([GOST_ITEM])
    _seed_equiv([("GOST-5927-70", "DIN-934")])  # unrelated pair

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None
    assert "ГОСТ 7798-70" in results[0]["name"]


def test_item_without_standard_is_dropped_in_din_mode():
    """An item with no standard must not pass as "no verdict" in DIN mode."""
    _seed_catalog([
        DIN_ITEM,
        {"name": "Болт М12x60 без стандарта", "standard_text": "", "standard_key": None},
    ])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    names = [c["name"] for c in results[0]["candidates"]]
    assert all("без стандарта" not in n for n in names)


def test_standard_badge_true_for_din_counterpart():
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    best = results[0]["candidates"][0]
    assert best["field_badges"]["standard"]["match"] is True

"""Tests for DB-backed size/diameter/length detection rules."""

import pytest
from datetime import datetime, timezone


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


def _seed_rules():
    from app.seed import seed_default_size_rules
    seed_default_size_rules()


def _detect(text, item_type=None):
    from app.services.size_detector import detect_size, load_active_size_rules
    rules = load_active_size_rules()
    return detect_size(text, item_type=item_type, rules=rules)


# ── Test 1: "Болт М12х50 ГОСТ 7798" → M12x50 ──────────────────────────────

def test_bolt_m12x50():
    _seed_rules()
    result = _detect("Болт М12х50 ГОСТ 7798")
    assert result is not None
    assert result.size_norm == "M12x50"
    assert result.size_kind == "diameter_length"
    assert result.diameter == "M12"
    assert result.length == "50"


# ── Test 2: "Болт 12*50 8.8" → 12x50 ───────────────────────────────────────

def test_bolt_12_star_50():
    _seed_rules()
    # * is preprocessed to x
    result = _detect("Болт 12*50 8.8")
    assert result is not None
    assert result.size_norm == "12x50"
    assert result.diameter == "M12"  # M prefix added for diameter_length
    assert result.length == "50"


# ── Test 3: "Саморез 4,8x35" → 4.8x35 ─────────────────────────────────────

def test_screw_4_8x35():
    _seed_rules()
    result = _detect("Саморез 4,8x35")
    assert result is not None
    assert result.size_norm == "4.8x35"
    assert result.diameter == "4.8"  # no M prefix for decimal diameters
    assert result.length == "35"


# ── Test 4: "Диск 125x1.6x22" → triple_size ────────────────────────────────

def test_disc_triple_size():
    _seed_rules()
    result = _detect("Диск 125x1.6x22")
    assert result is not None
    assert result.size_kind == "triple_size"
    assert result.size_norm == "125x1.6x22"
    assert result.diameter == "125"
    assert result.thickness == "1.6"
    assert result.width == "22"


# ── Test 5: "Гайка М20-7H" → thread with tolerance ─────────────────────────

def test_nut_m20_7h():
    _seed_rules()
    result = _detect("Гайка М20-7H")
    assert result is not None
    assert result.size_kind == "thread"
    assert result.diameter == "M20"
    assert result.tolerance == "7H"


# ── Test 6: Размер не найден → None ─────────────────────────────────────────

def test_no_size():
    _seed_rules()
    result = _detect("Краска белая 1л")
    assert result is None


# ── Test 7: Cyrillic М and х normalized ──────────────────────────────────────

def test_cyrillic_normalization():
    _seed_rules()
    # Cyrillic М and х should be normalized to Latin M and x
    result = _detect("Болт М16х80")
    assert result is not None
    assert result.size_norm == "M16x80"


# ── Test 8: "M 12 x 50" (with spaces) ──────────────────────────────────────

def test_spaces_in_size():
    _seed_rules()
    result = _detect("Болт M 12 x 50")
    assert result is not None
    assert result.size_norm == "M12x50"
    assert result.diameter == "M12"
    assert result.length == "50"


# ── Test 9: Diameter only: M12 ──────────────────────────────────────────────

def test_diameter_only():
    _seed_rules()
    result = _detect("Гайка М12 ГОСТ 5915")
    assert result is not None
    assert result.size_kind == "diameter"
    assert result.diameter == "M12"
    assert result.size_norm == "M12"


# ── Test 10: Diameter with d prefix ─────────────────────────────────────────

def test_d_prefix_diameter():
    _seed_rules()
    result = _detect("Штифт d8 ГОСТ 3128")
    assert result is not None
    assert result.size_kind == "diameter"
    assert result.diameter == "d8"


# ── Test 11: "125х1,6х22" (Cyrillic x + comma) ─────────────────────────────

def test_triple_cyrillic_comma():
    _seed_rules()
    result = _detect("Диск 125х1,6х22")
    assert result is not None
    assert result.size_kind == "triple_size"
    assert result.size_norm == "125x1.6x22"


# ── Test 12: Priority — triple_size wins over diameter_length ────────────────

def test_triple_priority_over_pair():
    _seed_rules()
    # Triple size has priority 200, diameter_length has 180/170/150
    result = _detect("125x1.6x22")
    assert result is not None
    assert result.size_kind == "triple_size"


# ── Test 13: Inactive rule skipped ──────────────────────────────────────────

def test_inactive_rule_skipped():
    from app.database import get_db_session
    from app.models import SizeRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(SizeRule(
            pattern_raw=r"testsize(?P<d>\d+)",
            match_type="regex",
            size_kind="custom",
            normalize_template="TEST{d}",
            priority=999,
            is_active=False,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect("Болт testsize42")
    assert result is None


# ── Test 14: Seed is idempotent ─────────────────────────────────────────────

def test_seed_idempotent():
    from app.database import get_db_session
    from app.models import SizeRule
    _seed_rules()
    session = get_db_session()
    count1 = session.query(SizeRule).count()
    session.close()

    _seed_rules()
    session = get_db_session()
    count2 = session.query(SizeRule).count()
    session.close()
    assert count1 == count2


# ── Test 15: extract_size uses DB rules ─────────────────────────────────────

def test_extract_size_uses_db():
    _seed_rules()
    from app.extractors import extract_size
    assert extract_size("Болт М12х50") == "M12x50"
    assert extract_size("Саморез 4,8x35") == "4.8x35"


# ── Test 16: extract_diameter uses DB rules ─────────────────────────────────

def test_extract_diameter_uses_db():
    _seed_rules()
    from app.extractors import extract_diameter
    assert extract_diameter("Болт М12х50") == "M12"


# ── Test 17: extract_length uses DB rules ───────────────────────────────────

def test_extract_length_uses_db():
    _seed_rules()
    from app.extractors import extract_length
    assert extract_length("Болт М12х50") == "50"


# ── Test 18: CRUD routes ───────────────────────────────────────────────────

def test_size_rules_crud():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    # List page
    resp = client.get("/size-rules")
    assert resp.status_code == 200
    assert "Правила распознавания размеров" in resp.text

    # Create
    resp2 = client.post("/size-rules/new", data={
        "pattern_raw": r"testpat(?P<d>\d+)",
        "match_type": "regex",
        "size_kind": "custom",
        "normalize_template": "T{d}",
        "priority": "50",
        "is_active": "true",
        "note": "",
    })
    assert resp2.status_code == 200

    # Verify listed
    resp3 = client.get("/size-rules")
    assert "testpat" in resp3.text

    # Edit form
    import re
    m = re.search(r"/size-rules/(\d+)/edit", resp3.text)
    assert m
    rid = int(m.group(1))

    resp4 = client.get(f"/size-rules/{rid}/edit")
    assert resp4.status_code == 200

    # Update
    resp5 = client.post(f"/size-rules/{rid}/edit", data={
        "pattern_raw": r"testpat2(?P<d>\d+)",
        "match_type": "regex",
        "size_kind": "diameter",
        "normalize_template": "T2{d}",
        "priority": "60",
        "is_active": "true",
        "note": "updated",
    })
    assert resp5.status_code == 200

    # Toggle
    resp6 = client.post(f"/size-rules/{rid}/toggle")
    assert resp6.status_code == 200

    # Delete
    resp7 = client.post(f"/size-rules/{rid}/delete")
    assert resp7.status_code == 200
    assert "testpat2" not in resp7.text


# ── Test 19: bare NxL (no M prefix) → M-prefixed size ────────────────────

class TestBareNxLSizeExtraction:
    """Sizes like '8*20', '14x130' without M prefix → M8x20, M14x130."""

    def test_bare_star_notation(self):
        _seed_rules()
        from app.extractors import extract_size
        assert extract_size("Болт 8*20 8,8 ГОСТ 7798-70") == "M8x20"

    def test_bare_star_large(self):
        _seed_rules()
        from app.extractors import extract_size
        assert extract_size("Болт 22*150 8,8 ГОСТ 7798-70") == "M22x150"

    def test_bare_with_other_text(self):
        _seed_rules()
        from app.extractors import extract_size
        assert extract_size("Болт 14*130 8,8 кл.пр. 7798-70 б/п кг") == "M14x130"

    def test_m_prefix_unchanged(self):
        """M12x60 already has prefix → stays M12x60."""
        _seed_rules()
        from app.extractors import extract_size
        assert extract_size("Болт M12x60 ГОСТ 7798-70") == "M12x60"

    def test_screw_decimal_no_m_prefix(self):
        """4.2x70 (screw) has decimal → no M prefix added."""
        _seed_rules()
        from app.extractors import extract_size
        assert extract_size("Саморез 4.2x70") == "4.2x70"

    def test_ensure_m_prefix_unit(self):
        from app.extractors import _ensure_m_prefix
        assert _ensure_m_prefix("8x20") == "M8x20"
        assert _ensure_m_prefix("22x150") == "M22x150"
        assert _ensure_m_prefix("M12x60") == "M12x60"
        assert _ensure_m_prefix("4.2x70") == "4.2x70"
        assert _ensure_m_prefix("") == ""
        assert _ensure_m_prefix("125x1.6x22") == "M125x1.6x22"


# ── Test 19: M16-6H tolerance ───────────────────────────────────────────────

def test_m16_6h():
    _seed_rules()
    result = _detect("Гайка M16-6H DIN 934")
    assert result is not None
    assert result.size_kind == "thread"
    assert result.diameter == "M16"
    assert result.tolerance == "6H"
    assert result.size_norm == "M16-6H"


# ── Test 20: Screw 6.3x64 ──────────────────────────────────────────────────

def test_screw_6_3x64():
    _seed_rules()
    result = _detect("Саморез 6.3x64 DIN 7504")
    assert result is not None
    assert result.size_norm == "6.3x64"
    assert result.diameter == "6.3"
    assert result.length == "64"

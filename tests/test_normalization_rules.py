"""Tests for unified normalization rules system."""

import json
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


def _seed():
    from app.seed import seed_default_normalization_rules
    seed_default_normalization_rules()


def _detect_coating(text):
    from app.services.normalization_service import detect_coating, load_rules
    rules = load_rules("coating")
    return detect_coating(text, rules=rules)


def _detect_strength(text):
    from app.services.normalization_service import detect_strength, load_rules
    rules = load_rules("strength")
    return detect_strength(text, rules=rules)


def _detect_size(text):
    from app.services.normalization_service import detect_size, load_rules
    rules = load_rules("size")
    return detect_size(text, rules=rules)


# ── Coating detection ────────────────────────────────────────────────────────

def test_coating_zn():
    _seed()
    result = _detect_coating("Болт М12 Zn DIN 933")
    assert result is not None
    assert result.normalized_code == "zinc"
    assert result.normalized_name == "цинк"


def test_coating_galvanized():
    _seed()
    result = _detect_coating("Bolt galvanized M10")
    assert result is not None
    assert result.normalized_code == "zinc"


def test_coating_no_coating():
    _seed()
    result = _detect_coating("Болт М12 б/п")
    assert result is not None
    assert result.normalized_code == "none"
    assert result.normalized_name == "без покрытия"


def test_coating_not_found():
    _seed()
    result = _detect_coating("Болт М12 DIN 933")
    assert result is None


# ── Strength detection ───────────────────────────────────────────────────────

def test_strength_8_8():
    _seed()
    result = _detect_strength("Болт DIN931 M12 8.8 Zn")
    assert result is not None
    assert result.normalized_name == "8.8"
    assert result.extra.get("family") == "metric"


def test_strength_comma():
    _seed()
    result = _detect_strength("Болт М12 кл.пр. 8,8")
    assert result is not None
    assert result.normalized_name == "8.8"


def test_strength_a2_70():
    _seed()
    result = _detect_strength("Болт A2-70 DIN 933 M8x20")
    assert result is not None
    assert result.normalized_code == "A2-70"
    assert result.extra.get("family") == "stainless"


def test_strength_a4_80():
    _seed()
    result = _detect_strength("Болт A4 80 DIN 933")
    assert result is not None
    assert result.normalized_code == "A4-80"
    assert result.extra.get("family") == "stainless"


def test_strength_not_found():
    _seed()
    result = _detect_strength("Болт М12х80 DIN 933")
    assert result is None


def test_strength_stainless_priority():
    """A2-70 should win over 8.8 when both present (higher priority)."""
    _seed()
    result = _detect_strength("Болт A2-70 8.8")
    assert result is not None
    assert result.normalized_code == "A2-70"


# ── Size detection ───────────────────────────────────────────────────────────

def test_size_m12x50():
    _seed()
    result = _detect_size("Болт М12х50 ГОСТ 7798")
    assert result is not None
    assert result.extra["size_norm"] == "M12x50"
    assert result.extra["diameter"] == "M12"
    assert result.extra["length"] == "50"
    assert result.extra["size_kind"] == "diameter_length"


def test_size_star_separator():
    _seed()
    result = _detect_size("Болт 12*50 DIN 933")
    assert result is not None
    assert result.extra["size_norm"] == "12x50"


def test_size_decimal_diameter():
    _seed()
    result = _detect_size("Саморез 4.8x35")
    assert result is not None
    assert result.extra["size_norm"] == "4.8x35"
    assert result.extra["diameter"] == "4.8"


def test_size_triple():
    _seed()
    result = _detect_size("Диск 125x1.6x22")
    assert result is not None
    assert result.extra["size_kind"] == "triple_size"
    assert result.extra["size_norm"] == "125x1.6x22"


def test_size_thread_tolerance():
    _seed()
    result = _detect_size("Гайка М20-7H")
    assert result is not None
    assert result.extra["size_kind"] == "thread"
    assert result.extra["diameter"] == "M20"
    assert result.extra["tolerance"] == "7H"


def test_size_diameter_only():
    _seed()
    result = _detect_size("Шайба М12 ГОСТ 5915")
    assert result is not None
    assert result.extra["diameter"] == "M12"
    assert result.extra["size_kind"] == "diameter"


def test_size_d_prefix():
    _seed()
    result = _detect_size("Штифт d8 ГОСТ 3128")
    assert result is not None
    assert result.extra["diameter"] == "d8"


def test_size_not_found():
    _seed()
    result = _detect_size("Герметик силиконовый")
    assert result is None


def test_size_cyrillic():
    """Cyrillic М and х should be normalized."""
    _seed()
    result = _detect_size("Болт М12х50")
    assert result is not None
    assert result.extra["size_norm"] == "M12x50"


# ── Seed idempotency ────────────────────────────────────────────────────────

def test_seed_idempotent():
    from app.database import get_db_session
    from app.models import NormalizationRule
    _seed()
    session = get_db_session()
    count1 = session.query(NormalizationRule).count()
    session.close()

    _seed()
    session = get_db_session()
    count2 = session.query(NormalizationRule).count()
    session.close()
    assert count1 == count2
    assert count1 > 0


# ── Inactive rules skipped ──────────────────────────────────────────────────

def test_inactive_rule_skipped():
    from app.database import get_db_session
    from app.models import NormalizationRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(NormalizationRule(
            rule_type="coating",
            pattern_raw="testcoating",
            match_type="contains",
            normalized_code="TEST",
            normalized_name="тест",
            priority=100,
            is_active=False,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect_coating("Болт testcoating M12")
    assert result is None


# ── CRUD routes ─────────────────────────────────────────────────────────────

def test_normalization_rules_crud():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    # List page
    resp = client.get("/normalization-rules")
    assert resp.status_code == 200
    assert "Правила нормализации" in resp.text

    # Create
    resp2 = client.post("/normalization-rules/new", data={
        "rule_type": "coating",
        "pattern_raw": "testpattern123",
        "match_type": "contains",
        "normalized_code": "TEST",
        "normalized_name": "тест",
        "extra_json": "",
        "priority": "50",
        "is_active": "true",
        "note": "",
    })
    assert resp2.status_code == 200

    # Verify listed
    resp3 = client.get("/normalization-rules")
    assert "testpattern123" in resp3.text

    # Filter by type
    resp3b = client.get("/normalization-rules?type=coating")
    assert "testpattern123" in resp3b.text

    # Edit form
    import re
    m = re.search(r"/normalization-rules/(\d+)/edit", resp3.text)
    assert m
    rid = int(m.group(1))

    resp4 = client.get(f"/normalization-rules/{rid}/edit")
    assert resp4.status_code == 200
    assert "testpattern123" in resp4.text

    # Update
    resp5 = client.post(f"/normalization-rules/{rid}/edit", data={
        "rule_type": "strength",
        "pattern_raw": "testpattern456",
        "match_type": "exact",
        "normalized_code": "TEST2",
        "normalized_name": "тест2",
        "extra_json": '{"family":"metric"}',
        "priority": "60",
        "is_active": "true",
        "note": "обновлено",
    })
    assert resp5.status_code == 200

    # Toggle
    resp6 = client.post(f"/normalization-rules/{rid}/toggle")
    assert resp6.status_code == 200

    # Delete
    resp7 = client.post(f"/normalization-rules/{rid}/delete")
    assert resp7.status_code == 200
    assert "testpattern456" not in resp7.text


# ── Unified result format ───────────────────────────────────────────────────

def test_result_format_consistent():
    """All three detectors should return NormMatch with consistent fields."""
    _seed()
    from app.services.normalization_service import NormMatch
    c = _detect_coating("Болт М12 Zn")
    assert isinstance(c, NormMatch)
    assert c.rule_type == "coating"

    s = _detect_strength("Болт 8.8")
    assert isinstance(s, NormMatch)
    assert s.rule_type == "strength"

    z = _detect_size("Болт М12х50")
    assert isinstance(z, NormMatch)
    assert z.rule_type == "size"
    assert "diameter" in z.extra
    assert "size_kind" in z.extra


# ── Integration with extractors ─────────────────────────────────────────────

def test_extract_coating_uses_unified():
    _seed()
    from app.extractors import extract_coating
    assert extract_coating("Болт М12 Zn") == "цинк"


def test_extract_strength_uses_unified():
    _seed()
    from app.extractors import extract_strength
    assert extract_strength("Болт М12 8,8") == "8.8"


def test_extract_size_uses_unified():
    _seed()
    from app.extractors import extract_size
    result = extract_size("Болт М12х50")
    assert result == "M12x50"


def test_extract_diameter_uses_unified():
    _seed()
    from app.extractors import extract_diameter
    assert extract_diameter("Болт М12х50") == "M12"


def test_extract_length_uses_unified():
    _seed()
    from app.extractors import extract_length
    assert extract_length("Болт М12х50") == "50"

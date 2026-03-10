"""Tests for DB-backed strength class detection rules."""

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
    from app.seed import seed_default_strength_rules
    seed_default_strength_rules()


def _detect(text):
    from app.services.strength_detector import detect_strength_class, load_active_strength_rules
    rules = load_active_strength_rules()
    return detect_strength_class(text, rules=rules)


# ── Test 1: "Болт DIN931 M12 8.8 Zn" → "8.8" ──────────────────────────────

def test_bolt_din931_8_8():
    _seed_rules()
    result = _detect("Болт DIN931 M12 8.8 Zn")
    assert result is not None
    assert result.strength_name == "8.8"
    assert result.strength_code == "8.8"
    assert result.strength_family == "metric"


# ── Test 2: "Болт М12 кл.пр. 8,8" → "8.8" ─────────────────────────────────

def test_bolt_klpr_8_comma_8():
    _seed_rules()
    result = _detect("Болт М12 кл.пр. 8,8")
    assert result is not None
    assert result.strength_name == "8.8"


# ── Test 3: "Болт A2-70 DIN 933 M8x20" → "A2-70" ──────────────────────────

def test_bolt_a2_70():
    _seed_rules()
    result = _detect("Болт A2-70 DIN 933 M8x20")
    assert result is not None
    assert result.strength_code == "A2-70"
    assert result.strength_name == "A2-70"
    assert result.strength_family == "stainless"


# ── Test 4: "Болт A4 80 DIN 933" → "A4-80" ─────────────────────────────────

def test_bolt_a4_80():
    _seed_rules()
    result = _detect("Болт A4 80 DIN 933")
    assert result is not None
    assert result.strength_code == "A4-80"
    assert result.strength_family == "stainless"


# ── Test 5: Класс не найден → None ──────────────────────────────────────────

def test_no_strength():
    _seed_rules()
    result = _detect("Болт М12х80 DIN 933")
    assert result is None


# ── Test 6: Приоритет — stainless побеждает metric ───────────────────────────

def test_stainless_priority_wins():
    """A2-70 should be detected even when 8.8 is also present,
    because stainless rules have higher priority."""
    _seed_rules()
    result = _detect("Болт A2-70 8.8")
    assert result is not None
    assert result.strength_code == "A2-70"
    assert result.strength_family == "stainless"


# ── Test 7: 10.9 ────────────────────────────────────────────────────────────

def test_10_9():
    _seed_rules()
    result = _detect("Шпилька М16 10.9")
    assert result is not None
    assert result.strength_name == "10.9"


# ── Test 8: 12,9 (comma) → "12.9" ──────────────────────────────────────────

def test_12_comma_9():
    _seed_rules()
    result = _detect("Болт М20 12,9")
    assert result is not None
    assert result.strength_name == "12.9"


# ── Test 9: 4.6 ─────────────────────────────────────────────────────────────

def test_4_6():
    _seed_rules()
    result = _detect("Болт М10 4.6 без покрытия")
    assert result is not None
    assert result.strength_name == "4.6"


# ── Test 10: 5,8 (comma) → "5.8" ───────────────────────────────────────────

def test_5_comma_8():
    _seed_rules()
    result = _detect("Гайка М8 кл.пр 5,8")
    assert result is not None
    assert result.strength_name == "5.8"


# ── Test 11: "класс прочности 8.8" ─────────────────────────────────────────

def test_klass_prochnosti_8_8():
    _seed_rules()
    result = _detect("Болт М12 класс прочности 8.8")
    assert result is not None
    assert result.strength_name == "8.8"


# ── Test 12: encoded tail .88 ───────────────────────────────────────────────

def test_encoded_88():
    _seed_rules()
    result = _detect("Болт М12х80.88.016")
    assert result is not None
    assert result.strength_name == "8.8"


# ── Test 13: Inactive rule skipped ──────────────────────────────────────────

def test_inactive_rule_skipped():
    from app.database import get_db_session
    from app.models import StrengthRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(StrengthRule(
            pattern_raw="teststr",
            match_type="contains",
            strength_code="TEST",
            strength_name="тест",
            strength_family="metric",
            priority=100,
            is_active=False,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect("Болт teststr M12")
    assert result is None


# ── Test 14: Seed is idempotent ─────────────────────────────────────────────

def test_seed_idempotent():
    from app.database import get_db_session
    from app.models import StrengthRule
    _seed_rules()
    session = get_db_session()
    count1 = session.query(StrengthRule).count()
    session.close()

    _seed_rules()
    session = get_db_session()
    count2 = session.query(StrengthRule).count()
    session.close()
    assert count1 == count2


# ── Test 15: extract_strength uses DB rules ─────────────────────────────────

def test_extract_strength_uses_db():
    _seed_rules()
    from app.extractors import extract_strength
    assert extract_strength("Болт М12 8,8") == "8.8"
    assert extract_strength("Болт A2-70 DIN 933") == "A2-70"


# ── Test 16: CRUD routes ───────────────────────────────────────────────────

def test_strength_rules_crud():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    # List page
    resp = client.get("/strength-rules")
    assert resp.status_code == 200
    assert "Правила распознавания классов прочности" in resp.text

    # Create
    resp2 = client.post("/strength-rules/new", data={
        "pattern_raw": "testpattern",
        "match_type": "contains",
        "strength_code": "TEST",
        "strength_name": "тест",
        "strength_family": "metric",
        "priority": "50",
        "is_active": "true",
        "note": "",
    })
    assert resp2.status_code == 200

    # Verify listed
    resp3 = client.get("/strength-rules")
    assert "testpattern" in resp3.text

    # Edit form
    import re
    m = re.search(r"/strength-rules/(\d+)/edit", resp3.text)
    assert m
    rid = int(m.group(1))

    resp4 = client.get(f"/strength-rules/{rid}/edit")
    assert resp4.status_code == 200
    assert "testpattern" in resp4.text

    # Update
    resp5 = client.post(f"/strength-rules/{rid}/edit", data={
        "pattern_raw": "testpattern2",
        "match_type": "exact",
        "strength_code": "TEST2",
        "strength_name": "тест2",
        "strength_family": "stainless",
        "priority": "60",
        "is_active": "true",
        "note": "обновлено",
    })
    assert resp5.status_code == 200

    # Toggle
    resp6 = client.post(f"/strength-rules/{rid}/toggle")
    assert resp6.status_code == 200

    # Delete
    resp7 = client.post(f"/strength-rules/{rid}/delete")
    assert resp7.status_code == 200
    assert "testpattern2" not in resp7.text

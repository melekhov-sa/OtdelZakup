"""Tests for DB-backed coating detection rules."""

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
    """Seed coating rules via the seed function."""
    from app.seed import seed_default_coating_rules
    seed_default_coating_rules()


# ── Helpers ─────────────────────────────────────────────────────────────────

def _detect(text):
    from app.services.coating_detector import detect_coating, load_active_coating_rules
    rules = load_active_coating_rules()
    return detect_coating(text, rules=rules)


# ── Test 1: "Болт DIN931 M12 8.8 Zn" → покрытие = "цинк" ──────────────────

def test_bolt_din931_zn():
    _seed_rules()
    result = _detect("Болт DIN931 - M12 1.75(Основной) 100 8.8 Zn")
    assert result is not None
    assert result.coating_name == "цинк"
    assert result.coating_code == "zinc"
    assert result.raw_match == "Zn"


# ── Test 2: "Болт М12 оцинковка" → "цинк" ──────────────────────────────────

def test_bolt_ocinkovka():
    _seed_rules()
    result = _detect("Болт М12 оцинковка")
    assert result is not None
    assert result.coating_name == "цинк"


# ── Test 3: "Болт М12 б/п" → "без покрытия" ────────────────────────────────

def test_bolt_no_coating():
    _seed_rules()
    result = _detect("Болт М12 б/п")
    assert result is not None
    assert result.coating_name == "без покрытия"
    assert result.coating_code == "none"


# ── Test 4: Покрытие не найдено → None ──────────────────────────────────────

def test_no_coating_found():
    _seed_rules()
    result = _detect("Болт М12х80 DIN 933")
    assert result is None


# ── Test 5: Приоритет правил ────────────────────────────────────────────────

def test_priority_wins():
    """Higher priority rule wins when multiple rules could match."""
    from app.database import get_db_session
    from app.models import CoatingRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(CoatingRule(
            pattern_raw="spec",
            match_type="contains",
            coating_code="special",
            coating_name="спецпокрытие",
            priority=200,
            is_active=True,
            created_at=now, updated_at=now,
        ))
        session.add(CoatingRule(
            pattern_raw="spec",
            match_type="contains",
            coating_code="other",
            coating_name="другое",
            priority=50,
            is_active=True,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect("Болт spec finish")
    assert result is not None
    assert result.coating_name == "спецпокрытие"


# ── Test 6: ZN (uppercase) → "цинк" ────────────────────────────────────────

def test_zn_uppercase():
    _seed_rules()
    result = _detect("Гайка M10 ZN")
    assert result is not None
    assert result.coating_name == "цинк"


# ── Test 7: "оцинкованный" → "цинк" ────────────────────────────────────────

def test_ocinkovanniy():
    _seed_rules()
    result = _detect("Болт оцинкованный М10х60")
    assert result is not None
    assert result.coating_name == "цинк"


# ── Test 8: "оц" (short form) → "цинк" ─────────────────────────────────────

def test_oc_short():
    _seed_rules()
    result = _detect("Гайка М8 оц.")
    assert result is not None
    assert result.coating_name == "цинк"


# ── Test 9: Inactive rule is skipped ────────────────────────────────────────

def test_inactive_rule_skipped():
    from app.database import get_db_session
    from app.models import CoatingRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(CoatingRule(
            pattern_raw="ztest",
            match_type="contains",
            coating_code="test",
            coating_name="тестовое",
            priority=100,
            is_active=False,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect("Болт ztest M12")
    assert result is None


# ── Test 10: "galvanized" → "цинк" ─────────────────────────────────────────

def test_galvanized():
    _seed_rules()
    result = _detect("Bolt M12 galvanized")
    assert result is not None
    assert result.coating_name == "цинк"


# ── Test 11: Regex match_type ───────────────────────────────────────────────

def test_regex_match_type():
    from app.database import get_db_session
    from app.models import CoatingRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(CoatingRule(
            pattern_raw=r"(?<![a-z])a[24](?![a-z])",
            match_type="regex",
            coating_code="stainless",
            coating_name="нержавейка",
            priority=100,
            is_active=True,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    result = _detect("Bolt M12 A2-70")
    assert result is not None
    assert result.coating_name == "нержавейка"


# ── Test 12: Exact match_type ───────────────────────────────────────────────

def test_exact_match_type():
    from app.database import get_db_session
    from app.models import CoatingRule
    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        session.add(CoatingRule(
            pattern_raw="zn",
            match_type="exact",
            coating_code="zinc",
            coating_name="цинк",
            priority=100,
            is_active=True,
            created_at=now, updated_at=now,
        ))
        session.commit()
    finally:
        session.close()
    # "zn" as separate word
    result = _detect("Bolt M12 zn")
    assert result is not None
    assert result.coating_name == "цинк"
    # "zn" embedded in longer word should NOT match
    result2 = _detect("Bolt M12 znavigator")
    assert result2 is None


# ── Test 13: Seed is idempotent ─────────────────────────────────────────────

def test_seed_idempotent():
    from app.database import get_db_session
    from app.models import CoatingRule
    _seed_rules()
    session = get_db_session()
    count1 = session.query(CoatingRule).count()
    session.close()

    _seed_rules()  # second call
    session = get_db_session()
    count2 = session.query(CoatingRule).count()
    session.close()
    assert count1 == count2


# ── Test 14: extract_coating from extractors uses DB rules ──────────────────

def test_extract_coating_uses_db():
    _seed_rules()
    from app.extractors import extract_coating
    assert extract_coating("Болт М12 Zn") == "цинк"
    assert extract_coating("Гайка б/п М10") == "без покрытия"


# ── Test 15: CRUD routes ───────────────────────────────────────────────────

def test_coating_rules_crud():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    # List page
    resp = client.get("/coating-rules")
    assert resp.status_code == 200
    assert "Правила распознавания покрытий" in resp.text

    # Create
    resp2 = client.post("/coating-rules/new", data={
        "pattern_raw": "тестшаблон",
        "match_type": "contains",
        "coating_code": "test",
        "coating_name": "тестовое",
        "priority": "50",
        "is_active": "true",
        "note": "",
    })
    assert resp2.status_code == 200

    # Verify it appears
    resp3 = client.get("/coating-rules")
    assert "тестшаблон" in resp3.text

    # Edit form
    import re
    m = re.search(r"/coating-rules/(\d+)/edit", resp3.text)
    assert m
    rid = int(m.group(1))

    resp4 = client.get(f"/coating-rules/{rid}/edit")
    assert resp4.status_code == 200
    assert "тестшаблон" in resp4.text

    # Update
    resp5 = client.post(f"/coating-rules/{rid}/edit", data={
        "pattern_raw": "тестшаблон2",
        "match_type": "exact",
        "coating_code": "test2",
        "coating_name": "тестовое2",
        "priority": "60",
        "is_active": "true",
        "note": "обновлено",
    })
    assert resp5.status_code == 200

    # Toggle
    resp6 = client.post(f"/coating-rules/{rid}/toggle")
    assert resp6.status_code == 200

    # Delete
    resp7 = client.post(f"/coating-rules/{rid}/delete")
    assert resp7.status_code == 200
    assert "тестшаблон2" not in resp7.text

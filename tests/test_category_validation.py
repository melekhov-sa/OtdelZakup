"""Tests for category-based validation engine.

Test cases:
1. Анкер забиваемый стальной: only diameter required → ok if diameter present
2. Болт фундаментный: no execution_type → missing
3. Шплинт DIN 11024: standard + diameter → length not required (exception)
4. Шплинт DIN 94: no coating → missing coating (exception adds coating)
5. Метрическая гайка: missing standard/coating/strength_class/diameter
"""

import json
import pytest

from datetime import datetime, timezone


# ── Test isolation fixture ────────────────────────────────────────────────────

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
    db_mod.DB_PATH      = db_path
    db_mod.engine       = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _seed_rules_and_exceptions(session):
    """Seed the base rules and exceptions needed for the 5 test cases."""
    from app.models import BaseValidationRule, ValidationRuleException
    now = datetime.now(timezone.utc)

    # Rule 1: Anchors — require type, diameter, length
    r1 = BaseValidationRule(
        id=1, category_code="anchors", category_name="Анкеры",
        required_fields=json.dumps(["type", "diameter", "length"]),
        priority=100, is_active=True, created_at=now, updated_at=now,
    )
    # Rule 2: Foundation bolts — require execution_type, standard, diameter, length
    r2 = BaseValidationRule(
        id=2, category_code="foundation_bolts", category_name="Болты фундаментные",
        required_fields=json.dumps(["execution_type", "standard", "diameter", "length"]),
        priority=100, is_active=True, created_at=now, updated_at=now,
    )
    # Rule 10: Metric nuts — require standard, strength_class, coating, diameter
    r10 = BaseValidationRule(
        id=10, category_code="metric_fasteners", category_name="Метрический крепеж",
        item_type_code="nut", item_type_name="Гайки",
        required_fields=json.dumps(["standard", "strength_class", "coating", "diameter"]),
        priority=100, is_active=True, created_at=now, updated_at=now,
    )
    # Rule 32: Pins & cotters — require standard, diameter, length
    r32 = BaseValidationRule(
        id=32, category_code="pins_cotter", category_name="Штифты и шплинты",
        required_fields=json.dumps(["standard", "steel_grade", "diameter", "length"]),
        priority=100, is_active=True, created_at=now, updated_at=now,
    )
    session.add_all([r1, r2, r10, r32])
    session.flush()

    # Exception: Anchor забиваемый стальной → only diameter
    e1 = ValidationRuleException(
        base_rule_id=1, match_type_name="анкер забиваемый стальной",
        override_required_fields=json.dumps(["diameter"]),
        note="Анкер забиваемый стальной — только диаметр",
        priority=10, is_active=True, created_at=now, updated_at=now,
    )
    # Exception: DIN 11024 → only diameter (no length)
    e2 = ValidationRuleException(
        base_rule_id=32, match_standard="DIN 11024",
        override_required_fields=json.dumps(["diameter"]),
        note="DIN 11024 — длина не требуется",
        priority=10, is_active=True, created_at=now, updated_at=now,
    )
    # Exception: DIN 94 → standard, diameter, length, coating
    e3 = ValidationRuleException(
        base_rule_id=32, match_standard="DIN 94",
        override_required_fields=json.dumps(["standard", "diameter", "length", "coating"]),
        note="DIN 94 — дополнительно обязательно покрытие",
        priority=10, is_active=True, created_at=now, updated_at=now,
    )
    session.add_all([e1, e2, e3])
    session.commit()


@pytest.fixture()
def seeded_db():
    """Seed DB with rules and exceptions, return (rules, exceptions)."""
    from app.database import get_db_session
    from app.models import BaseValidationRule, ValidationRuleException
    session = get_db_session()
    try:
        _seed_rules_and_exceptions(session)
        rules = session.query(BaseValidationRule).filter(BaseValidationRule.is_active.is_(True)).all()
        excs = session.query(ValidationRuleException).filter(ValidationRuleException.is_active.is_(True)).all()
        session.expunge_all()
        return rules, excs
    finally:
        session.close()


# ── Test 1: Анкер забиваемый стальной — exception → only diameter → ok ──────

def test_anchor_zabivnoy_steel_only_diameter(seeded_db):
    """Анкер забиваемый стальной with diameter=M10 → ok (exception removes type/length)."""
    from app.category_validator import validate_row
    rules, excs = seeded_db

    row = {
        "item_type": "анкер",
        "name_raw": "Анкер забиваемый стальной М10",
        "diameter": "10",
    }
    result = validate_row(row, rules=rules, exceptions=excs)

    assert result is not None
    assert result.category_name == "Анкеры"
    assert result.exception_note is not None
    assert "забиваемый" in result.exception_note.lower() or "диаметр" in result.exception_note.lower()
    assert result.required_fields == ["diameter"]
    assert result.missing_fields == []
    assert result.status == "ok"


# ── Test 2: Болт фундаментный — no execution_type → missing ────────────────

def test_foundation_bolt_missing_execution_type(seeded_db):
    """Болт фундаментный without execution_type → needs_review."""
    from app.category_validator import validate_row
    rules, excs = seeded_db

    row = {
        "item_type": "болт",
        "name_raw": "Болт фундаментный М20х300",
        "gost": "ГОСТ 24379.1-2012",
        "diameter": "20",
        "length": "300",
    }
    result = validate_row(row, rules=rules, exceptions=excs)

    assert result is not None
    assert result.category_name == "Болты фундаментные"
    assert "execution_type" in result.missing_fields
    assert result.status != "ok"


# ── Test 3: Шплинт DIN 11024 — exception removes length → ok ────────────────

def test_cotter_pin_din_11024_no_length_needed(seeded_db):
    """Шплинт DIN 11024 with standard+diameter → ok (exception removes length)."""
    from app.category_validator import validate_row
    rules, excs = seeded_db

    row = {
        "item_type": "шплинт",
        "name_raw": "Шплинт DIN 11024 3,2",
        "din": "DIN 11024",
        "diameter": "3.2",
    }
    result = validate_row(row, rules=rules, exceptions=excs)

    assert result is not None
    assert result.category_name == "Штифты и шплинты"
    assert result.exception_note is not None
    assert result.required_fields == ["diameter"]
    assert result.missing_fields == []
    assert result.status == "ok"


# ── Test 4: Шплинт DIN 94 — no coating → missing coating ────────────────────

def test_cotter_pin_din_94_missing_coating(seeded_db):
    """Шплинт DIN 94 without coating → needs_review (exception adds coating)."""
    from app.category_validator import validate_row
    rules, excs = seeded_db

    row = {
        "item_type": "шплинт",
        "name_raw": "Шплинт DIN 94 2x20",
        "din": "DIN 94",
        "diameter": "2",
        "length": "20",
        # coating is missing
    }
    result = validate_row(row, rules=rules, exceptions=excs)

    assert result is not None
    assert result.category_name == "Штифты и шплинты"
    assert result.exception_note is not None
    assert "coating" in result.required_fields
    assert "coating" in result.missing_fields
    assert result.status == "needs_review"


# ── Test 5: Метрическая гайка — missing many fields ──────────────────────────

def test_metric_nut_missing_multiple_fields(seeded_db):
    """Гайка with no standard/coating/strength/diameter → manual_required."""
    from app.category_validator import validate_row
    rules, excs = seeded_db

    row = {
        "item_type": "гайка",
        "name_raw": "Гайка",
    }
    result = validate_row(row, rules=rules, exceptions=excs)

    assert result is not None
    assert result.category_name == "Метрический крепеж"
    assert "standard" in result.missing_fields
    assert "strength_class" in result.missing_fields
    assert "coating" in result.missing_fields
    assert "diameter" in result.missing_fields
    assert result.status == "manual_required"  # diameter missing → manual_required


# ── Test 6: classify_row returns None for unknown text ──────────────────────

def test_classify_row_unknown_returns_none():
    """Unknown product type → classify returns None → validate_row returns None."""
    from app.category_validator import classify_row, validate_row

    row = {"item_type": "", "name_raw": "Непонятный товар XYZ"}
    assert classify_row(row) == (None, None, None)

    # validate_row should return None (fallback to old logic)
    result = validate_row(row, rules=[], exceptions=[])
    assert result is None


# ── Test 7: Stainless bolt classified correctly ──────────────────────────────

def test_stainless_bolt_classified():
    """Bolt with A2 steel grade → stainless category."""
    from app.category_validator import classify_row

    row = {
        "item_type": "болт",
        "name_raw": "Болт М8х20 A2-70",
        "steel_grade": "A2",
    }
    cat, subcat, type_code = classify_row(row)
    assert cat == "stainless_fasteners"
    assert subcat is None
    assert type_code == "bolt_screw_stud"


# ── Test 8: Seed function is idempotent ──────────────────────────────────────

def test_seed_idempotent():
    """Running seed twice creates rules only once."""
    from app.seed import seed_initial_validation_rules
    from app.database import get_db_session
    from app.models import BaseValidationRule, ValidationRuleException

    r1 = seed_initial_validation_rules()
    assert r1["created"] > 0

    session = get_db_session()
    count1 = session.query(BaseValidationRule).count()
    exc_count1 = session.query(ValidationRuleException).count()
    session.close()

    # Second run — no new records
    r2 = seed_initial_validation_rules()
    assert r2["created"] == 0
    assert r2["exceptions"] == 0

    session = get_db_session()
    count2 = session.query(BaseValidationRule).count()
    exc_count2 = session.query(ValidationRuleException).count()
    session.close()

    assert count1 == count2
    assert exc_count1 == exc_count2


# ── Test 9: Seed creates expected number of rules ────────────────────────────

def test_seed_creates_expected_counts():
    """Seed creates 29 rules and 6 exceptions."""
    from app.seed import seed_initial_validation_rules

    result = seed_initial_validation_rules()
    assert result["created"] == 29
    assert result["exceptions"] == 6

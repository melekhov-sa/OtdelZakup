"""Tests for BaseValidationRule + ValidationRuleException CRUD routes."""

import re

import pytest
from fastapi.testclient import TestClient


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


@pytest.fixture()
def client():
    from app.main import app
    return TestClient(app)


# ── Test 1: List page renders (empty) ────────────────────────────────────────

def test_category_rules_list_renders(client):
    resp = client.get("/validation-rules")
    assert resp.status_code == 200
    assert "Правила проверки заявок" in resp.text


# ── Test 2: Full CRUD for a rule ─────────────────────────────────────────────

def test_category_rule_crud(client):
    # Create
    resp = client.post(
        "/validation-rules/new",
        data={
            "category_code": "metric",
            "category_name": "Метрический крепеж",
            "subcategory_code": "",
            "subcategory_name": "",
            "item_type_code": "nut",
            "item_type_name": "Гайки",
            "required_fields": ["standard", "diameter", "coating"],
            "priority": "10",
            "is_active": "true",
        },
    )
    assert resp.status_code == 200  # follows redirect
    assert "Метрический крепеж" in resp.text
    assert "Гайки" in resp.text

    # Find rule id
    m = re.search(r"/validation-rules/(\d+)/edit", resp.text)
    assert m, "edit link not found"
    rule_id = int(m.group(1))

    # Edit form loads
    resp2 = client.get(f"/validation-rules/{rule_id}/edit")
    assert resp2.status_code == 200
    assert "Метрический крепеж" in resp2.text

    # Update
    resp3 = client.post(
        f"/validation-rules/{rule_id}/edit",
        data={
            "category_code": "metric",
            "category_name": "Метрический крепеж v2",
            "subcategory_code": "",
            "subcategory_name": "",
            "item_type_code": "nut",
            "item_type_name": "Гайки",
            "required_fields": ["standard", "diameter"],
            "priority": "20",
            "is_active": "true",
        },
    )
    assert resp3.status_code == 200
    assert "Метрический крепеж v2" in resp3.text

    # Toggle
    resp4 = client.post(f"/validation-rules/{rule_id}/toggle")
    assert resp4.status_code == 200

    # Delete
    resp5 = client.post(f"/validation-rules/{rule_id}/delete")
    assert resp5.status_code == 200
    assert "Метрический крепеж v2" not in resp5.text


# ── Test 3: required_fields saved as JSON ────────────────────────────────────

def test_category_rule_saves_required_fields(client):
    """Verify required_fields are correctly persisted."""
    # Create rule with multiple fields
    client.post(
        "/validation-rules/new",
        data={
            "category_code": "anchors",
            "category_name": "Анкеры",
            "required_fields": ["type", "diameter", "length"],
            "priority": "10",
            "is_active": "true",
        },
    )

    # Check DB directly
    from app.database import get_db_session
    from app.models import BaseValidationRule
    session = get_db_session()
    try:
        rule = session.query(BaseValidationRule).filter(
            BaseValidationRule.category_code == "anchors"
        ).first()
        assert rule is not None
        assert set(rule.required_fields_list) == {"type", "diameter", "length"}
    finally:
        session.close()


# ── Test 4: Exception CRUD ───────────────────────────────────────────────────

def test_exception_crud(client):
    # First create a base rule
    client.post(
        "/validation-rules/new",
        data={
            "category_code": "pins_cotters",
            "category_name": "Штифты шплинты",
            "required_fields": ["standard", "diameter", "length"],
            "priority": "10",
            "is_active": "true",
        },
    )

    # Get rule id
    resp = client.get("/validation-rules")
    m = re.search(r"/validation-rules/(\d+)/exceptions", resp.text)
    assert m
    rule_id = int(m.group(1))

    # Exceptions page renders
    resp2 = client.get(f"/validation-rules/{rule_id}/exceptions")
    assert resp2.status_code == 200
    assert "Штифты шплинты" in resp2.text

    # Create exception
    resp3 = client.post(
        f"/validation-rules/{rule_id}/exceptions/new",
        data={
            "match_type_name": "",
            "match_standard": "DIN 11024",
            "override_required_fields": ["diameter"],
            "note": "DIN 11024 — длина не требуется",
            "priority": "10",
            "is_active": "true",
        },
    )
    assert resp3.status_code == 200
    assert "DIN 11024" in resp3.text

    # Find exception id
    m2 = re.search(rf"/validation-rules/{rule_id}/exceptions/(\d+)/edit", resp3.text)
    assert m2
    exc_id = int(m2.group(1))

    # Edit form loads
    resp4 = client.get(f"/validation-rules/{rule_id}/exceptions/{exc_id}/edit")
    assert resp4.status_code == 200
    assert "DIN 11024" in resp4.text

    # Update
    resp5 = client.post(
        f"/validation-rules/{rule_id}/exceptions/{exc_id}/edit",
        data={
            "match_standard": "DIN 11024",
            "override_required_fields": ["diameter"],
            "note": "DIN 11024 — обновлено",
            "priority": "20",
            "is_active": "true",
        },
    )
    assert resp5.status_code == 200
    assert "обновлено" in resp5.text

    # Toggle
    resp6 = client.post(f"/validation-rules/{rule_id}/exceptions/{exc_id}/toggle")
    assert resp6.status_code == 200

    # Delete exception
    resp7 = client.post(f"/validation-rules/{rule_id}/exceptions/{exc_id}/delete")
    assert resp7.status_code == 200
    assert "DIN 11024" not in resp7.text


# ── Test 5: Exception overrides base rule required_fields ─────────────────

def test_exception_overrides_base_rule():
    """Integration test: exception replaces base rule fields in validation."""
    import json
    from datetime import datetime, timezone
    from app.database import get_db_session
    from app.models import BaseValidationRule, ValidationRuleException
    from app.category_validator import validate_row

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = BaseValidationRule(
            category_code="anchors", category_name="Анкеры",
            required_fields=json.dumps(["type", "diameter", "length"]),
            priority=10, is_active=True, created_at=now, updated_at=now,
        )
        session.add(rule)
        session.flush()

        exc = ValidationRuleException(
            base_rule_id=rule.id,
            match_type_name="анкер забиваемый стальной",
            override_required_fields=json.dumps(["diameter"]),
            note="Только диаметр", priority=10, is_active=True,
            created_at=now, updated_at=now,
        )
        session.add(exc)
        session.commit()

        rules = session.query(BaseValidationRule).all()
        excs = session.query(ValidationRuleException).all()
        session.expunge_all()
    finally:
        session.close()

    # Without exception match — all 3 fields present → ok, no exception
    row_generic = {"item_type": "анкер", "name_raw": "Анкер клиновой М12x100", "diameter": "12", "length": "100"}
    result = validate_row(row_generic, rules=rules, exceptions=excs)
    assert result is not None
    assert result.exception_note is None
    assert result.required_fields == ["type", "diameter", "length"]
    assert result.status == "ok"  # all fields present (type → item_type = "анкер")

    # With exception match — only diameter required
    row_zabiv = {"item_type": "анкер", "name_raw": "Анкер забиваемый стальной М10", "diameter": "10"}
    result2 = validate_row(row_zabiv, rules=rules, exceptions=excs)
    assert result2 is not None
    assert result2.required_fields == ["diameter"]
    assert result2.missing_fields == []
    assert result2.exception_note == "Только диаметр"
    assert result2.status == "ok"


# ── Test 6: Delete rule cascades exceptions ──────────────────────────────────

def test_delete_rule_cascades_exceptions(client):
    """Deleting a rule should also remove its exceptions."""
    # Create rule
    client.post(
        "/validation-rules/new",
        data={
            "category_code": "test_cascade",
            "category_name": "Тест каскад",
            "required_fields": ["diameter"],
            "priority": "5",
            "is_active": "true",
        },
    )
    resp = client.get("/validation-rules")
    m = re.search(r"/validation-rules/(\d+)/edit", resp.text)
    rule_id = int(m.group(1))

    # Create exception
    client.post(
        f"/validation-rules/{rule_id}/exceptions/new",
        data={
            "match_standard": "TEST-123",
            "override_required_fields": ["diameter"],
            "note": "test exc",
            "priority": "5",
            "is_active": "true",
        },
    )

    # Verify exception exists
    from app.database import get_db_session
    from app.models import ValidationRuleException
    session = get_db_session()
    count_before = session.query(ValidationRuleException).filter(
        ValidationRuleException.base_rule_id == rule_id
    ).count()
    session.close()
    assert count_before == 1

    # Delete rule
    client.post(f"/validation-rules/{rule_id}/delete")

    # Verify exception also deleted
    session = get_db_session()
    count_after = session.query(ValidationRuleException).filter(
        ValidationRuleException.base_rule_id == rule_id
    ).count()
    session.close()
    assert count_after == 0

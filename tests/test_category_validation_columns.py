"""Tests for validation status/missing columns in results table."""

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


# ── Test 1: format_missing_fields empty list → empty string ────────────────

def test_format_missing_fields_empty():
    from app.category_validator import format_missing_fields
    assert format_missing_fields([]) == ""


# ── Test 2: format_missing_fields translates to Russian labels ──────────────

def test_format_missing_fields_russian_labels():
    from app.category_validator import format_missing_fields
    result = format_missing_fields(["diameter", "coating", "strength_class"])
    assert result == "Диаметр, Покрытие, Класс прочности"


# ── Test 3: format_missing_fields unknown key passes through ────────────────

def test_format_missing_fields_unknown_key():
    from app.category_validator import format_missing_fields
    result = format_missing_fields(["diameter", "some_unknown"])
    assert "Диаметр" in result
    assert "some_unknown" in result


# ── Test 4: status_label returns Russian labels ─────────────────────────────

def test_status_label():
    from app.category_validator import status_label
    assert status_label("ok") == "ОК"
    assert status_label("needs_review") == "Уточнить"
    assert status_label("manual_required") == "Заполнить вручную"
    assert status_label("unknown") == "unknown"


# ── Test 5: _add_category_validation_columns adds columns from traces ──────

def test_add_category_validation_columns():
    import pandas as pd
    from app.main import _add_category_validation_columns

    df = pd.DataFrame({"name": ["A", "B", "C"]})
    traces = [
        {"category_validation": {
            "available": True, "status": "ok",
            "missing_field_keys": [],
        }},
        {"category_validation": {
            "available": True, "status": "needs_review",
            "missing_field_keys": ["coating"],
        }},
        {"category_validation": {"available": False}},
    ]
    _add_category_validation_columns(df, traces)

    assert list(df["validation_status"]) == ["ok", "needs_review", ""]
    assert df["validation_missing"].iloc[0] == ""
    assert df["validation_missing"].iloc[1] == "Покрытие"
    assert df["validation_missing"].iloc[2] == ""


# ── Test 6: _render_validation_status_cell produces colored badges ──────────

def test_render_validation_status_cell():
    from app.main import _render_validation_status_cell

    ok_html = _render_validation_status_cell("ok")
    assert "ОК" in ok_html
    assert "#2e7d32" in ok_html  # green

    review_html = _render_validation_status_cell("needs_review")
    assert "Уточнить" in review_html
    assert "#e65100" in review_html  # orange

    manual_html = _render_validation_status_cell("manual_required")
    assert "Заполнить" in manual_html
    assert "#c62828" in manual_html  # red

    empty_html = _render_validation_status_cell("")
    assert empty_html == "<td></td>"


# ── Test 7: validate_row produces correct status for each scenario ──────────

def test_validate_row_status_scenarios():
    """End-to-end: validate_row status matches expected for different inputs."""
    from app.database import get_db_session
    from app.models import BaseValidationRule, ValidationRuleException
    from app.category_validator import validate_row

    now = datetime.now(timezone.utc)
    session = get_db_session()
    try:
        rule = BaseValidationRule(
            category_code="metric_fasteners", category_name="Метрический крепеж",
            item_type_code="nut", item_type_name="Гайки",
            required_fields=json.dumps(["standard", "coating", "diameter"]),
            priority=100, is_active=True, created_at=now, updated_at=now,
        )
        session.add(rule)
        session.commit()
        rules = session.query(BaseValidationRule).all()
        session.expunge_all()
    finally:
        session.close()

    # All present → ok
    row_ok = {"item_type": "гайка", "name_raw": "Гайка DIN 934 M10 цинк",
              "din": "DIN 934", "coating": "цинк", "diameter": "10"}
    r = validate_row(row_ok, rules=rules, exceptions=[])
    assert r.status == "ok"
    assert r.missing_fields == []

    # Missing coating → needs_review
    row_review = {"item_type": "гайка", "name_raw": "Гайка DIN 934 M10",
                  "din": "DIN 934", "diameter": "10"}
    r2 = validate_row(row_review, rules=rules, exceptions=[])
    assert r2.status == "needs_review"
    assert "coating" in r2.missing_fields

    # Missing diameter → manual_required
    row_manual = {"item_type": "гайка", "name_raw": "Гайка"}
    r3 = validate_row(row_manual, rules=rules, exceptions=[])
    assert r3.status == "manual_required"
    assert "diameter" in r3.missing_fields

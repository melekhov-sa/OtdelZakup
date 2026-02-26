"""Tests for size inference rules engine."""

import pytest
from unittest.mock import MagicMock


# ── Test isolation fixture (mirrors other test files) ─────────────────────────

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


# ── Helper: build a mock rule ─────────────────────────────────────────────────

def _make_rule(item_types, mode, name="Тестовое правило", rule_id=1):
    rule = MagicMock()
    rule.item_types_list = item_types
    rule.mode = mode
    rule.name = name
    rule.id = rule_id
    return rule


# ── Test 1: nut with diameter → DIAMETER_AS_SIZE fills size ──────────────────

def test_size_inference_nut_diameter_as_size():
    """Nut row with diameter extracted: rule DIAMETER_AS_SIZE sets size = diameter."""
    from app.size_inference import apply_size_inference

    rule = _make_rule(["гайка"], "DIAMETER_AS_SIZE", "Гайка: размер = диаметр")

    row_dict = {"item_type": "гайка", "diameter": "M20", "length": "", "size": ""}
    updated, trace = apply_size_inference(row_dict, [rule])

    assert updated["size"] == "M20"
    assert trace["applied"] is True
    assert trace["result_size"] == "M20"
    assert trace["mode"] == "DIAMETER_AS_SIZE"
    assert "M20" in trace["reason"]


# ── Test 2: existing size must NOT be overridden by inference ─────────────────

def test_size_inference_does_not_override_existing_size():
    """When size is already present, inference must not modify it."""
    from app.size_inference import apply_size_inference

    rule = _make_rule(["гайка"], "DIAMETER_AS_SIZE")

    row_dict = {"item_type": "гайка", "diameter": "M12", "length": "", "size": "M12x150"}
    updated, trace = apply_size_inference(row_dict, [rule])

    assert updated["size"] == "M12x150"
    assert trace["applied"] is False


# ── Test 3: full pipeline — readiness satisfied by inference for nut ──────────

def test_readiness_size_requirement_satisfied_by_inference_for_nut():
    """Nut row with M20 diameter, no explicit size: inference fills size → readiness ok."""
    import io
    import re

    import pandas as pd
    from fastapi.testclient import TestClient

    from app.database import get_db_session
    from app.models import ReadinessRule, SizeInferenceRule
    from app.seed import seed_default_inference_rules

    # Insert a readiness rule requiring [size, qty] for гайка
    session = get_db_session()
    try:
        rr = ReadinessRule(
            name="Гайка",
            description="Гайка: размер и количество",
            item_type="гайка",
            priority=10,
            is_active=True,
        )
        rr.require_fields_list = ["size", "qty"]
        session.add(rr)
        session.commit()
    finally:
        session.close()

    # Seed default inference rules (includes DIAMETER_AS_SIZE for гайка)
    seed_default_inference_rules()

    from app.main import app as fastapi_app

    client = TestClient(fastapi_app)

    # Row: гайка М20, qty given as combined "50 шт" so RowParser can parse qty+uom
    buf = io.BytesIO()
    pd.DataFrame([{"Наименование": "Гайка М20 DIN 934", "Количество": "50 шт"}]).to_excel(
        buf, index=False, engine="openpyxl"
    )
    buf.seek(0)
    resp = client.post(
        "/upload",
        files={"file": ("test.xlsx", buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert resp.status_code == 200
    m = re.search(r'name="file_id"\s+value="([^"]+)"', resp.text)
    assert m, "file_id not found"
    file_id = m.group(1)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["size", "diameter", "item_type"]},
    )
    assert resp2.status_code == 200
    # With inference active, size = M20 (from diameter), so status should NOT be manual.
    # Check table rows specifically (the CSS also contains the string data-status="manual").
    assert '<tr data-status="manual">' not in resp2.text, (
        "Expected nut with M20 diameter to pass readiness via size inference"
    )
    assert '<tr data-status="ok">' in resp2.text

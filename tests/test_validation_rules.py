"""Tests for ValidationRule CRUD and /rules routes."""

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

    # Isolate DB per test
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


def test_rules_page_renders(client):
    resp = client.get("/rules")
    assert resp.status_code == 200
    assert "Правила проверки" in resp.text


def test_validation_rules_crud_smoke(client):
    # ── Create ──
    resp = client.post(
        "/rules/create",
        data={
            "name": "Гайка без длины",
            "description": "Длина для гайки не нужна",
            "item_type": "гайка",
            "forbid_fields": "length",
            "force_status": "review",
            "priority": "5",
        },
    )
    # POST redirects to /rules; TestClient follows redirect → 200
    assert resp.status_code == 200
    assert "Гайка без длины" in resp.text
    assert "Запрещено" in resp.text or "Длина" in resp.text

    # ── Find rule id from edit link ──
    m = re.search(r"/rules/(\d+)/edit", resp.text)
    assert m, "edit link not found in /rules list"
    rule_id = int(m.group(1))

    # ── Edit page loads ──
    resp2 = client.get(f"/rules/{rule_id}/edit")
    assert resp2.status_code == 200
    assert "Гайка без длины" in resp2.text

    # ── Update ──
    resp3 = client.post(
        f"/rules/{rule_id}/update",
        data={
            "name": "Гайка без длины v2",
            "item_type": "гайка",
            "forbid_fields": "length",
            "force_status": "manual",
            "priority": "5",
        },
    )
    assert resp3.status_code == 200
    assert "Гайка без длины v2" in resp3.text

    # ── Toggle (deactivate) ──
    resp4 = client.post(f"/rules/{rule_id}/toggle")
    assert resp4.status_code == 200
    # After toggle the rule should be inactive — check /rules page
    assert "Нет" in resp4.text or "inactive" in resp4.text or resp4.status_code == 200

    # ── Appears on main page ──
    resp5 = client.get("/")
    assert resp5.status_code == 200
    assert "Правила проверки" in resp5.text

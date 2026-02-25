import io

import pandas as pd
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


def _make_xlsx(rows: list[dict]) -> io.BytesIO:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _upload_and_transform(client, rows, fields):
    """Upload xlsx, extract file_id, transform with given fields, return HTML."""
    import re

    xlsx = _make_xlsx(rows)
    resp = client.post(
        "/upload",
        files={"file": ("test.xlsx", xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert resp.status_code == 200
    m = re.search(r'name="file_id"\s+value="([^"]+)"', resp.text)
    assert m, "file_id not found"
    file_id = m.group(1)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": fields},
    )
    assert resp2.status_code == 200
    return resp2.text


def _seed():
    """Insert default readiness rules into the test DB."""
    from app.seed import seed_default_rules

    seed_default_rules()


# ── 1. Washer with size+qty → ok ────────────────────────────


def test_readiness_ok_for_washer_size_qty(client):
    _seed()
    rows = [{"Код": "001", "Номенклатура": "Шайба М12 ГОСТ 11371-78", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["size", "item_type"])
    assert 'data-status="ok"' in html


# ── 2. Bolt with custom rule requiring strength → review ─────


def test_readiness_review_when_missing_strength_if_rule_requires(client):
    from app.database import get_db_session
    from app.models import ReadinessRule

    # Create a custom rule requiring size+qty+strength for болт
    session = get_db_session()
    try:
        rule = ReadinessRule(
            name="Болт с прочностью",
            description="Болт: размер, количество, класс прочности",
            item_type="болт",
            priority=5,
            is_active=True,
        )
        rule.require_fields_list = ["size", "qty", "strength"]
        session.add(rule)
        session.commit()
    finally:
        session.close()

    # Bolt with size and qty but no strength
    rows = [{"Код": "001", "Номенклатура": "Болт М12x80 оц", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["size", "strength", "item_type"])
    assert 'data-status="review"' in html
    assert "Класс прочности" in html  # reason column


# ── 3. Nut without size → manual ────────────────────────────


def test_readiness_manual_when_missing_size(client):
    _seed()
    # Nut without any recognizable size
    rows = [{"Код": "001", "Номенклатура": "Гайка специальная", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["size", "item_type"])
    assert 'data-status="manual"' in html
    assert "Размер" in html  # reason column


# ── 4. Readiness page renders ───────────────────────────────


def test_readiness_page_renders(client):
    _seed()
    resp = client.get("/readiness")
    assert resp.status_code == 200
    html = resp.text
    assert "Правила готовности" in html
    assert "По умолчанию" in html

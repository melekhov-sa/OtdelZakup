"""Routes of the settings snapshot page."""
import json

import pytest
from fastapi.testclient import TestClient

from app.models import StandardEquivalent


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


@pytest.fixture
def client():
    from fastapi import FastAPI
    from app.settings_backup_routes import settings_backup_router
    app = FastAPI()
    app.include_router(settings_backup_router)
    return TestClient(app)


def _add_equiv(src="GOST-7798-70", dst="DIN-933"):
    from app.database import get_db_session
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()


def test_page_opens(client):
    response = client.get("/settings/backup")
    assert response.status_code == 200
    assert "Выгрузить" in response.text
    assert "Загрузить" in response.text


def test_export_returns_a_named_file(client):
    _add_equiv()
    response = client.get("/settings/backup/export")
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    assert "otdelzakup-settings-" in response.headers["content-disposition"]
    payload = json.loads(response.text)
    assert payload["tables"]["standard_equivalents"][0]["src_canonical"] == "GOST-7798-70"


def test_import_replaces_data(client):
    _add_equiv("GOST-5927-70", "DIN-934")
    snapshot = json.loads(client.get("/settings/backup/export").text)

    _add_equiv("GOST-7798-70", "DIN-933")

    files = {"file": ("snap.json", json.dumps(snapshot).encode("utf-8"), "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200

    from app.database import get_db_session
    session = get_db_session()
    pairs = {(r.src_canonical, r.dst_canonical) for r in session.query(StandardEquivalent).all()}
    session.close()
    assert pairs == {("GOST-5927-70", "DIN-934")}


def test_import_of_broken_json_reports_an_error(client):
    _add_equiv()
    files = {"file": ("snap.json", b"{not json", "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200
    assert "не читается" in response.text

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_import_of_future_format_reports_an_error(client):
    _add_equiv()
    payload = {"format_version": 999, "tables": {}}
    files = {"file": ("snap.json", json.dumps(payload).encode("utf-8"), "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200
    assert "не поддерживается" in response.text

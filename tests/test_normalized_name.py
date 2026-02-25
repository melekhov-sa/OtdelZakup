"""Tests for the normalized name feature (Step 8)."""

import io
import re

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

    # Seed default template
    from app.seed import seed_default_template

    seed_default_template()


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


def _make_xlsx(rows: list[dict]) -> io.BytesIO:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _upload_file(client, rows, filename="test.xlsx"):
    xlsx = _make_xlsx(rows)
    return client.post(
        "/upload",
        files={"file": (filename, xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )


def _extract_file_id(html: str) -> str:
    m = re.search(r'name="file_id"\s+value="([^"]+)"', html)
    assert m, "file_id hidden input not found"
    return m.group(1)


def _first_full_download_url(html: str) -> str:
    """Return first download URL that is not the ok-only variant."""
    links = re.findall(r'href="(/download/[^"]+)"', html)
    assert links, "No download link found in HTML"
    return next((lnk for lnk in links if "__ok_only__" not in lnk), links[0])


_BOLT_ROW = [{"Код": "001", "Номенклатура": "Болт M12x80 8.8 оц ГОСТ 7798-70", "Заказ": 10}]


# ── 1. Column hidden by default ─────────────────────────────


def test_normalized_column_hidden_by_default(client):
    resp = _upload_file(client, _BOLT_ROW)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["size", "strength"]},
    )
    assert resp2.status_code == 200
    assert "Нормализованное наименование" not in resp2.text


# ── 2. Column shown when enabled ────────────────────────────


def test_normalized_column_shown_when_enabled(client):
    resp = _upload_file(client, _BOLT_ROW)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["size", "strength", "normalized_name"]},
    )
    assert resp2.status_code == 200
    html = resp2.text
    assert "Нормализованное наименование" in html
    # Default template: "{item_type} {size} {strength} {standard}"
    # For "Болт M12x80 8.8 оц ГОСТ 7798-70" the column should contain "болт" and "M12"
    assert "болт" in html.lower()
    assert "M12" in html


# ── 3. Excel export respects normalized option ──────────────


def test_export_respects_normalized_option(client):
    # Without normalized_name — column must be absent from xlsx
    resp = _upload_file(client, _BOLT_ROW)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)

    resp2 = client.post("/transform", data={"file_id": file_id, "fields": ["size", "strength"]})
    assert resp2.status_code == 200
    dl_url = _first_full_download_url(resp2.text)
    df_no = pd.read_excel(io.BytesIO(client.get(dl_url).content), engine="openpyxl")
    assert "Нормализованное наименование" not in df_no.columns

    # With normalized_name — column must be present in xlsx
    resp3 = _upload_file(client, _BOLT_ROW, filename="test2.xlsx")
    assert resp3.status_code == 200
    file_id2 = _extract_file_id(resp3.text)

    resp4 = client.post(
        "/transform",
        data={"file_id": file_id2, "fields": ["size", "strength", "normalized_name"]},
    )
    assert resp4.status_code == 200
    dl_url2 = _first_full_download_url(resp4.text)
    df_yes = pd.read_excel(io.BytesIO(client.get(dl_url2).content), engine="openpyxl")
    assert "Нормализованное наименование" in df_yes.columns


# ── 4. Switching active template changes column content ──────


def test_template_switch_active(client):
    # Deactivate default template and create a short one (no {standard})
    import app.database as db_mod
    from app.models import NameTemplate

    session = db_mod.get_db_session()
    try:
        session.query(NameTemplate).update({"is_active": False})
        tpl = NameTemplate(
            name="Короткий",
            template_string="{item_type} {size}",
            is_active=True,
            priority=1,
        )
        session.add(tpl)
        session.commit()
    finally:
        session.close()

    resp = _upload_file(client, _BOLT_ROW)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["size", "strength", "normalized_name"]},
    )
    assert resp2.status_code == 200
    dl_url = _first_full_download_url(resp2.text)
    df = pd.read_excel(io.BytesIO(client.get(dl_url).content), engine="openpyxl")

    assert "Нормализованное наименование" in df.columns
    val = str(df["Нормализованное наименование"].iloc[0])
    # Short template "{item_type} {size}" — standard (ГОСТ) must be absent
    assert "ГОСТ" not in val
    # But item_type (болт) must be present
    assert "болт" in val.lower()

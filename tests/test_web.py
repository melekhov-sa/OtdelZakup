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


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


def _make_xlsx(rows: list[dict]) -> io.BytesIO:
    """Create an in-memory .xlsx file from a list of dicts."""
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _upload_file(client, rows, filename="test.xlsx"):
    """Helper: upload an xlsx and return the response."""
    xlsx = _make_xlsx(rows)
    return client.post(
        "/upload",
        files={"file": (filename, xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )


def _extract_file_id(html: str) -> str:
    m = re.search(r'name="file_id"\s+value="([^"]+)"', html)
    assert m, "file_id hidden input not found"
    return m.group(1)


# ── 1. GET / ──────────────────────────────────────────────


def test_get_root_ok(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text
    assert "<form" in html
    assert 'name="file"' in html


# ── 2. POST /upload — happy path ─────────────────────────


def test_upload_xlsx_ok(client):
    data = [{"col": "Item A"}, {"col": "Item B"}]
    xlsx = _make_xlsx(data)

    resp = client.post(
        "/upload",
        files={"file": ("test.xlsx", xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert resp.status_code == 200
    html = resp.text
    assert "test.xlsx" in html
    assert "2" in html  # total rows
    assert "Item A" in html
    assert "Item B" in html


# ── 3. Non-.xlsx rejected ────────────────────────────────


def test_upload_not_xlsx_rejected(client):
    buf = io.BytesIO(b"just some text")
    resp = client.post(
        "/upload",
        files={"file": ("notes.txt", buf, "text/plain")},
    )
    assert resp.status_code == 400
    assert ".xlsx" in resp.text


# ── 4. Preview limited to 200 rows ──────────────────────


def test_upload_limits_preview_to_200_rows(client):
    rows = [{"id": i, "value": f"row_{i}"} for i in range(250)]
    xlsx = _make_xlsx(rows)

    resp = client.post(
        "/upload",
        files={"file": ("big.xlsx", xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert resp.status_code == 200
    html = resp.text
    assert "250" in html
    assert "row_199" in html
    assert "row_240" not in html


# ── 5. Upload page shows checkboxes ─────────────────────


def test_upload_shows_checkboxes(client):
    rows = [{"name": "Болт M12x80 8.8 оц ГОСТ 7798-70"}]
    resp = _upload_file(client, rows)
    assert resp.status_code == 200
    html = resp.text
    assert 'name="fields"' in html
    assert 'value="diameter"' in html
    assert 'value="strength"' in html
    assert "file_id" in html


# ── 6. Transform with selected fields ───────────────────


def test_transform_with_selected_fields(client):
    rows = [
        {"name": "Болт M12x80 8.8 оц ГОСТ 7798-70"},
        {"name": "Гайка М16 10.9 DIN 934"},
    ]
    resp = _upload_file(client, rows)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["diameter", "strength"]},
    )
    assert resp2.status_code == 200
    html = resp2.text
    assert "Исходная таблица" in html
    assert "Преобразованная таблица" in html
    assert "Диаметр" in html
    assert "Класс прочности" in html
    assert "M12" in html
    assert "8.8" in html
    assert "M16" in html
    assert "10.9" in html


# ── 7. Transform without fields ─────────────────────────


def test_transform_without_fields(client):
    rows = [{"name": "Болт M12x80 8.8"}]
    resp = _upload_file(client, rows)
    file_id = _extract_file_id(resp.text)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id},
    )
    assert resp2.status_code == 200
    html = resp2.text
    assert "Диаметр" not in html
    assert "Класс прочности" not in html
    assert "M12x80" in html

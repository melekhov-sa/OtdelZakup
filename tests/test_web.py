import io
import os

import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def _set_upload_dir(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    # Re-import so the module picks up the new env var
    import app.main as main_mod

    main_mod.UPLOAD_DIR = upload_dir


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
    # Total should reflect all 250
    assert "250" in html
    # Row 199 (0-indexed) should be visible
    assert "row_199" in html
    # Row 240 should NOT be in the preview
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

    # Extract file_id from the hidden input
    import re
    m = re.search(r'name="file_id"\s+value="([^"]+)"', resp.text)
    assert m, "file_id hidden input not found"
    file_id = m.group(1)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": ["diameter", "strength"]},
    )
    assert resp2.status_code == 200
    html = resp2.text
    # Result page should have both tables
    assert "Исходная таблица" in html
    assert "Преобразованная таблица" in html
    # Extracted columns should appear
    assert "Диаметр" in html
    assert "Класс прочности" in html
    # Extracted values
    assert "M12" in html
    assert "8.8" in html
    assert "M16" in html
    assert "10.9" in html


# ── 7. Transform without fields ─────────────────────────


def test_transform_without_fields(client):
    rows = [{"name": "Болт M12x80 8.8"}]
    resp = _upload_file(client, rows)

    import re
    m = re.search(r'name="file_id"\s+value="([^"]+)"', resp.text)
    file_id = m.group(1)

    resp2 = client.post(
        "/transform",
        data={"file_id": file_id},
    )
    assert resp2.status_code == 200
    html = resp2.text
    # No extra columns like "Диаметр" should appear
    assert "Диаметр" not in html
    assert "Класс прочности" not in html
    # Original data still present
    assert "M12x80" in html

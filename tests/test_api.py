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


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


def _make_xlsx(rows: list[dict]) -> io.BytesIO:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _api_upload(client, rows, filename="test.xlsx"):
    xlsx = _make_xlsx(rows)
    return client.post(
        "/api/v1/upload",
        files={"file": (filename, xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )


# ── POST /api/v1/upload ─────────────────────────────────────


def test_api_upload_ok(client):
    rows = [
        {"name": "Болт M12x80 8.8 оц ГОСТ 7798-70", "qty": 100},
        {"name": "Гайка М16 10.9 DIN 934", "qty": 200},
    ]
    resp = _api_upload(client, rows)
    assert resp.status_code == 200

    body = resp.json()
    assert "file_id" in body
    assert len(body["file_id"]) == 16
    assert body["filename"] == "test.xlsx"
    assert body["rows_total"] == 2
    assert "name" in body["columns"]
    assert "qty" in body["columns"]


def test_api_upload_not_xlsx(client):
    buf = io.BytesIO(b"hello")
    resp = client.post(
        "/api/v1/upload",
        files={"file": ("bad.txt", buf, "text/plain")},
    )
    assert resp.status_code == 400
    assert "error" in resp.json()


# ── GET /api/v1/preview/{file_id} ───────────────────────────


def test_api_preview_ok(client):
    rows = [{"col": f"val_{i}"} for i in range(10)]
    resp = _api_upload(client, rows)
    fid = resp.json()["file_id"]

    resp2 = client.get(f"/api/v1/preview/{fid}?limit=5")
    assert resp2.status_code == 200

    body = resp2.json()
    assert body["file_id"] == fid
    assert body["rows_total"] == 10
    assert body["limit"] == 5
    assert len(body["rows"]) == 5
    assert body["columns"] == ["col"]
    # Each row is a list, not a dict
    assert isinstance(body["rows"][0], list)


def test_api_preview_default_limit(client):
    rows = [{"x": i} for i in range(250)]
    resp = _api_upload(client, rows)
    fid = resp.json()["file_id"]

    resp2 = client.get(f"/api/v1/preview/{fid}")
    body = resp2.json()
    assert body["rows_total"] == 250
    assert body["limit"] == 200
    assert len(body["rows"]) == 200


# ── POST /api/v1/transform ──────────────────────────────────


def test_api_transform_ok(client):
    rows = [
        {"name": "Болт M12x80 8.8 оц ГОСТ 7798-70"},
        {"name": "Гайка М16 10.9 DIN 934"},
    ]
    resp = _api_upload(client, rows)
    fid = resp.json()["file_id"]

    resp2 = client.post(
        "/api/v1/transform",
        json={"file_id": fid, "fields": ["diameter", "strength"]},
    )
    assert resp2.status_code == 200

    body = resp2.json()
    assert body["file_id"] == fid
    assert body["rows_total"] == 2
    assert body["fields"] == ["diameter", "strength"]
    assert "Диаметр" in body["columns"]
    assert "Класс прочности" in body["columns"]
    # Check extracted values in rows
    flat = [cell for row in body["rows"] for cell in row]
    assert "M12" in flat
    assert "8.8" in flat
    assert "M16" in flat
    assert "10.9" in flat


def test_api_transform_no_fields(client):
    rows = [{"name": "Болт M12x80"}]
    resp = _api_upload(client, rows)
    fid = resp.json()["file_id"]

    resp2 = client.post(
        "/api/v1/transform",
        json={"file_id": fid, "fields": []},
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["fields"] == []
    assert "Диаметр" not in body["columns"]


def test_api_transform_with_limit(client):
    rows = [{"name": f"Болт M{i}x10"} for i in range(50)]
    resp = _api_upload(client, rows)
    fid = resp.json()["file_id"]

    resp2 = client.post(
        "/api/v1/transform",
        json={"file_id": fid, "fields": ["diameter"], "limit": 10},
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["rows_total"] == 50
    assert len(body["rows"]) == 10
    assert "Диаметр" in body["columns"]


# ── 404 for unknown file_id ─────────────────────────────────


def test_api_preview_not_found(client):
    resp = client.get("/api/v1/preview/0000000000000000")
    assert resp.status_code == 404
    assert resp.json()["error"] == "not found"


def test_api_transform_not_found(client):
    resp = client.post(
        "/api/v1/transform",
        json={"file_id": "0000000000000000", "fields": ["diameter"]},
    )
    assert resp.status_code == 404
    assert resp.json()["error"] == "not found"


# ── Determinism: same file → same file_id ────────────────────


def test_api_upload_deterministic_file_id(client):
    rows = [{"a": 1}]
    resp1 = _api_upload(client, rows, filename="one.xlsx")
    resp2 = _api_upload(client, rows, filename="one.xlsx")
    # Same bytes → same file_id
    assert resp1.json()["file_id"] == resp2.json()["file_id"]

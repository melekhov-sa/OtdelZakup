"""Tests for the row-analysis (trace/explain) feature (Step 9)."""

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

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()

    from app.seed import seed_default_rules
    seed_default_rules()


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
    assert m, "file_id not found"
    return m.group(1)


_ROWS = [
    {"Код": "001", "Номенклатура": "Болт M12x80 8.8 оц ГОСТ 7798-70", "Заказ": 10},
    {"Код": "002", "Номенклатура": "Канцтовары прочие", "Заказ": 5},
]


def _upload_and_transform(client, rows=_ROWS, fields=None):
    resp = _upload_file(client, rows)
    assert resp.status_code == 200
    file_id = _extract_file_id(resp.text)
    resp2 = client.post(
        "/transform",
        data={"file_id": file_id, "fields": fields or ["size", "strength", "item_type"]},
    )
    assert resp2.status_code == 200
    return file_id, resp2


# ── 1. Trace created after transform ─────────────────────────


def test_trace_created_for_rows(client):
    file_id, _ = _upload_and_transform(client)

    import app.trace as trace_mod

    traces = trace_mod.load_traces(file_id)
    assert traces is not None
    assert len(traces) == 2

    for i, trace in enumerate(traces, start=1):
        assert trace["row_number"] == i
        assert "raw_inputs" in trace
        assert "extracted_fields" in trace
        assert "readiness" in trace
        assert "validation" in trace
        assert "final" in trace
        assert trace["final"]["status"] in ("ok", "review", "manual")


# ── 2. Analysis endpoint returns 200 with expected JSON ──────


def test_analysis_endpoint_returns_200(client):
    file_id, _ = _upload_and_transform(client)

    resp = client.get(f"/files/{file_id}/rows/1/analysis")
    assert resp.status_code == 200

    data = resp.json()
    assert data["row_number"] == 1
    assert "raw_inputs" in data
    assert "extracted_fields" in data
    assert "enrichment" in data
    assert "readiness" in data
    assert "validation" in data
    assert "final" in data
    assert data["final"]["status_label"] in (
        "Не требует проверки", "Требуется просмотреть", "Требуется вручную разобрать"
    )


def test_analysis_endpoint_404_without_transform(client):
    resp = _upload_file(client, _ROWS)
    file_id = _extract_file_id(resp.text)

    # No transform → no traces
    resp2 = client.get(f"/files/{file_id}/rows/1/analysis")
    assert resp2.status_code == 404
    assert "error" in resp2.json()


# ── 3. Validation rule visible in analysis ───────────────────


def test_analysis_shows_validation_reason(client):
    # Create a validation rule: bolts must have coating
    client.post(
        "/rules/create",
        data={
            "name": "Болт — покрытие обязательно",
            "description": "Для болтов покрытие обязательно",
            "item_type": "болт",
            "require_fields": "coating",
            "priority": "1",
        },
    )

    # Upload bolt row with NO coating indicator
    rows = [{"Код": "001", "Номенклатура": "Болт M12x80 8.8 ГОСТ 7798-70", "Заказ": 10}]
    file_id, _ = _upload_and_transform(client, rows=rows, fields=["item_type", "size", "strength"])

    resp = client.get(f"/files/{file_id}/rows/1/analysis")
    assert resp.status_code == 200
    data = resp.json()

    applied = data["validation"]["applied_rules"]
    assert len(applied) >= 1
    names = [r["name"] for r in applied]
    assert "Болт — покрытие обязательно" in names

    # Reason should mention missing coating
    matched = next(r for r in applied if r["name"] == "Болт — покрытие обязательно")
    assert "покрытие" in matched["reason"].lower() or "coating" in matched["reason"].lower()

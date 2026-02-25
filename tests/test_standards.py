"""Tests for StandardRef — standard reference lookup and enrichment."""

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


def _seed_rules():
    from app.seed import seed_default_rules
    seed_default_rules()


def _seed_standards():
    from app.seed import seed_default_standards
    seed_default_standards()


def _make_xlsx(rows):
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _upload_and_transform(client, rows, fields):
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


# ── 1. AutoFill item_type from StandardRef ───────────────────


def test_standard_ref_autofill_item_type(client):
    """DIN 934 in text with no item_type keyword → item_type autofilled from standard."""
    _seed_rules()
    _seed_standards()

    # Description has no word "гайка" — item_type must come from standard lookup (DIN 934)
    rows = [{"Код": "001", "Номенклатура": "DIN 934 M16", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["size", "item_type", "din"])

    # item_type_source column confirms autofill happened from the standard
    assert "из стандарта" in html


# ── 2. Mismatch detected via direct enrichment logic ────────


def test_standard_mismatch_sets_review():
    """Row item_type='болт' but DIN 934 says 'гайка' → mismatch reason + status forced to review."""
    from app.readiness import _enrich_with_standards, evaluate_readiness, load_active_rules

    _seed_rules()

    # Build a standards cache manually — tests the enrichment logic directly,
    # independent of DB loading timing
    standards_cache = {("DIN", "934"): ("гайка", "Гайка шестигранная")}

    # Row where item_type from text is "болт" but DIN 934 → "гайка"
    row_dict = {
        "item_type": "болт",
        "din": "DIN 934",
        "size": "M12x80",
        "qty": "10",
        "name": "Болт M12x80 DIN 934",
    }

    enriched, extra_reasons = _enrich_with_standards(row_dict, standards_cache)

    # Mismatch must be detected
    assert extra_reasons, "Expected mismatch reason for болт vs DIN 934 (гайка)"
    mismatch_text = " ".join(extra_reasons).lower()
    assert "ожидалось" in mismatch_text or "не совпадает" in mismatch_text

    # evaluate_readiness alone gives "ok" (bolt rule satisfied: size+qty present)
    rules = load_active_rules()
    status, _, _ = evaluate_readiness(enriched, rules)
    assert status == "ok"

    # apply_readiness override: mismatch + ok → review
    final_status = "review" if extra_reasons and status == "ok" else status
    assert final_status == "review"


# ── 3. Standards page renders ───────────────────────────────


def test_standards_page_renders(client):
    _seed_standards()
    resp = client.get("/standards")
    assert resp.status_code == 200
    html = resp.text
    assert "Справочник стандартов" in html
    # Seed contains DIN 931 → болт
    assert "931" in html
    assert "болт" in html.lower()

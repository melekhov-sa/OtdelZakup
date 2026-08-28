"""Quantity written inside the name must be read from a file too.

A заявка saying "Болт 8*20 ... 15кг" gave qty=15 when pasted as text and
nothing when uploaded as a file: only the text path ran tail extraction,
the file path just looked for a "Кол-во" column and gave up.
"""

import base64
import io

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(tmp_path / "uploads"))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(tmp_path / "cache"))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = tmp_path / "uploads"
    cache_mod.CACHE_DIR = tmp_path / "cache"

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


@pytest.fixture()
def client():
    from app.main import app
    return TestClient(app)


LINES = [
    "Болт 8*20 8,8 ГОСТ 7798-70 б/п кг можно цинк   15кг",
    "Болт 22*90 8.8 ГОСТ 7798-70 б/п кг   8кг",
]


def _xlsx(rows):
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    return base64.b64encode(buf.getvalue()).decode()


def test_file_request_reads_quantity_from_the_name(client):
    resp = client.post("/api/v1/parse-request-base64", json={
        "file_base64": _xlsx([{"Наименование": line} for line in LINES]),
        "filename": "zayavka.xlsx",
    })

    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert [(r["qty"], r["unit"]) for r in rows] == [(15.0, "кг"), (8.0, "кг")]


def test_a_quantity_column_still_wins_over_the_name(client):
    """An explicit column is the author's intent; the tail is a fallback."""
    resp = client.post("/api/v1/parse-request-base64", json={
        "file_base64": _xlsx([
            {"Наименование": LINES[0], "Количество": 200, "Ед.изм": "шт"},
        ]),
        "filename": "zayavka.xlsx",
    })

    row = resp.json()["rows"][0]
    assert row["qty"] == 200
    assert row["unit"] == "шт"


def test_text_and_file_agree_on_the_same_lines(client):
    """Same заявка, same answer, whichever way it arrived."""
    as_text = client.post("/api/v1/parse-request", data={"text": "\n".join(LINES)}).json()
    as_file = client.post("/api/v1/parse-request-base64", json={
        "file_base64": _xlsx([{"Наименование": line} for line in LINES]),
        "filename": "zayavka.xlsx",
    }).json()

    assert [(r["qty"], r["unit"]) for r in as_text["rows"]] == \
           [(r["qty"], r["unit"]) for r in as_file["rows"]]

"""The matched item must name its characteristic, not just its uid.

In the customer's 1C a characteristic is a distinct nomenclature variant —
often a coating, but not always, so it cannot be inferred from our parsed
``coating`` field. It has to come from the catalog as 1C spelled it.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient


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


def _session():
    import app.database as db_mod
    return db_mod.SessionLocal()


def _add_item(session, name, uid, uid_char=None, char_name=None):
    from app.models import InternalItem
    item = InternalItem(
        name=name, item_type="болт", size="M12X80", size_norm="M12X80",
        standard_key="GOST-7798-70", uid_1c=uid, uid_1c_char=uid_char,
        char_name=char_name, is_active=True,
    )
    session.add(item)
    session.commit()
    return item


def test_match_carries_the_characteristic(client):
    session = _session()
    _add_item(session, "Болт М12х80 ГОСТ 7798-70 цинк", "uid-bolt",
              uid_char="char-cink", char_name="Цинк 5.6")
    session.close()

    resp = client.post("/api/v1/match-request", json={
        "rows": [{"row_no": 1, "name": "Болт М12х80 ГОСТ 7798-70 цинк", "qty": 10}],
    })

    assert resp.status_code == 200
    match = resp.json()["rows"][0]["match"]
    assert match is not None, "позиция не подобралась — тест не о том"
    assert match["uid_1c_char"] == "char-cink"
    assert match["char_name"] == "Цинк 5.6"
    assert match["candidates"][0]["uid_1c_char"] == "char-cink"
    assert match["candidates"][0]["char_name"] == "Цинк 5.6"


def test_item_without_characteristic_omits_the_fields(client):
    """No characteristic — no keys at all, rather than empty strings."""
    session = _session()
    _add_item(session, "Болт М12х80 ГОСТ 7798-70", "uid-bolt")
    session.close()

    resp = client.post("/api/v1/match-request", json={
        "rows": [{"row_no": 1, "name": "Болт М12х80 ГОСТ 7798-70", "qty": 10}],
    })

    match = resp.json()["rows"][0]["match"]
    assert match is not None
    assert "uid_1c_char" not in match
    assert "char_name" not in match
    assert "uid_1c_char" not in match["candidates"][0]


def test_catalog_sync_stores_the_characteristic_name():
    """Without this the name is only glued onto the match text and lost.

    ``char_name`` arrives in the 1C payload; the sync used to append it to the
    name for matching purposes and keep no copy, so it could not be handed
    back — and it cannot be recovered from the glued name afterwards.
    """
    from app.models import InternalItem
    from app.sync_1c import sync_from_1c

    session = _session()
    sync_from_1c({
        "folders": [],
        "items": [{
            "uid_1c": "uid-bolt",
            "uid_1c_char": "char-cink",
            "name": "Болт М12х80 ГОСТ 7798-70",
            "char_name": "Цинк 5.6",
            "is_active": True,
        }],
    }, session)

    item = session.query(InternalItem).filter_by(uid_1c="uid-bolt").one()
    stored = item.char_name
    session.close()

    assert stored == "Цинк 5.6"


def _add_analog(session, src, dst):
    from app.models import StandardEquivalent
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()


def test_request_can_ask_for_analog_matching(client):
    """Analogs are not wanted on every request, so the caller decides per call.

    Until now the only switch was a global setting in the web UI; 1C had no
    way to say "this заявка allows substitutes, that one does not".
    """
    session = _session()
    _add_item(session, "Болт М12х80 ГОСТ 7798-70 цинк", "uid-bolt")
    _add_analog(session, "GOST-7798-70", "DIN-931")
    session.close()

    body = {"rows": [{"row_no": 1, "name": "Болт М12х80 DIN 931 цинк", "qty": 10}]}

    with_analogs = client.post("/api/v1/match-request", json={**body, "use_analogs": True})
    without = client.post("/api/v1/match-request", json={**body, "use_analogs": False})

    assert with_analogs.status_code == 200
    assert without.status_code == 200

    assert with_analogs.json()["rows"][0]["match"] is not None, (
        "с включёнными аналогами ГОСТ 7798-70 ↔ DIN 931 должен подобраться"
    )
    assert without.json()["rows"][0]["match"] is None, (
        "с выключенными аналогами подбираться не должен"
    )

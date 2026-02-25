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


# ── 5. All rules disabled → ok ──────────────────────────────


def test_all_rules_disabled_defaults_to_ok(client):
    """Disabling all readiness rules → non-empty rows get status ok."""
    _seed()
    from app.database import get_db_session
    from app.models import ReadinessRule

    session = get_db_session()
    try:
        session.query(ReadinessRule).update({"is_active": False})
        session.commit()
    finally:
        session.close()

    rows = [{"Код": "001", "Номенклатура": "Анкер M12x145", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["size", "item_type"])
    assert 'data-status="ok"' in html


# ── 6. Empty name is always manual (unit test) ──────────────


def test_empty_name_is_manual_even_if_rules_disabled():
    """Empty name → manual + 'Пустое наименование' regardless of rules."""
    import pandas as pd
    from app.readiness import apply_readiness

    df_orig = pd.DataFrame([{"name": "", "qty": 10, "uom": "шт", "code": "001"}])
    df_trans = df_orig.copy()
    result = apply_readiness(df_orig, df_trans, rules=[], standards_cache={})
    assert result.at[0, "status"] == "manual"
    assert "Пустое наименование" in result.at[0, "reason"]


# ── 7. Missing qty/uom in default rule → manual ─────────────


def test_readiness_missing_qty_drives_manual(client):
    """Default rule requires name+qty+uom; row without qty column → manual."""
    _seed()
    # Include Код so auto-detect score >= 2; no Заказ → qty=None, uom=None
    rows = [{"Код": "001", "Номенклатура": "Анкер M12x145"}]
    html = _upload_and_transform(client, rows, ["size"])
    assert 'data-status="manual"' in html


# ── 8. Validation rule can downgrade ok → review ────────────


def test_validation_only_can_downgrade(client):
    """Readiness ok, validation force_status=review → final is review."""
    from app.database import get_db_session
    from app.models import ReadinessRule, ValidationRule

    session = get_db_session()
    try:
        # Readiness: only requires name → passes for any non-empty name
        rule = ReadinessRule(
            name="Только имя",
            description="",
            item_type=None,
            priority=0,
            is_active=True,
        )
        rule.require_fields_list = ["name"]
        session.add(rule)
        # Validation: bolt without coating → force review
        vr = ValidationRule(
            name="Болт — покрытие",
            description="",
            item_type="болт",
            priority=1,
            is_active=True,
            force_status="review",
        )
        vr.require_fields_list = ["coating"]
        vr.forbid_fields_list = []
        session.add(vr)
        session.commit()
    finally:
        session.close()

    # Bolt row with no coating
    rows = [{"Код": "001", "Номенклатура": "Болт M12x80 8.8 ГОСТ 7798-70", "Заказ": 10}]
    html = _upload_and_transform(client, rows, ["item_type", "size", "strength"])
    assert 'data-status="review"' in html

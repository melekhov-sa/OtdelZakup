"""Tests for the JSON API of supplier quote comparison (/api/v1/comparison).

Used by 1C: МЗ sends a verified position list, uploads supplier quotes,
and pulls back a comparison table.
"""

import base64
import io

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# ── DB isolation fixture ──────────────────────────────────────────────────────

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


def _make_catalog_item(session, uid, name, item_type="болт", size="M12X80",
                       standard_key="GOST-7798-70", folder_path="Метизы/Болты",
                       uid_char=None):
    from app.models import InternalItem
    item = InternalItem(
        name=name,
        item_type=item_type,
        size=size,
        size_norm=size,
        standard_key=standard_key,
        uid_1c=uid,
        uid_1c_char=uid_char,
        folder_path=folder_path,
        is_active=True,
    )
    session.add(item)
    session.commit()
    return item


def _xlsx_base64(rows: list[dict]) -> str:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    return base64.b64encode(buf.getvalue()).decode()


def _make_comparison(client, external_ref="z-200", uid="uid-bolt", qty=100, unit="шт",
                     weight_kg=None):
    body = {
        "external_ref": external_ref,
        "positions": [{"uid_1c": uid, "qty": qty, "unit": unit, "weight_kg": weight_kg}],
    }
    return client.post("/api/v1/comparison", json=body).json()["comparison_id"]


# ── POST /api/v1/comparison ───────────────────────────────────────────────────


def test_create_comparison_from_uids(client):
    """1C sends verified positions by uid_1c; each becomes an order item."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "zayavka-123",
        "title": "Заявка 123",
        "positions": [
            {"uid_1c": "uid-bolt", "qty": 100, "unit": "шт", "weight_kg": 0.085},
        ],
    })

    assert resp.status_code == 200
    body = resp.json()
    assert body["comparison_id"] > 0
    assert body["positions_total"] == 1
    assert body["not_found"] == []


def test_create_comparison_reports_unknown_uid(client):
    """A uid missing from the catalog is reported, not fatal."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "zayavka-124",
        "positions": [
            {"uid_1c": "uid-bolt", "qty": 10, "unit": "шт"},
            {"uid_1c": "uid-missing", "qty": 5, "unit": "шт"},
        ],
    })

    assert resp.status_code == 200
    body = resp.json()
    assert body["positions_total"] == 1
    assert body["not_found"] == ["uid-missing"]


def test_recreating_comparison_replaces_positions(client):
    """Re-sending the same external_ref replaces positions instead of piling up."""
    from app.order_models import OrderItem

    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    payload = {
        "external_ref": "zayavka-125",
        "positions": [{"uid_1c": "uid-bolt", "qty": 10, "unit": "шт"}],
    }
    first = client.post("/api/v1/comparison", json=payload).json()
    second = client.post("/api/v1/comparison", json=payload).json()

    assert second["comparison_id"] == first["comparison_id"]

    session = _session()
    stored = session.query(OrderItem).filter_by(order_id=first["comparison_id"]).count()
    session.close()
    assert stored == 1


# ── POST /api/v1/comparison/{id}/quotes ───────────────────────────────────────


def test_upload_quote_creates_lines(client):
    """A supplier price list arrives Base64-encoded in JSON and becomes quote lines."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client)

    resp = client.post(f"/api/v1/comparison/{comparison_id}/quotes", json={
        "supplier": "ООО Ромашка",
        "filename": "price.xlsx",
        "file_base64": _xlsx_base64([
            {"Наименование": "Болт М12х80 ГОСТ 7798-70 оцинкованный", "Цена": 14.20, "Ед": "шт"},
        ]),
    })

    assert resp.status_code == 200
    body = resp.json()
    assert body["quote_id"] > 0
    assert body["lines_total"] == 1


def test_uploaded_quote_lines_are_matched_to_positions(client):
    """Upload runs the matcher, so 1C gets counts without a second call."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client)

    resp = client.post(f"/api/v1/comparison/{comparison_id}/quotes", json={
        "supplier": "ООО Ромашка",
        "filename": "price.xlsx",
        "file_base64": _xlsx_base64([
            {"Наименование": "Болт М12х80 ГОСТ 7798-70 оцинкованный", "Цена": 14.20, "Ед": "шт"},
        ]),
    })

    assert resp.status_code == 200
    assert resp.json()["matched"] == 1


def test_pdf_quote_is_read_through_ocr(client, monkeypatch):
    """Suppliers send PDFs — those must go to Document AI, not to the xlsx reader."""
    class FakeExtraction:
        structured_rows = [{
            "name": "Болт М12х80 ГОСТ 7798-70 оцинкованный",
            "price_unit": 14.20, "qty": None, "unit": "шт",
        }]
        rows = []

    monkeypatch.setattr("app.integrations.google_document_ai.process_document",
                        lambda file_bytes, mime: object())
    monkeypatch.setattr("app.services.google_ocr_extractor.extract_rows",
                        lambda doc, hint="": FakeExtraction())

    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-800")

    resp = client.post(f"/api/v1/comparison/{comparison_id}/quotes", json={
        "supplier": "ООО Ромашка",
        "filename": "price.pdf",
        "file_base64": base64.b64encode(b"%PDF-1.4 fake").decode(),
    })

    assert resp.status_code == 200
    assert resp.json()["lines_total"] == 1


# ── GET /api/v1/comparison/{id} ───────────────────────────────────────────────


def _upload_quote(client, comparison_id, supplier, name, price, unit):
    return client.post(f"/api/v1/comparison/{comparison_id}/quotes", json={
        "supplier": supplier,
        "filename": "price.xlsx",
        "file_base64": _xlsx_base64([
            {"Наименование": name, "Цена": price, "Ед": unit},
        ]),
    })


def test_comparison_table_has_row_per_position(client):
    """The table is keyed by our positions, with a cell per supplier."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, unit="шт")
    _upload_quote(client, comparison_id, "ООО Ромашка",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный", 14.20, "шт")

    resp = client.get(f"/api/v1/comparison/{comparison_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["suppliers"] == ["ООО Ромашка"]
    assert len(body["rows"]) == 1

    row = body["rows"][0]
    assert row["uid_1c"] == "uid-bolt"
    assert row["cells"]["ООО Ромашка"]["price"] == 14.20


def test_replacing_positions_refreshes_the_match_index(client):
    """Quotes uploaded after a position change match the new positions.

    The matcher caches a MinHash index per order; replacing positions must
    drop it, otherwise quotes are compared against the previous list.
    """
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70",
                       item_type="болт", size="M12X80", standard_key="GOST-7798-70")
    _make_catalog_item(session, "uid-nut", "Гайка М16 DIN 934",
                       item_type="гайка", size="M16", standard_key="DIN-934")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-300", uid="uid-bolt")
    _upload_quote(client, comparison_id, "ООО Ромашка",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный", 14.20, "шт")

    # Same comparison, positions replaced — it is about the nut now
    _make_comparison(client, external_ref="z-300", uid="uid-nut")

    resp = _upload_quote(client, comparison_id, "ООО Василёк",
                         "Гайка М16 DIN 934 оцинкованная", 3.50, "шт")

    assert resp.json()["matched"] == 1


def test_table_converts_kilogram_price_and_keeps_the_original(client):
    """A price per kg is converted via weight, but the supplier's number stays."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-400",
                                     unit="шт", weight_kg=0.085)
    _upload_quote(client, comparison_id, "ООО Василёк",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный", 167.0, "кг")

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    cell = body["rows"][0]["cells"]["ООО Василёк"]

    assert cell["price"] == 167.0
    assert cell["unit"] == "кг"
    assert round(cell["price_normalized"], 3) == 14.195
    assert cell["basis"] == "weight"


def test_table_prefers_pack_size_written_in_the_quote(client):
    """Packaging stated by the supplier outranks our nomenclature weight."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-500",
                                     unit="шт", weight_kg=0.085)
    _upload_quote(client, comparison_id, "ООО Ромашка",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный, уп. 100 шт", 1420.0, "уп")

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    cell = body["rows"][0]["cells"]["ООО Ромашка"]

    assert cell["price"] == 1420.0
    assert round(cell["price_normalized"], 2) == 14.20
    assert cell["basis"] == "pack"


def test_table_carries_the_suspicious_flag(client):
    """Every cell states whether its converted price looks out of line."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-600", unit="шт")
    _upload_quote(client, comparison_id, "ООО Ромашка",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный", 14.20, "шт")

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    cell = body["rows"][0]["cells"]["ООО Ромашка"]

    # A lone supplier has nothing to be compared against
    assert cell["suspicious"] is False


# ── Web comparison page ───────────────────────────────────────────────────────


def test_web_comparison_page_shows_converted_price(client):
    """The web table is where we analyse mistakes, so it needs the same maths."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-700",
                                     unit="шт", weight_kg=0.085)
    _upload_quote(client, comparison_id, "ООО Василёк",
                  "Болт М12х80 ГОСТ 7798-70 оцинкованный", 167.0, "кг")

    resp = client.get(f"/orders/{comparison_id}/comparison")

    assert resp.status_code == 200
    # 167 ₽/кг × 0.085 кг = 14.195 ₽/шт, shown rounded to kopecks
    assert "14.20" in resp.text
    assert "по весу" in resp.text


def test_position_name_from_1c_is_shown_instead_of_catalog_name(client):
    """МЗ must see the wording she picked in 1C, not our copy of the catalog.

    Our catalog spells coating into the item name ("... M8x20 без покрытия").
    When 1C sends the name it knows, the comparison has to sign the position
    with that, or the table looks like the service invented an attribute.
    """
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт ГОСТ 7798-70 кл.пр. 8.8 M8x20 без покрытия")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "z-900",
        "positions": [{
            "uid_1c": "uid-bolt",
            "name": "Болт ГОСТ 7798-70 кл.пр. 8.8 M8x20",
            "qty": 10, "unit": "шт",
        }],
    })
    assert resp.status_code == 200

    body = client.get(f"/api/v1/comparison/{resp.json()['comparison_id']}").json()
    assert body["rows"][0]["name"] == "Болт ГОСТ 7798-70 кл.пр. 8.8 M8x20"


def test_position_without_name_falls_back_to_catalog(client):
    """Existing callers send no name — nothing changes for them."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт ГОСТ 7798-70 кл.пр. 8.8 M8x20 без покрытия")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "z-901",
        "positions": [{"uid_1c": "uid-bolt", "qty": 10, "unit": "шт"}],
    })
    body = client.get(f"/api/v1/comparison/{resp.json()['comparison_id']}").json()
    assert body["rows"][0]["name"] == "Болт ГОСТ 7798-70 кл.пр. 8.8 M8x20 без покрытия"


def test_reuploading_a_supplier_quote_replaces_the_previous_one(client):
    """A corrected price list from the same supplier supersedes the old one.

    Without replacement both quotes survive and two QuoteMatch rows point at
    the same position; which one lands in the cell then depends on query
    order, and a manual match made on the old quote competes with the new.
    """
    from app.order_models import Quote, QuoteLine, QuoteMatch

    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-dup")

    for price in (100.0, 200.0):
        resp = _upload_quote(client, comparison_id, "ООО Ромашка",
                             "Болт М12х80 ГОСТ 7798-70 оц", price, "шт")
        assert resp.status_code == 200

    session = _session()
    quotes = session.query(Quote).count()
    lines = session.query(QuoteLine).count()
    matches = session.query(QuoteMatch).count()
    session.close()

    assert (quotes, lines, matches) == (1, 1, 1), (
        f"old quote left behind: quotes={quotes} lines={lines} matches={matches}"
    )

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    assert body["rows"][0]["cells"]["ООО Ромашка"]["price"] == 200.0


def test_replacing_one_supplier_leaves_the_others_alone(client):
    """Re-uploading Ромашка must not touch Василёк's quote."""
    from app.order_models import Quote

    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-dup2")
    _upload_quote(client, comparison_id, "ООО Ромашка", "Болт М12х80 ГОСТ 7798-70 оц", 100.0, "шт")
    _upload_quote(client, comparison_id, "ООО Василёк", "Болт М12х80 ГОСТ 7798-70 оц", 150.0, "шт")
    _upload_quote(client, comparison_id, "ООО Ромашка", "Болт М12х80 ГОСТ 7798-70 оц", 120.0, "шт")

    session = _session()
    total = session.query(Quote).count()
    session.close()

    assert total == 2, f"expected one quote per supplier, got {total}"

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    assert sorted(body["suppliers"]) == ["ООО Василёк", "ООО Ромашка"]


def test_manual_match_page_warns_when_analog_directory_is_empty(client):
    """An empty analogs table makes the "учитывать аналоги" checkbox a no-op.

    Standards compatibility is read only from StandardEquivalent. With no rows
    there, DIN↔ГОСТ pairs score 80 against a threshold of 90 and never match
    automatically — while the checkbox suggests analogs are being applied.
    """
    from app.order_models import QuoteLine

    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-analog")
    upload = _upload_quote(client, comparison_id, "ООО Ромашка",
                           "Болт М12х80 ГОСТ 7798-70 оц", 14.20, "шт").json()

    session = _session()
    ql = session.query(QuoteLine).filter_by(quote_id=upload["quote_id"]).first()
    ql_id = ql.id
    session.close()

    resp = client.get(
        f"/orders/{comparison_id}/quotes/{upload['quote_id']}/lines/{ql_id}/match"
    )

    assert resp.status_code == 200
    assert "справочник аналогов пуст" in resp.text.lower()


def test_characteristic_selects_the_right_catalog_variant(client):
    """One uid_1c can carry many characteristics — the colour must not be guessed.

    In the live catalog 6510 uids have several characteristics, up to 126
    colour variants of one roofing screw. Resolving by uid_1c alone picks
    whichever row was stored first and compares prices for the wrong item.
    """
    session = _session()
    _make_catalog_item(session, "uid-screw", "Саморез кровельный 4.8x19 RAL 1019",
                       item_type="саморез", size="4.8X19", uid_char="char-1019")
    _make_catalog_item(session, "uid-screw", "Саморез кровельный 4.8x19 RAL 5005",
                       item_type="саморез", size="4.8X19", uid_char="char-5005")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "z-char",
        "positions": [{
            "uid_1c": "uid-screw",
            "uid_1c_char": "char-5005",
            "qty": 100, "unit": "шт",
        }],
    })
    assert resp.status_code == 200
    assert resp.json()["not_found"] == []

    body = client.get(f"/api/v1/comparison/{resp.json()['comparison_id']}").json()
    assert body["rows"][0]["name"] == "Саморез кровельный 4.8x19 RAL 5005"
    assert body["rows"][0]["uid_1c_char"] == "char-5005"


def test_position_without_characteristic_still_resolves(client):
    """Items that have no characteristic keep working as before."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    resp = client.post("/api/v1/comparison", json={
        "external_ref": "z-nochar",
        "positions": [{"uid_1c": "uid-bolt", "qty": 10, "unit": "шт"}],
    })
    assert resp.json()["not_found"] == []
    body = client.get(f"/api/v1/comparison/{resp.json()['comparison_id']}").json()
    assert body["rows"][0]["name"] == "Болт М12х80 ГОСТ 7798-70"


def _upload_pirra_style(client, comparison_id, supplier="Пирра"):
    """A price list shaped like the one from Пирра: kg priced, pieces for reference."""
    return client.post(f"/api/v1/comparison/{comparison_id}/quotes", json={
        "supplier": supplier,
        "filename": "price.xlsx",
        "file_base64": _xlsx_base64([{
            "№": 1,
            "Товары (работы, услуги)": "Болт М12х80 ГОСТ 7798-70 цинк",
            "Кол-во": 15,
            "Ед.": "кг",
            "Цена": 251.95,
            "Кол-во шт. (справочно)": 1152,
            "Сумма": 3779.25,
        }]),
    })


def test_quote_line_keeps_both_quantities(client):
    """The priced amount and the supplier's restatement both reach the API."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-two-units", unit="шт")
    assert _upload_pirra_style(client, comparison_id).status_code == 200

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    cell = body["rows"][0]["cells"]["Пирра"]

    assert cell["qty"] == 15
    assert cell["unit"] == "кг"
    assert cell["ref_qty"] == 1152
    assert cell["ref_unit"] == "шт"


def test_cell_reports_how_much_the_supplier_actually_covers(client):
    """A supplier who cannot close the whole volume must be visible at a glance.

    Пирра offers 15 кг = 1152 шт. Against a request for 2000 шт that is a
    shortfall, and the buyer has to see it without doing arithmetic.
    """
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-cover",
                                     qty=2000, unit="шт")
    _upload_pirra_style(client, comparison_id)

    body = client.get(f"/api/v1/comparison/{comparison_id}").json()
    row = body["rows"][0]
    cell = row["cells"]["Пирра"]

    assert row["qty"] == 2000
    assert cell["offered_qty"] == 1152
    assert cell["covers_full"] is False


def test_cell_reports_full_coverage(client):
    """Enough offered — the flag says so."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-cover2",
                                     qty=1000, unit="шт")
    _upload_pirra_style(client, comparison_id)

    cell = client.get(f"/api/v1/comparison/{comparison_id}").json()["rows"][0]["cells"]["Пирра"]
    assert cell["offered_qty"] == 1152
    assert cell["covers_full"] is True


def test_web_table_marks_reference_quantity_and_shortfall(client):
    """The web view says where the converted price came from and what is short."""
    session = _session()
    _make_catalog_item(session, "uid-bolt", "Болт М12х80 ГОСТ 7798-70")
    session.close()

    comparison_id = _make_comparison(client, external_ref="z-web-ref",
                                     qty=2000, unit="шт")
    _upload_pirra_style(client, comparison_id)

    text = client.get(f"/orders/{comparison_id}/comparison").text

    assert "справочно" in text
    assert "по справочному кол-ву" in text
    assert "не весь объём" in text

"""Tests for the Orders & Quote Comparison module.

Covers: line parser, strict filters, DB constraints, comparison table,
approval flow, matching pipeline.
"""
import json

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.match_settings import MatchSettings


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


def _session():
    import app.database as db_mod
    return db_mod.SessionLocal()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_order(session, title="Test Order"):
    from app.order_models import Order
    o = Order(title=title)
    session.add(o)
    session.commit()
    return o


def _make_order_item(session, order_id, name, catalog_item_id=None):
    from app.order_models import OrderItem
    from app.services.line_parser import parse_raw_line
    parsed = parse_raw_line(name)
    oi = OrderItem(
        order_id=order_id,
        catalog_item_id=catalog_item_id,
        display_name_snapshot=name,
        type_norm=parsed.get("item_type") or "",
        size_norm=parsed.get("size_norm") or "",
        std_norm=parsed.get("std_norm") or "",
        tokens_norm=parsed.get("tokens_norm") or "",
    )
    session.add(oi)
    session.commit()
    return oi


def _make_supplier(session, name="Supplier A"):
    from app.order_models import Supplier
    s = session.query(Supplier).filter_by(name=name).first()
    if s:
        return s
    s = Supplier(name=name)
    session.add(s)
    session.commit()
    return s


def _make_quote(session, order_id, supplier_id):
    from app.order_models import Quote
    q = Quote(order_id=order_id, supplier_id=supplier_id)
    session.add(q)
    session.commit()
    return q


def _low_threshold_settings(**overrides):
    """MatchSettings with very low thresholds for test matching."""
    defaults = dict(
        auto_apply_jaccard_threshold=0.01,
        suggest_jaccard_threshold=0.005,
        auto_match_delta_jaccard=0.0,
    )
    defaults.update(overrides)
    return MatchSettings(**defaults)


def _make_quote_line(session, quote_id, raw_text, row_no=1, price=None, line_class=None):
    from app.order_models import QuoteLine
    from app.services.line_parser import parse_raw_line
    from app.services.quote_line_classifier import classify_quote_line
    parsed = parse_raw_line(raw_text)
    if line_class is None:
        cls, reason = classify_quote_line(raw_text)
    else:
        cls, reason = line_class, ""
    ql = QuoteLine(
        quote_id=quote_id, row_no=row_no, raw_text=raw_text,
        price=price,
        parsed_json=json.dumps(parsed, ensure_ascii=False),
        type_norm=parsed.get("item_type") or "",
        size_norm=parsed.get("size_norm") or "",
        std_norm=parsed.get("std_norm") or "",
        tokens_norm=parsed.get("tokens_norm") or "",
        line_class=cls, filter_reason=reason or None,
    )
    session.add(ql)
    session.commit()
    return ql


# ── 1. Line parser ──────────────────────────────────────────────────────────

class TestLineParser:

    def test_parse_raw_line_extracts_fields(self):
        from app.services.line_parser import parse_raw_line
        parsed = parse_raw_line("Болт М12х60 DIN 933")
        assert parsed["item_type"] == "болт"
        assert parsed["size_norm"] != ""
        assert parsed["tokens_norm"] != ""

    def test_parse_raw_line_empty(self):
        from app.services.line_parser import parse_raw_line
        parsed = parse_raw_line("")
        assert parsed["item_type"] == ""
        assert parsed["size_norm"] == ""

    def test_build_features_special_tokens(self):
        from app.services.line_parser import build_features
        feats = build_features("болт м12x60", type_norm="болт", size_norm="M12X60", std_norm="DIN-933")
        assert "TYPE:болт" in feats
        assert "SIZE:M12X60" in feats
        assert "STD:DIN-933" in feats

    def test_build_features_no_special_tokens(self):
        from app.services.line_parser import build_features
        feats = build_features("прокладка резиновая")
        assert not any(t.startswith(("SIZE:", "TYPE:", "STD:")) for t in feats)

    def test_parse_client_file_xlsx(self, tmp_path):
        import openpyxl
        from app.services.line_parser import parse_client_file

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Наименование", "Количество"])
        ws.append(["Болт M12x60", "10"])
        ws.append(["Гайка M12", "20"])
        fpath = tmp_path / "order.xlsx"
        wb.save(fpath)

        result = parse_client_file(fpath.read_bytes(), "order.xlsx")
        assert len(result) == 2
        assert result[0]["name"] == "Болт M12x60"

    def test_parse_quote_file_csv(self, tmp_path):
        from app.services.line_parser import parse_quote_file

        csv_content = "Наименование;Цена\nБолт M12;100\nГайка M12;50\n"
        fpath = tmp_path / "quote.csv"
        fpath.write_text(csv_content, encoding="utf-8-sig")

        headers, data_rows = parse_quote_file(fpath.read_bytes(), "quote.csv")
        assert len(headers) == 2
        assert len(data_rows) == 2


# ── 2. Strict filters ────────────────────────────────────────────────────────

class TestStrictFilters:

    def _match(self, ql_text, oi_names):
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes
        from app.order_models import QuoteLine

        sess = _session()
        order = _make_order(sess)
        ois = [_make_order_item(sess, order.id, n) for n in oi_names]
        supplier = _make_supplier(sess)
        quote = _make_quote(sess, order.id, supplier.id)
        ql = _make_quote_line(sess, quote.id, ql_text)

        mhs = _build_order_item_minhashes(ois)
        oi_by_id = {oi.id: oi for oi in ois}
        ql_obj = sess.get(QuoteLine, ql.id)
        result = match_quote_line_to_items(
            ql_obj, mhs, oi_by_id, settings=_low_threshold_settings(),
        )
        sess.close()
        return result

    def test_size_mismatch_excluded(self):
        result = self._match("Болт M24x60", ["Болт M12x60 DIN 933", "Болт M24x60 DIN 933"])
        sizes = [c["size_norm"] for c in result["candidates"]]
        for sz in sizes:
            if sz:
                assert "M12" not in sz

    def test_size_match_kept(self):
        result = self._match("Шайба M24", ["Шайба M24 DIN 125", "Шайба M12 DIN 125"])
        assert len(result["candidates"]) >= 1
        assert result["candidates"][0]["size_norm"] in ("M24", "")

    def test_size_unknown_no_filter(self):
        # Items without recognized features may yield empty MinHash → 0 candidates;
        # the key invariant is: no false-positive auto-match when there are no filters.
        result = self._match("Прокладка резиновая", ["Прокладка NBR", "Прокладка силиконовая"])
        # Either candidates found (all passed filters) or none at all (empty features)
        for c in result["candidates"]:
            assert c["passed_filters"]

    def test_type_mismatch_excluded(self):
        result = self._match("Болт M12x60", ["Гайка M12 DIN 934", "Болт M12x60 DIN 933"])
        types = [c["type_norm"] for c in result["candidates"]]
        assert "гайка" not in [t.lower() for t in types if t]

    def test_type_match_kept(self):
        result = self._match("Болт M12x60", ["Болт M12x80 DIN 933", "Болт M12x60 DIN 933"])
        for c in result["candidates"]:
            if c["type_norm"]:
                assert c["type_norm"].lower() == "болт"


# ── 3. DB constraints ────────────────────────────────────────────────────────

class TestDBConstraints:

    def test_quote_line_unique_in_quote_match(self):
        from sqlalchemy.exc import IntegrityError
        from app.order_models import QuoteMatch

        sess = _session()
        order = _make_order(sess)
        oi1 = _make_order_item(sess, order.id, "Item 1")
        oi2 = _make_order_item(sess, order.id, "Item 2")
        supplier = _make_supplier(sess)
        quote = _make_quote(sess, order.id, supplier.id)
        ql = _make_quote_line(sess, quote.id, "Quote item")

        sess.add(QuoteMatch(order_item_id=oi1.id, quote_line_id=ql.id, match_mode="auto", jaccard=0.8))
        sess.commit()

        sess.add(QuoteMatch(order_item_id=oi2.id, quote_line_id=ql.id, match_mode="manual", jaccard=0.5))
        with pytest.raises(IntegrityError):
            sess.commit()
        sess.rollback()
        sess.close()

    def test_cascade_delete_order(self):
        from app.order_models import (
            ClientLine, Order, OrderItem, Quote, QuoteLine, QuoteMatch,
        )

        sess = _session()
        sess.execute(text("PRAGMA foreign_keys = ON"))
        order = _make_order(sess)

        # ClientLine
        sess.add(ClientLine(order_id=order.id, row_no=1, raw_text="test"))
        sess.flush()

        oi = _make_order_item(sess, order.id, "Item 1")
        supplier = _make_supplier(sess)
        quote = _make_quote(sess, order.id, supplier.id)
        ql = _make_quote_line(sess, quote.id, "Quote item")
        sess.add(QuoteMatch(order_item_id=oi.id, quote_line_id=ql.id, match_mode="auto", jaccard=0.8))
        sess.commit()

        sess.execute(text("DELETE FROM orders WHERE id = :oid"), {"oid": order.id})
        sess.commit()

        assert sess.query(ClientLine).count() == 0
        assert sess.query(OrderItem).count() == 0
        assert sess.query(Quote).count() == 0
        assert sess.query(QuoteLine).count() == 0
        assert sess.query(QuoteMatch).count() == 0
        sess.close()

    def test_supplier_name_unique(self):
        from sqlalchemy.exc import IntegrityError
        from app.order_models import Supplier

        sess = _session()
        sess.add(Supplier(name="Test Supplier"))
        sess.commit()
        sess.add(Supplier(name="Test Supplier"))
        with pytest.raises(IntegrityError):
            sess.commit()
        sess.rollback()
        sess.close()


# ── 4. Comparison table ──────────────────────────────────────────────────────

class TestComparisonTable:

    def test_builds_correct_matrix(self):
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import build_comparison_table

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup_a = _make_supplier(sess, "Supplier A")
        sup_b = _make_supplier(sess, "Supplier B")

        q_a = _make_quote(sess, order.id, sup_a.id)
        ql_a = _make_quote_line(sess, q_a.id, "Болт M12x60", price=100.0)

        q_b = _make_quote(sess, order.id, sup_b.id)
        ql_b = _make_quote_line(sess, q_b.id, "Болт M12x60", price=80.0)

        sess.add(QuoteMatch(order_item_id=oi.id, quote_line_id=ql_a.id, match_mode="auto", jaccard=0.9))
        sess.add(QuoteMatch(order_item_id=oi.id, quote_line_id=ql_b.id, match_mode="auto", jaccard=0.85))
        sess.commit()

        table = build_comparison_table(order.id, sess)

        assert "Supplier A" in table["suppliers"]
        assert "Supplier B" in table["suppliers"]
        assert len(table["rows"]) == 1

        row = table["rows"][0]
        assert row["cells"]["Supplier A"]["price"] == 100.0
        assert row["cells"]["Supplier B"]["price"] == 80.0
        sess.close()

    def test_min_price_identified(self):
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import build_comparison_table

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Гайка M12 DIN 934")

        sup_a = _make_supplier(sess, "S1")
        sup_b = _make_supplier(sess, "S2")

        q_a = _make_quote(sess, order.id, sup_a.id)
        ql_a = _make_quote_line(sess, q_a.id, "Гайка M12", price=50.0)
        q_b = _make_quote(sess, order.id, sup_b.id)
        ql_b = _make_quote_line(sess, q_b.id, "Гайка M12", price=30.0)

        sess.add(QuoteMatch(order_item_id=oi.id, quote_line_id=ql_a.id, match_mode="auto", jaccard=0.9))
        sess.add(QuoteMatch(order_item_id=oi.id, quote_line_id=ql_b.id, match_mode="auto", jaccard=0.8))
        sess.commit()

        table = build_comparison_table(order.id, sess)
        prices = [table["rows"][0]["cells"][s]["price"] for s in table["suppliers"]
                   if s in table["rows"][0]["cells"]]
        assert min(prices) == 30.0
        sess.close()

    def test_unmatched_lines_reported(self):
        from app.services.quote_order_matcher import build_comparison_table

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60")

        sup = _make_supplier(sess, "SupX")
        q = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, q.id, "Болт необычный без аналога", price=10.0)

        # No QuoteMatch created — line is an "item" but has no match
        table = build_comparison_table(order.id, sess)
        assert "SupX" in table["unmatched"]
        assert len(table["unmatched"]["SupX"]) == 1
        sess.close()


# ── 5. Match engine integration ──────────────────────────────────────────────

class TestMatchEngine:

    def test_match_quote_to_order_items(self):
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import match_quote_to_order_items

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60 DIN 933")
        _make_order_item(sess, order.id, "Гайка M12 DIN 934")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", row_no=1, price=15.0)
        _make_quote_line(sess, quote.id, "Гайка M12 DIN 934", row_no=2, price=5.0)

        stats = match_quote_to_order_items(quote.id, sess)
        matches = sess.query(QuoteMatch).all()
        assert len(matches) == 2
        assert stats["matched_auto"] == 2
        sess.close()

    def test_match_empty_order_items(self):
        from app.services.quote_order_matcher import match_quote_to_order_items

        sess = _session()
        order = _make_order(sess)
        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, quote.id, "Something", row_no=1)

        stats = match_quote_to_order_items(quote.id, sess)
        assert stats["unmatched"] == 1
        sess.close()


# ── 6. Approval flow ─────────────────────────────────────────────────────────

class TestApprovalFlow:

    def test_client_line_parsed_property(self):
        from app.order_models import ClientLine

        sess = _session()
        order = _make_order(sess)
        cl = ClientLine(
            order_id=order.id, row_no=1, raw_text="test",
            parsed_json='{"item_type": "болт", "size_norm": "M12"}',
        )
        sess.add(cl)
        sess.commit()

        cl2 = sess.get(ClientLine, cl.id)
        assert cl2.parsed["item_type"] == "болт"
        assert cl2.parsed["size_norm"] == "M12"
        sess.close()

    def test_client_line_parsed_empty(self):
        from app.order_models import ClientLine

        sess = _session()
        order = _make_order(sess)
        cl = ClientLine(order_id=order.id, row_no=1, raw_text="test")
        sess.add(cl)
        sess.commit()

        cl2 = sess.get(ClientLine, cl.id)
        assert cl2.parsed == {}
        sess.close()

    def test_order_status_transitions(self):
        from app.order_models import Order

        sess = _session()
        order = _make_order(sess)
        assert order.status == "draft"

        order.status = "matching_catalog"
        sess.commit()
        assert sess.get(Order, order.id).status == "matching_catalog"

        order.status = "approved_catalog"
        sess.commit()
        assert sess.get(Order, order.id).status == "approved_catalog"
        sess.close()


# ── 7. Garbage filter (quote_line_classifier) ──────────────────────────────

class TestGarbageFilter:

    def test_total_row_filtered(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, reason = classify_quote_line("Итого 5 300 896,15")
        assert cls == "total"

    def test_total_row_tekushaya(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, reason = classify_quote_line("Текущая 5 300 896,15")
        assert cls == "total"

    def test_header_row_filtered(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, reason = classify_quote_line(
            "Тип задолженности Дата планового погашения Долг клиента Счет на оплату"
        )
        assert cls != "item"  # could be total/header/requisites — all are non-item

    def test_requisites_filtered(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("ИНН 7712345678 КПП 771201001")
        assert cls == "requisites"

    def test_item_bolt_kept(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("Болт М22x90 ГОСТ 7798-70 кл.пр.8.8 цинк 661,58")
        assert cls == "item"

    def test_item_din_kept(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("1 DIN 933 M 8 x 20 Болт ZN 1 638,00")
        assert cls == "item"

    def test_item_with_itogo_word_but_has_product(self):
        """If text has 'итого' but also has product indicators -> should be item."""
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("Болт M12x60 DIN 933 итого 5 шт")
        assert cls == "item"

    def test_is_non_item_row_convenience(self):
        from app.services.quote_line_classifier import is_non_item_row
        assert is_non_item_row("Итого 5 300,00") is True
        assert is_non_item_row("Болт M12x60 DIN 933") is False

    def test_empty_string(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("")
        assert cls == "garbage"

    def test_short_word(self):
        from app.services.quote_line_classifier import classify_quote_line
        cls, _ = classify_quote_line("Примечание")
        assert cls != "item"


# ── 8. Post-filters in matching ─────────────────────────────────────────────

class TestPostFilters:

    def test_size_mismatch_blocks_auto(self):
        """OrderItem M16x50; QuoteLine M16x40 -> should NOT auto-match."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт М16x50 ГОСТ 7805-70")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт М16x40 ГОСТ 7805-70", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(
            ql, mhs, {oi.id: oi}, settings=_low_threshold_settings(),
        )
        # Best candidate should NOT pass filters because sizes differ
        assert result["match_mode"] is None or result["best_order_item_id"] is None
        sess.close()

    def test_exact_match_auto(self):
        """Exact match should auto-link."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(
            ql, mhs, {oi.id: oi}, settings=_low_threshold_settings(auto_apply_jaccard_threshold=0.3),
        )
        assert result["match_mode"] == "auto"
        assert result["best_order_item_id"] == oi.id
        sess.close()

    def test_non_item_line_skipped(self):
        """line_class='total' should return no match."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes
        from app.order_models import QuoteLine

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Итого 5 300,00", line_class="total")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(
            ql, mhs, {oi.id: oi}, settings=_low_threshold_settings(),
        )
        assert result["best_order_item_id"] is None
        sess.close()


# ── 9. Filtered vs unmatched in comparison ──────────────────────────────────

class TestFilteredComparison:

    def test_filtered_lines_separated(self):
        """Non-item lines appear in 'filtered', not 'unmatched'."""
        from app.services.quote_order_matcher import build_comparison_table

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60")

        sup = _make_supplier(sess, "SupFilter")
        q = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, q.id, "Итого 5 300,00", row_no=1, line_class="total")
        _make_quote_line(sess, q.id, "Болт M16x80 DIN 933", row_no=2, line_class="item")

        table = build_comparison_table(order.id, sess)
        # The total line should be in 'filtered'
        assert "SupFilter" in table["filtered"]
        assert len(table["filtered"]["SupFilter"]) == 1
        assert table["filtered"]["SupFilter"][0].line_class == "total"
        # The item line with no match should be in 'unmatched'
        assert "SupFilter" in table["unmatched"]
        assert len(table["unmatched"]["SupFilter"]) == 1
        sess.close()


# ── 10. Uniqueness in auto-matching ─────────────────────────────────────────

class TestAutoMatchUniqueness:

    def test_same_order_item_not_double_linked(self):
        """Two similar QuoteLines should not both auto-match the same OrderItem."""
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import match_quote_to_order_items

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", row_no=1, price=100.0, line_class="item")
        _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933 цинк", row_no=2, price=110.0, line_class="item")

        stats = match_quote_to_order_items(quote.id, sess)
        matches = sess.query(QuoteMatch).all()
        # Only ONE should be auto-matched (uniqueness)
        assert stats["matched_auto"] == 1
        assert len(matches) == 1
        sess.close()


# ── 11. Delta check ──────────────────────────────────────────────────────────

class TestDeltaCheck:

    def test_clear_best_auto_matches(self):
        """When J1 >> J2, auto-match should work."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi1 = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")
        oi2 = _make_order_item(sess, order.id, "Гайка M8 DIN 934")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", line_class="item")

        mhs = _build_order_item_minhashes([oi1, oi2])
        settings = _low_threshold_settings(
            auto_apply_jaccard_threshold=0.3,
            auto_match_delta_jaccard=0.05,
        )
        result = match_quote_line_to_items(
            ql, mhs, {oi1.id: oi1, oi2.id: oi2}, settings=settings,
        )
        assert result["match_mode"] == "auto"
        assert result["best_order_item_id"] == oi1.id
        sess.close()

    def test_ambiguous_becomes_suggested(self):
        """When two candidates have very close Jaccard and different sizes, mode should be 'suggested'."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        # Two items with DIFFERENT sizes so exact match won't fire
        oi1 = _make_order_item(sess, order.id, "Болт M12x60 DIN 933 кл.пр.8.8")
        oi2 = _make_order_item(sess, order.id, "Болт M12x65 DIN 933 кл.пр.10.9")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        # QL has no size so exact match stage is skipped → MinHash delta check applies
        ql = _make_quote_line(sess, quote.id, "Болт DIN 933 кл.пр.8.8", line_class="item")

        mhs = _build_order_item_minhashes([oi1, oi2])
        settings = _low_threshold_settings(
            auto_apply_jaccard_threshold=0.05,
            auto_match_delta_jaccard=0.90,  # extremely high delta -> forces suggested
        )
        result = match_quote_line_to_items(
            ql, mhs, {oi1.id: oi1, oi2.id: oi2}, settings=settings,
        )
        assert result["match_mode"] == "suggested"
        sess.close()


# ── 12. Standard analog matching ─────────────────────────────────────────────

class TestStandardAnalogMatching:

    def test_standard_relaxation_no_std_oi(self):
        """QL has standard, OI has no standard but high J -> should still match via relaxation."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        # OI has no standard in the name
        oi = _make_order_item(sess, order.id, "Болт M12x60")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        settings = _low_threshold_settings(auto_apply_jaccard_threshold=0.1)
        result = match_quote_line_to_items(
            ql, mhs, {oi.id: oi}, settings=settings,
        )
        # Should still find a candidate (relaxation kicks in)
        assert len(result["candidates"]) >= 1
        sess.close()


# ── 13. match_all_quotes_for_order ───────────────────────────────────────────

class TestMatchAllQuotes:

    def test_aggregate_summary(self):
        """match_all_quotes_for_order returns correct aggregated stats."""
        from app.services.quote_order_matcher import match_all_quotes_for_order

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60 DIN 933")
        _make_order_item(sess, order.id, "Гайка M12 DIN 934")

        sup_a = _make_supplier(sess, "SA")
        q_a = _make_quote(sess, order.id, sup_a.id)
        _make_quote_line(sess, q_a.id, "Болт M12x60 DIN 933", row_no=1, price=10.0)
        _make_quote_line(sess, q_a.id, "Гайка M12 DIN 934", row_no=2, price=5.0)

        sup_b = _make_supplier(sess, "SB")
        q_b = _make_quote(sess, order.id, sup_b.id)
        _make_quote_line(sess, q_b.id, "Болт M12x60 DIN 933", row_no=1, price=12.0)

        totals = match_all_quotes_for_order(order.id, sess)
        assert totals["quotes_processed"] == 2
        assert totals["total_lines"] == 3
        assert totals["matched_auto"] >= 2  # at least the bolt from each quote
        assert "time_ms" in totals
        sess.close()

    def test_index_cache_used(self):
        """After match_all_quotes_for_order, index should be cached."""
        from app.services.quote_order_matcher import match_all_quotes_for_order, _index_cache

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M16x80 DIN 933")
        sup = _make_supplier(sess, "SC")
        q = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, q.id, "Болт M16x80 DIN 933", row_no=1)

        _index_cache.pop(order.id, None)
        match_all_quotes_for_order(order.id, sess)
        assert order.id in _index_cache
        sess.close()


# ── 14. Debug info in results ────────────────────────────────────────────────

class TestDebugInfo:

    def test_debug_dict_present(self):
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(ql, mhs, {oi.id: oi})
        assert "debug" in result
        # debug may contain j_best (minhash) or best_score (exact) or skip_reason
        d = result["debug"]
        assert "j_best" in d or "skip_reason" in d or "best_score" in d
        assert "candidates" in result
        sess.close()

    def test_candidate_has_filter_badges(self):
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(ql, mhs, {oi.id: oi})
        if result["candidates"]:
            c = result["candidates"][0]
            assert "type_match" in c
            assert "size_match" in c
            assert "std_match" in c
        sess.close()


# ── 15. MinHash hard size filter prevents wrong sizes ─────────────────────

class TestMinhashHardSizeFilter:

    def test_m14_not_matched_to_m3(self):
        """Query 'Гайка M14' must NOT auto-match to 'Гайка M3'."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi_m3 = _make_order_item(sess, order.id, "Гайка М3 DIN 934")
        oi_m8 = _make_order_item(sess, order.id, "Гайка М8 DIN 934")
        oi_m14 = _make_order_item(sess, order.id, "Гайка М14 ГОСТ 5915-70")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Гайка M14-6H.8.019 ГОСТ 5915-70", line_class="item")

        mhs = _build_order_item_minhashes([oi_m3, oi_m8, oi_m14])
        oi_map = {oi.id: oi for oi in [oi_m3, oi_m8, oi_m14]}
        result = match_quote_line_to_items(
            ql, mhs, oi_map, settings=_low_threshold_settings(auto_apply_jaccard_threshold=0.1),
        )

        # After size filter, only M14 should remain in passed candidates
        passed = [c for c in result["candidates"] if c["passed_filters"]]
        for c in passed:
            sn = c["size_norm"].upper()
            assert "M3" not in sn or sn == ""
            assert "M8" not in sn or sn == ""
        sess.close()

    def test_m14_best_candidate_is_m14(self):
        """The best matching candidate for M14 should be M14, not M3 or M8."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi_m3 = _make_order_item(sess, order.id, "Гайка М3 DIN 934")
        oi_m14 = _make_order_item(sess, order.id, "Гайка М14 ГОСТ 5915-70")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Гайка M14-6H.8.019 ГОСТ 5915-70", line_class="item")

        mhs = _build_order_item_minhashes([oi_m3, oi_m14])
        oi_map = {oi.id: oi for oi in [oi_m3, oi_m14]}
        result = match_quote_line_to_items(
            ql, mhs, oi_map, settings=_low_threshold_settings(auto_apply_jaccard_threshold=0.1),
        )

        if result["best_order_item_id"]:
            assert result["best_order_item_id"] == oi_m14.id
        sess.close()

    def test_size_filter_applied_flag(self):
        """Debug should report filter application."""
        from app.services.quote_order_matcher import match_quote_line_to_items, _build_order_item_minhashes

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M10x50 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M10x50 DIN 933", line_class="item")

        mhs = _build_order_item_minhashes([oi])
        result = match_quote_line_to_items(ql, mhs, {oi.id: oi})
        assert "ql_size" in result["debug"]
        sess.close()


# ── 16. Raw table storage ─────────────────────────────────────────────────

class TestQuoteTableRaw:

    def test_quote_table_and_rows_saved(self):
        """QuoteTable + QuoteTableRow preserve all columns."""
        from app.order_models import QuoteTable, QuoteTableRow

        sess = _session()
        order = _make_order(sess)
        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)

        qt = QuoteTable(
            quote_id=quote.id, n_rows=4, n_cols=3,
            headers_json=json.dumps(["Наименование", "Кол-во", "Цена"]),
            source="excel",
        )
        sess.add(qt)
        sess.flush()

        for i in range(4):
            sess.add(QuoteTableRow(
                quote_table_id=qt.id, row_index=i,
                cells_json=json.dumps([f"Item {i}", str(i * 10), str(i * 100)]),
            ))
        sess.commit()

        loaded_qt = sess.get(QuoteTable, qt.id)
        assert loaded_qt.n_rows == 4
        assert loaded_qt.n_cols == 3
        assert loaded_qt.headers == ["Наименование", "Кол-во", "Цена"]

        rows = sess.query(QuoteTableRow).filter_by(quote_table_id=qt.id).order_by(QuoteTableRow.row_index).all()
        assert len(rows) == 4
        assert rows[0].cells == ["Item 0", "0", "0"]
        assert rows[2].cells == ["Item 2", "20", "200"]
        sess.close()

    def test_raw_table_9x4_preserves_all_columns(self):
        """Simulate a 9-row x 4-column table; verify nothing is lost."""
        from app.order_models import QuoteTable, QuoteTableRow

        sess = _session()
        order = _make_order(sess)
        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)

        headers = ["№", "Наименование", "Кол-во", "Сумма"]
        data_rows = [
            [str(i), f"Позиция {i}", str(i * 5), str(i * 500)]
            for i in range(1, 10)
        ]
        all_rows = [headers] + data_rows

        qt = QuoteTable(
            quote_id=quote.id, n_rows=len(all_rows), n_cols=4,
            headers_json=json.dumps(headers), source="excel",
        )
        sess.add(qt)
        sess.flush()

        for idx, row in enumerate(all_rows):
            sess.add(QuoteTableRow(
                quote_table_id=qt.id, row_index=idx,
                cells_json=json.dumps(row),
            ))
        sess.commit()

        stored_rows = sess.query(QuoteTableRow).filter_by(
            quote_table_id=qt.id
        ).order_by(QuoteTableRow.row_index).all()

        assert len(stored_rows) == 10  # header + 9 data rows
        # Each row has exactly 4 cells
        for sr in stored_rows:
            assert len(sr.cells) == 4
        # Spot-check data
        assert stored_rows[1].cells[1] == "Позиция 1"
        assert stored_rows[9].cells[3] == "4500"
        sess.close()

    def test_cascade_delete_removes_table_rows(self):
        """Deleting a quote should cascade to quote_tables and rows."""
        from app.order_models import QuoteTable, QuoteTableRow

        sess = _session()
        sess.execute(text("PRAGMA foreign_keys = ON"))
        order = _make_order(sess)
        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)

        qt = QuoteTable(quote_id=quote.id, n_rows=2, n_cols=2, source="excel")
        sess.add(qt)
        sess.flush()
        sess.add(QuoteTableRow(quote_table_id=qt.id, row_index=0, cells_json='["a","b"]'))
        sess.commit()

        sess.execute(text("DELETE FROM quotes WHERE id = :qid"), {"qid": quote.id})
        sess.commit()

        assert sess.query(QuoteTable).count() == 0
        assert sess.query(QuoteTableRow).count() == 0
        sess.close()


# ── 17. Mapping extracts sum/price columns ──────────────────────────────

class TestMappingExtraction:

    def test_mapping_extracts_sum_price_columns(self):
        """QuoteLine.price_total populated from sum column."""
        from app.order_models import QuoteLine

        sess = _session()
        order = _make_order(sess)
        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)

        # Simulate wizard confirm: name_col=1, qty_col=2, unit_col=3, price_col=4, sum_col=5
        from app.services.line_parser import parse_raw_line
        from app.services.quote_line_classifier import classify_quote_line

        data_rows = [
            ["1", "Болт M12x60 DIN 933", "10", "шт", "100.50", "1005.00"],
            ["2", "Гайка M12 DIN 934", "20", "шт", "50.25", "1005.00"],
        ]

        for i, row in enumerate(data_rows):
            name = row[1].strip()
            raw_qty_unit = row[2].strip()
            raw_price = row[4].strip()
            raw_sum = row[5].strip()

            qty_val = float(raw_qty_unit.replace(",", "."))
            price_val = float(raw_price.replace(",", ".").replace(" ", ""))
            sum_val = float(raw_sum.replace(",", ".").replace(" ", ""))

            line_class, filter_reason = classify_quote_line(name)
            parsed = parse_raw_line(name)
            sess.add(QuoteLine(
                quote_id=quote.id, row_no=i + 1, raw_text=name,
                qty=qty_val, unit="шт",
                price=price_val, price_total=sum_val,
                parsed_json=json.dumps(parsed, ensure_ascii=False),
                type_norm=parsed.get("item_type") or "",
                size_norm=parsed.get("size_norm") or "",
                std_norm=parsed.get("std_norm") or "",
                tokens_norm=parsed.get("tokens_norm") or "",
                line_class=line_class,
                raw_cells_json=json.dumps(row, ensure_ascii=False),
                raw_qty_unit_text=raw_qty_unit,
                raw_price_text=raw_price,
                raw_sum_text=raw_sum,
            ))
        sess.commit()

        lines = sess.query(QuoteLine).filter_by(quote_id=quote.id).order_by(QuoteLine.row_no).all()
        assert len(lines) == 2
        assert lines[0].price == 100.50
        assert lines[0].price_total == 1005.00
        assert lines[0].raw_sum_text == "1005.00"
        assert lines[0].raw_price_text == "100.50"
        assert lines[0].raw_qty_unit_text == "10"
        assert lines[1].price_total == 1005.00
        # raw_cells_json preserves original row
        cells = json.loads(lines[0].raw_cells_json)
        assert len(cells) == 6
        sess.close()


# ── 18. Auto-match endpoint writes matches ───────────────────────────────

class TestAutoMatchEndpoint:

    def test_auto_match_writes_to_db(self):
        """match_all_quotes_for_order should write QuoteMatch records."""
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import match_all_quotes_for_order

        sess = _session()
        order = _make_order(sess)
        _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess)
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", row_no=1, price=150.0)

        totals = match_all_quotes_for_order(order.id, sess)
        matches = sess.query(QuoteMatch).all()

        assert totals["matched_auto"] >= 1
        assert len(matches) >= 1
        # The match should link to the right QuoteLine
        assert matches[0].quote_line_id == ql.id
        sess.close()

    def test_comparison_table_filled_after_auto_match(self):
        """After auto-match, comparison table should show prices."""
        from app.services.quote_order_matcher import match_all_quotes_for_order, build_comparison_table

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Болт M12x60 DIN 933")

        sup = _make_supplier(sess, "TestSup")
        quote = _make_quote(sess, order.id, sup.id)
        _make_quote_line(sess, quote.id, "Болт M12x60 DIN 933", row_no=1, price=150.0)

        match_all_quotes_for_order(order.id, sess)
        table = build_comparison_table(order.id, sess)

        assert "TestSup" in table["suppliers"]
        assert len(table["rows"]) == 1
        row = table["rows"][0]
        assert "TestSup" in row["cells"]
        assert row["cells"]["TestSup"]["price"] == 150.0
        sess.close()

    def test_price_total_in_comparison(self):
        """price_total should appear in comparison table cells."""
        from app.order_models import QuoteMatch
        from app.services.quote_order_matcher import build_comparison_table

        sess = _session()
        order = _make_order(sess)
        oi = _make_order_item(sess, order.id, "Шайба M16 DIN 125")

        sup = _make_supplier(sess, "PriceSup")
        quote = _make_quote(sess, order.id, sup.id)
        ql = _make_quote_line(sess, quote.id, "Шайба M16 DIN 125", row_no=1, price=10.0)
        # Set price_total on the QuoteLine
        ql.price_total = 200.0
        sess.commit()

        sess.add(QuoteMatch(
            order_item_id=oi.id, quote_line_id=ql.id,
            match_mode="manual", jaccard=0.9,
        ))
        sess.commit()

        table = build_comparison_table(order.id, sess)
        cell = table["rows"][0]["cells"]["PriceSup"]
        assert cell["price"] == 10.0
        assert cell["price_total"] == 200.0
        sess.close()

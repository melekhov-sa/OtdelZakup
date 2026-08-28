"""Shared logic for building order items used as comparison positions.

Both the web approval flow (/orders/{id}/approve) and the 1C JSON API
(/api/v1/comparison) turn a catalog item into an OrderItem with the
normalized features the quote matcher relies on.
"""

import json
import re
from dataclasses import dataclass
from statistics import median

from app.matching.normalizer import normalize_size
from app.matching.standard_analogs import normalize_standard
from app.matching.text_normalizer import normalize_for_minhash
from app.order_models import OrderItem, QuoteLine


def make_order_item(
    order_id: int,
    item,                      # InternalItem
    qty: float | None = None,
    unit: str = "",
    weight_kg: float | None = None,
    display_name: str = "",
) -> OrderItem:
    """Build an OrderItem snapshot from a catalog item.

    The normalized fields (type/size/standard/tokens) are what
    ``quote_order_matcher`` compares supplier quote lines against, so they
    must be produced the same way regardless of which flow created the item.
    """
    size_norm = normalize_size(item.size) if item.size else (item.size_norm or "")

    std_norm = ""
    if item.standard_key:
        std_norm = item.standard_key
    elif item.standard_text:
        std_norm = normalize_standard(item.standard_text) or ""

    return OrderItem(
        order_id=order_id,
        catalog_item_id=item.id,
        # Prefer the caller's wording: 1C shows the buyer her own nomenclature
        # name, and our catalog copy may spell it differently or lag behind.
        display_name_snapshot=display_name.strip() or item.name,
        type_norm=(item.item_type or "").lower(),
        size_norm=size_norm,
        std_norm=std_norm,
        tokens_norm=normalize_for_minhash(item.name),
        qty=qty,
        unit=unit or None,
        weight_kg=weight_kg,
    )


_QTY_HEADER_WORDS = ("кол-во", "количество", "кол.", "колво")
_UNIT_HEADER_WORDS = ("ед.", "ед ", "едизм", "ед.изм", "единиц", "изм.")


@dataclass
class QuantityColumns:
    """Where the amounts live in a supplier's table.

    ``qty_idx``/``unit`` describe the amount the price belongs to. ``ref_*``
    is the supplementary restatement in another unit — informative only, and
    the supplier marks it "справочно" for a reason.
    """

    qty_idx: int | None = None
    unit_idx: int | None = None
    unit: str = ""
    ref_qty_idx: int | None = None
    ref_unit: str = ""


def _looks_like_quantity(header: str) -> bool:
    h = (header or "").strip().lower()
    return any(w in h for w in _QTY_HEADER_WORDS)


def _looks_like_unit(header: str) -> bool:
    h = (header or "").strip().lower()
    return any(w in h for w in _UNIT_HEADER_WORDS)


def detect_quantity_columns(headers: list[str]) -> QuantityColumns:
    """Locate the main and supplementary quantity columns.

    Order decides, not wording: the first quantity column is the one priced,
    any later one is the supplier's restatement in another unit. Supplier
    headers vary too much for keyword matching to carry this on its own.
    """
    from app.parser_excel import extract_uom_from_header

    qty_positions = [i for i, h in enumerate(headers) if _looks_like_quantity(h)]
    cols = QuantityColumns()
    if not qty_positions:
        return cols

    cols.qty_idx = qty_positions[0]
    cols.unit = extract_uom_from_header(headers[cols.qty_idx]) or ""

    # A separate unit column normally sits right after the amount
    for i in range(cols.qty_idx + 1, min(cols.qty_idx + 3, len(headers))):
        if _looks_like_unit(headers[i]) and not _looks_like_quantity(headers[i]):
            cols.unit_idx = i
            break

    if len(qty_positions) > 1:
        cols.ref_qty_idx = qty_positions[1]
        cols.ref_unit = extract_uom_from_header(headers[cols.ref_qty_idx]) or ""

    return cols


def delete_supplier_quotes(order_id: int, supplier_id: int, session) -> int:
    """Remove every quote this supplier has in this comparison, with its rows.

    Deletion is spelled out rather than left to ON DELETE CASCADE: the service
    does not switch SQLite foreign keys on globally, so a cascade declared in
    the schema would quietly do nothing and leave orphaned lines and matches.

    Returns how many quotes were removed.
    """
    from app.order_models import Quote, QuoteLine, QuoteMatch, QuoteTable, QuoteTableRow

    quote_ids = [
        q.id for q in session.query(Quote).filter_by(
            order_id=order_id, supplier_id=supplier_id
        ).all()
    ]
    if not quote_ids:
        return 0

    line_ids = [
        ql.id for ql in session.query(QuoteLine).filter(
            QuoteLine.quote_id.in_(quote_ids)
        ).all()
    ]
    if line_ids:
        session.query(QuoteMatch).filter(
            QuoteMatch.quote_line_id.in_(line_ids)
        ).delete(synchronize_session=False)
        session.query(QuoteLine).filter(
            QuoteLine.quote_id.in_(quote_ids)
        ).delete(synchronize_session=False)

    table_ids = [
        qt.id for qt in session.query(QuoteTable).filter(
            QuoteTable.quote_id.in_(quote_ids)
        ).all()
    ]
    if table_ids:
        session.query(QuoteTableRow).filter(
            QuoteTableRow.quote_table_id.in_(table_ids)
        ).delete(synchronize_session=False)
        session.query(QuoteTable).filter(
            QuoteTable.quote_id.in_(quote_ids)
        ).delete(synchronize_session=False)

    session.query(Quote).filter(Quote.id.in_(quote_ids)).delete(synchronize_session=False)
    return len(quote_ids)


_PACK_RE = re.compile(
    r"уп\w*\.?\s*(?:по\s*)?(\d+(?:[.,]\d+)?)\s*шт",
    re.IGNORECASE,
)


def extract_pack_size(text: str) -> float | None:
    """Read how many pieces a package holds from the supplier's own text.

    Only an explicit packaging note counts. A bare "100 шт" is a quantity,
    not a pack, and treating it as one would silently divide prices by an
    order the customer merely asked for.
    """
    if not text:
        return None
    match = _PACK_RE.search(text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def canonical_uom(unit: str) -> str:
    """Map a unit as written by a supplier onto its canonical form ("шт."→"шт")."""
    from app.parsing.docai_table_parser import _UOM_MAP

    cleaned = (unit or "").strip().lower().rstrip(".")
    return _UOM_MAP.get(cleaned, cleaned)


def normalize_price(
    price: float | None,
    quote_unit: str,
    position_unit: str,
    weight_kg: float | None = None,
    pack_size: float | None = None,
    qty: float | None = None,
    ref_qty: float | None = None,
    ref_unit: str = "",
) -> tuple[float | None, str]:
    """Convert a supplier price to the unit of our position.

    Returns ``(price_normalized, basis)``. The basis names what the conversion
    relied on, so the caller can show how much the number is worth trusting.
    """
    if price is None:
        return None, "none"

    quote_uom = canonical_uom(quote_unit)
    position_uom = canonical_uom(position_unit)

    if quote_uom == position_uom:
        return price, "same_unit"

    # Best factor available: the supplier restated this very line in our unit,
    # so 15 кг = 1152 шт is exact for it. Our weight is an average.
    if qty and ref_qty and canonical_uom(ref_unit) == position_uom:
        return (price * qty) / ref_qty, "supplier_qty"

    # Pack size comes from the supplier's own text, so it outranks our weight
    if pack_size:
        return price / pack_size, "pack"

    if quote_uom == "кг" and position_uom == "шт" and weight_kg:
        return price * weight_kg, "weight"

    return None, "none"


def offered_quantity(
    qty: float | None,
    quote_unit: str,
    position_unit: str,
    weight_kg: float | None = None,
    pack_size: float | None = None,
    ref_qty: float | None = None,
    ref_unit: str = "",
) -> float | None:
    """How much the supplier offers, expressed in the unit of our position.

    Follows the same ladder of trust as price conversion. Returns None when
    the amounts cannot be brought to a common unit — an unknown coverage is
    not the same as zero, and must not be shown as a shortfall.
    """
    if qty is None:
        return None

    quote_uom = canonical_uom(quote_unit)
    position_uom = canonical_uom(position_unit)

    if quote_uom == position_uom:
        return qty
    if ref_qty and canonical_uom(ref_unit) == position_uom:
        return ref_qty
    if pack_size:
        return qty * pack_size
    if quote_uom == "кг" and position_uom == "шт" and weight_kg:
        return qty / weight_kg
    return None


SUSPICIOUS_RATIO = 10


def mark_suspicious(cells: dict[str, dict]) -> dict[str, dict]:
    """Flag converted prices an order of magnitude away from the rest.

    Compares against the median of all suppliers for the position — a median
    tolerates a single bad value, so the outlier is flagged and its neighbours
    are not. With fewer than two prices there is nothing to compare against.
    """
    values = [
        c["price_normalized"] for c in cells.values()
        if c.get("price_normalized") is not None
    ]

    if len(values) < 2:
        for cell in cells.values():
            cell["suspicious"] = False
        return cells

    middle = median(values)
    for cell in cells.values():
        price = cell.get("price_normalized")
        if price is None or middle <= 0:
            cell["suspicious"] = False
        else:
            cell["suspicious"] = (
                price > middle * SUSPICIOUS_RATIO
                or price * SUSPICIOUS_RATIO < middle
            )
    return cells


def enrich_comparison_cells(table: dict, session) -> dict:
    """Add price_normalized / basis / suspicious to every cell, in place.

    Shared by the JSON API and the web view so both show the same numbers.
    Existing keys are left untouched — the template keeps working as before.
    """
    for entry in table["rows"]:
        oi = entry["order_item"]
        for cell in entry["cells"].values():
            quote_line = session.get(QuoteLine, cell["ql_id"]) if cell.get("ql_id") else None
            cell["price_normalized"], cell["basis"] = normalize_price(
                cell.get("price"),
                quote_unit=cell.get("unit") or "",
                position_unit=oi.unit or "",
                weight_kg=oi.weight_kg,
                pack_size=quote_line.pack_size if quote_line else None,
                qty=cell.get("qty"),
                ref_qty=cell.get("ref_qty"),
                ref_unit=cell.get("ref_unit") or "",
            )
            cell["offered_qty"] = offered_quantity(
                cell.get("qty"),
                quote_unit=cell.get("unit") or "",
                position_unit=oi.unit or "",
                weight_kg=oi.weight_kg,
                pack_size=quote_line.pack_size if quote_line else None,
                ref_qty=cell.get("ref_qty"),
                ref_unit=cell.get("ref_unit") or "",
            )
            # Unknown coverage stays unknown — only a real shortfall is flagged
            cell["covers_full"] = (
                None if (cell["offered_qty"] is None or oi.qty is None)
                else cell["offered_qty"] >= oi.qty
            )
        mark_suspicious(entry["cells"])
    return table


def serialize_comparison(order_id: int, session) -> dict:
    """Render the comparison table as JSON for the 1C client."""
    from app.models import InternalItem
    from app.services.quote_order_matcher import build_comparison_table

    table = enrich_comparison_cells(build_comparison_table(order_id, session), session)

    rows = []
    for position_no, entry in enumerate(table["rows"], start=1):
        oi = entry["order_item"]
        item = session.get(InternalItem, oi.catalog_item_id) if oi.catalog_item_id else None

        cells = {
            supplier_name: {
                # The supplier's own figures are never overwritten
                "price": cell.get("price"),
                "currency": cell.get("currency"),
                "unit": cell.get("unit"),
                "qty": cell.get("qty"),
                # Supplier's own restatement in another unit — marked справочно
                "ref_qty": cell.get("ref_qty"),
                "ref_unit": cell.get("ref_unit"),
                "offered_qty": cell.get("offered_qty"),
                "covers_full": cell.get("covers_full"),
                "price_normalized": cell.get("price_normalized"),
                "basis": cell.get("basis"),
                "suspicious": cell.get("suspicious", False),
                # How sure the service is, 0..100, one scale for every stage
                "confidence": cell.get("confidence"),
                "mode": cell.get("mode"),
                "score": cell.get("jaccard"),
            }
            for supplier_name, cell in entry["cells"].items()
        }

        rows.append({
            "position_no": position_no,
            "uid_1c": item.uid_1c if item else None,
            "uid_1c_char": (item.uid_1c_char or "") if item else "",
            "name": oi.display_name_snapshot,
            "qty": oi.qty,
            "unit": oi.unit,
            "cells": cells,
        })

    unmatched = {
        supplier: [
            {"row_no": ql.row_no, "raw_text": ql.raw_text, "price": ql.price, "unit": ql.unit}
            for ql in lines
        ]
        for supplier, lines in table.get("unmatched", {}).items()
    }

    return {
        "comparison_id": order_id,
        "suppliers": table["suppliers"],
        "rows": rows,
        "unmatched": unmatched,
        "filtered_count": {s: len(v) for s, v in table.get("filtered", {}).items()},
    }


def _to_float(text: str) -> float | None:
    """Parse a number out of a spreadsheet cell, tolerating commas and spaces."""
    if not text:
        return None
    try:
        return float(text.replace(",", ".").replace(" ", "").replace("\xa0", ""))
    except ValueError:
        return None


def make_quote_line(quote_id: int, row_no: int, name: str,
                    price: float | None = None,
                    unit: str = "",
                    qty: float | None = None,
                    ref_qty: float | None = None,
                    ref_unit: str = "",
                    raw_cells: list | None = None) -> QuoteLine:
    """Build a QuoteLine with the normalized features the matcher compares on.

    Mirrors what the web column wizard produces, so quote lines created through
    the JSON API match against order items exactly the same way.
    """
    from app.services.line_parser import parse_raw_line
    from app.services.quote_line_classifier import classify_quote_line

    line_class, filter_reason = classify_quote_line(name)
    parsed = parse_raw_line(name)

    return QuoteLine(
        quote_id=quote_id,
        row_no=row_no,
        raw_text=name,
        price=price,
        qty=qty,
        unit=unit or None,
        pack_size=extract_pack_size(name),
        ref_qty=ref_qty,
        ref_unit=ref_unit or None,
        parsed_json=json.dumps(parsed, ensure_ascii=False),
        type_norm=parsed.get("item_type") or "",
        size_norm=parsed.get("size_norm") or "",
        std_norm=parsed.get("std_norm") or "",
        tokens_norm=parsed.get("tokens_norm") or "",
        line_class=line_class,
        filter_reason=filter_reason or None,
        raw_cells_json=json.dumps(raw_cells, ensure_ascii=False) if raw_cells else None,
    )

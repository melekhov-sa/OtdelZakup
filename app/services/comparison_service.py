"""Shared logic for building order items used as comparison positions.

Both the web approval flow (/orders/{id}/approve) and the 1C JSON API
(/api/v1/comparison) turn a catalog item into an OrderItem with the
normalized features the quote matcher relies on.
"""

import json
import re
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
        display_name_snapshot=item.name,
        type_norm=(item.item_type or "").lower(),
        size_norm=size_norm,
        std_norm=std_norm,
        tokens_norm=normalize_for_minhash(item.name),
        qty=qty,
        unit=unit or None,
        weight_kg=weight_kg,
    )


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

    # Pack size comes from the supplier's own text, so it outranks our weight
    if pack_size:
        return price / pack_size, "pack"

    if quote_uom == "кг" and position_uom == "шт" and weight_kg:
        return price * weight_kg, "weight"

    return None, "none"


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
                "price_normalized": cell.get("price_normalized"),
                "basis": cell.get("basis"),
                "suspicious": cell.get("suspicious", False),
                "score": cell.get("jaccard"),
            }
            for supplier_name, cell in entry["cells"].items()
        }

        rows.append({
            "position_no": position_no,
            "uid_1c": item.uid_1c if item else None,
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
        parsed_json=json.dumps(parsed, ensure_ascii=False),
        type_norm=parsed.get("item_type") or "",
        size_norm=parsed.get("size_norm") or "",
        std_norm=parsed.get("std_norm") or "",
        tokens_norm=parsed.get("tokens_norm") or "",
        line_class=line_class,
        filter_reason=filter_reason or None,
        raw_cells_json=json.dumps(raw_cells, ensure_ascii=False) if raw_cells else None,
    )

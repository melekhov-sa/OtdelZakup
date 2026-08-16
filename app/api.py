"""JSON API v1 — endpoints for 1C integration."""

from typing import List, Optional

from fastapi import APIRouter, File, Form, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.cache import (
    UPLOAD_DIR,
    file_id_from_bytes,
    load_dataframe,
    load_meta,
    load_raw_values,
    save_cache,
    save_raw_cache,
    update_cache_with_columns,
)
from app.extractors import EXTRACTORS, transform_dataframe
from app.parser_excel import ParseError, build_dataframe_from_columns, parse_excel
from app.readiness import apply_readiness

router = APIRouter(prefix="/api/v1")

# MIME types for PDF/image files that require Google Document AI
_OCR_MIME: dict[str, str] = {
    ".pdf":  "application/pdf",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".tiff": "image/tiff",
    ".tif":  "image/tiff",
    ".gif":  "image/gif",
    ".webp": "image/webp",
    ".bmp":  "image/bmp",
}


def _error(status: int, msg: str) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=status)


def _df_to_rows(df) -> list[list]:
    """Convert DataFrame to list-of-lists (NaN → None)."""
    return df.where(df.notna(), None).values.tolist()


def _detected_to_dict(detected) -> dict:
    return {
        "name_idx": detected.name_idx,
        "qty_idx": detected.qty_idx,
        "code_idx": detected.code_idx,
        "standard_idx": detected.standard_idx,
        "strength_col_idx": detected.strength_col_idx,
        "note_idx": detected.note_idx,
        "header_row": detected.header_row,
        "method": detected.method,
        "score": detected.score,
    }


# ── POST /api/v1/upload ─────────────────────────────────────


@router.post("/upload")
def api_upload(file: UploadFile = File(...)):
    fname = (file.filename or "").lower()
    if fname.endswith(".xls") and not fname.endswith(".xlsx"):
        return _error(400, "Формат .xls пока не поддерживается, сохраните как .xlsx.")
    if not fname.endswith(".xlsx"):
        return _error(400, "Только файлы .xlsx допускаются для загрузки.")

    file_bytes = file.file.read()

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename
    dest.write_bytes(file_bytes)

    try:
        result = parse_excel(dest)
    except ParseError as exc:
        return _error(400, str(exc))
    except Exception:
        return _error(400, "Не удалось прочитать файл. Убедитесь, что это корректный .xlsx.")

    fid = file_id_from_bytes(file_bytes)

    if result.needs_manual_selection:
        save_raw_cache(fid, file.filename, result.raw_values or [], _detected_to_dict(result.detected))
        preview_rows = (result.raw_values or [])[:21]
        return {
            "file_id": fid,
            "filename": file.filename,
            "needs_column_selection": True,
            "preview_rows": preview_rows,
            "num_columns": max(len(r) for r in preview_rows) if preview_rows else 0,
            "detected": _detected_to_dict(result.detected),
        }

    save_cache(fid, file.filename, result.df, detected_columns=_detected_to_dict(result.detected))
    return {
        "file_id": fid,
        "filename": file.filename,
        "rows_total": len(result.df),
        "columns": list(result.df.columns),
        "needs_column_selection": False,
    }


# ── POST /api/v1/apply-columns ──────────────────────────────


class ApplyColumnsRequest(BaseModel):
    file_id: str
    name_col: int
    qty_col: int = -1
    code_col: int = -1
    header_row: int


@router.post("/apply-columns")
def api_apply_columns(body: ApplyColumnsRequest):
    raw_values = load_raw_values(body.file_id)
    if raw_values is None:
        return _error(404, "not found")

    code_idx = body.code_col if body.code_col >= 0 else None
    qty_idx = body.qty_col if body.qty_col >= 0 else None

    try:
        df = build_dataframe_from_columns(
            raw_values, body.header_row, body.name_col, qty_idx, code_idx
        )
    except ParseError as exc:
        return _error(400, str(exc))
    except Exception:
        return _error(400, "Не удалось построить таблицу с указанными колонками.")

    detected_dict = {
        "name_idx": body.name_col,
        "qty_idx": qty_idx,
        "code_idx": code_idx,
        "header_row": body.header_row,
        "method": "manual",
        "score": 0,
    }
    update_cache_with_columns(body.file_id, df, detected_columns=detected_dict, manual_override=True)
    meta = load_meta(body.file_id)

    return {
        "file_id": body.file_id,
        "filename": meta["filename"],
        "rows_total": len(df),
        "columns": list(df.columns),
    }


# ── GET /api/v1/preview/{file_id} ───────────────────────────


@router.get("/preview/{file_id}")
def api_preview(file_id: str, limit: int = Query(default=200, ge=1)):
    meta = load_meta(file_id)
    if meta is None:
        return _error(404, "not found")

    df = load_dataframe(file_id)
    if df is None:
        return _error(404, "not found")

    preview = df.head(limit)

    return {
        "file_id": file_id,
        "rows_total": meta["rows_total"],
        "limit": limit,
        "columns": list(preview.columns),
        "rows": _df_to_rows(preview),
    }


# ── POST /api/v1/transform ──────────────────────────────────


class TransformRequest(BaseModel):
    file_id: str
    fields: List[str] = []
    limit: int = 200


@router.post("/transform")
def api_transform(body: TransformRequest):
    df = load_dataframe(body.file_id)
    if df is None:
        return _error(404, "not found")

    meta = load_meta(body.file_id)

    valid_fields = [f for f in body.fields if f in EXTRACTORS]
    transformed = transform_dataframe(df, valid_fields)
    transformed = apply_readiness(df, transformed)
    preview = transformed.head(body.limit)

    return {
        "file_id": body.file_id,
        "rows_total": meta["rows_total"],
        "fields": valid_fields,
        "columns": list(preview.columns),
        "rows": _df_to_rows(preview),
    }


# ── POST /api/v1/match-request ──────────────────────────────────────────────


class RequestRow(BaseModel):
    row_no: int = 0
    code: str = ""
    name: str
    qty: Optional[float] = None
    unit: str = ""


class MatchRequestBody(BaseModel):
    rows: List[RequestRow]


# Map decide_match mode → API match_mode label
_MODE_LABEL: dict[str, str] = {
    "AUTO_MEMORY":      "авто",
    "AUTO_EXACT":       "авто",
    "AUTO_MINHASH":     "авто",
    "AUTO_ANALOG":      "авто",
    "AUTO_SCORE":       "авто",
    "SUGGESTED":        "предложено",
    "SUGGESTED_ANALOG": "предложено",
    "NONE":             "не найдено",
    "MANUAL_SELECTED":  "вручную",
    "CONFIRMED":        "подтверждено",
}

# Map combined status → row_status for 1C
_STATUS_SEVERITY = {"ok": 0, "review": 1, "manual": 2}
_CAT_TO_STATUS = {"ok": "ok", "needs_review": "review", "manual_required": "manual"}


def _build_candidate_list(candidates: list, item_by_id: dict, limit: int = 5) -> list:
    """Build top-N candidate dicts for API response."""
    result = []
    for rank, c in enumerate(candidates[:limit], start=1):
        iid = c.get("item_id")
        item = item_by_id.get(iid)
        result.append({
            "rank": rank,
            "uid_1c": item.uid_1c if item else None,
            "name": c.get("name") or (item.name if item else None),
            "score": c.get("score", 0),
            "folder_path": (item.folder_path or "") if item else "",
        })
    return result


def _row_status_and_reason(
    mode: str,
    cv_result,  # CategoryValidationResult | None
    settings,
) -> tuple[str, str]:
    """Return (row_status, reason) combining matcher confidence + category validation."""
    auto_modes = {"AUTO_MEMORY", "AUTO_EXACT", "AUTO_MINHASH", "AUTO_ANALOG", "AUTO_SCORE"}

    # Category validation part
    cv_status = "ok"
    cv_reason = ""
    if cv_result is not None:
        cv_status = _CAT_TO_STATUS.get(cv_result.status, "review")
        if cv_result.missing_fields:
            cv_reason = "Проверка заявки: " + ", ".join(cv_result.missing_labels)

    # Match confidence part
    match_ok = mode in auto_modes

    # Combined status: worst of match confidence and category validation
    if not match_ok:
        match_status = "review"
    else:
        match_status = "ok"

    combined_sev = max(
        _STATUS_SEVERITY.get(match_status, 0),
        _STATUS_SEVERITY.get(cv_status, 0),
    )
    combined = {v: k for k, v in _STATUS_SEVERITY.items()}[combined_sev]

    return combined, cv_reason


@router.post("/match-request")
def api_match_request(body: MatchRequestBody):
    """Match structured product rows against the internal catalog.

    Each row must have at minimum a ``name`` field.
    Returns parsed fields, validation status/reason, and top-3 catalog candidates.

    row_status values:
      ok      — validated and auto-matched with confidence
      review  — candidate found but needs human confirmation, or minor validation issues
      manual  — validation failed on critical fields (size/diameter missing)
    """
    from app.category_validator import load_base_rules, load_exceptions, validate_row
    from app.database import get_db_session
    from app.inference_engine import apply_inference, load_active_inference_rules
    from app.match_settings import load_match_settings
    from app.matcher import decide_match
    from app.models import InternalItem
    from app.services.line_parser import parse_raw_line

    from app.catalog_cache import get_snapshot

    settings = load_match_settings()
    cv_rules = load_base_rules()
    cv_exceptions = load_exceptions()
    inference_rules = load_active_inference_rules()

    session = get_db_session()
    try:
        all_items, item_by_id = get_snapshot()
        from app.matcher import _get_type_size_idx  # noqa: PLC0415
        ts_idx = _get_type_size_idx(all_items)

        rows_out = []
        for idx, row in enumerate(body.rows, start=1):
            raw_text = row.name.strip()
            if not raw_text:
                continue

            row_no = row.row_no if row.row_no else idx

            # Parse extracted fields from name
            parsed = parse_raw_line(raw_text)
            row_dict = {**parsed, "name_raw": raw_text, "name": raw_text}
            row_dict, _ = apply_inference(row_dict, inference_rules)

            # Category validation
            cv_result = validate_row(row_dict, rules=cv_rules, exceptions=cv_exceptions)

            # Catalog matching
            decision = decide_match(
                row_dict, settings,
                session=session, all_items=all_items, item_by_id=item_by_id,
                type_size_idx=ts_idx,
            )

            mode = decision.get("mode", "NONE")
            score = decision.get("score", 0)
            candidates_raw = decision.get("candidates", [])
            candidates = _build_candidate_list(candidates_raw, item_by_id)

            row_status, reason = _row_status_and_reason(mode, cv_result, settings)

            # Best match (top-1)
            top = candidates[0] if candidates else None
            match_out = None
            if top:
                match_out = {
                    "uid_1c": top["uid_1c"],
                    "name": top["name"],
                    "score": top["score"],
                    "match_mode": _MODE_LABEL.get(mode, mode),
                    "candidates": candidates,
                }

            rows_out.append({
                "row_no": row_no,
                "code": row.code or None,
                "name": raw_text,
                "qty": row.qty,
                "unit": row.unit or None,
                "item_type": parsed.get("item_type") or None,
                "item_subtype": parsed.get("item_subtype") or None,
                "size": parsed.get("size") or None,
                "diameter": parsed.get("diameter") or None,
                "length": parsed.get("length") or None,
                "gost": parsed.get("gost") or None,
                "din": parsed.get("din") or None,
                "iso": parsed.get("iso") or None,
                "strength": parsed.get("strength") or None,
                "material": parsed.get("material") or None,
                "coating": parsed.get("coating") or None,
                "paint_color": parsed.get("paint_color") or None,
                "item_type_source": "из текста" if parsed.get("item_type") else None,
                "row_status": row_status,
                "reason": reason or None,
                "match": match_out,
            })

        return {"rows": rows_out}
    finally:
        session.close()


# ── POST /api/v1/process-quote ──────────────────────────────────────────────


def _detect_price_col(headers: list[str]) -> int | None:
    """Return index of price column from header names, or None."""
    for i, h in enumerate(headers):
        hl = h.strip().lower()
        if any(kw in hl for kw in ("цена", "price", "стоимость", "прайс")):
            return i
    return None


def _detect_name_col(headers: list[str]) -> int:
    """Return index of name column from header names, defaulting to 0."""
    for i, h in enumerate(headers):
        hl = h.strip().lower()
        if any(kw in hl for kw in ("наименование", "название", "позиция", "товар", "name")):
            return i
    return 0


def _detect_unit_col(headers: list[str]) -> int | None:
    for i, h in enumerate(headers):
        hl = h.strip().lower()
        if any(kw in hl for kw in ("ед", "единиц", "unit", "изм")):
            return i
    return None


@router.post("/process-quote")
def api_process_quote(
    file: UploadFile = File(...),
    supplier: str = Form(default=""),
):
    """Parse a supplier quote file (Excel/CSV) and match lines to catalog.

    Multipart form-data:
      file     — Excel (.xlsx) or CSV file
      supplier — supplier name (string)

    Each row gets the same status/candidates fields as /match-request.
    Additionally includes price/currency/unit from the quote file when detected.
    """
    from app.database import get_db_session
    from app.match_settings import load_match_settings
    from app.matcher import decide_match
    from app.models import InternalItem
    from app.services.line_parser import parse_raw_line, read_tabular_file

    file_bytes = file.file.read()
    filename = file.filename or "upload.xlsx"

    try:
        all_rows = read_tabular_file(file_bytes, filename)
    except Exception as exc:
        return _error(400, f"Не удалось прочитать файл: {exc}")

    if not all_rows:
        return {"supplier": supplier, "rows": []}

    headers = all_rows[0]
    data_rows = all_rows[1:]

    name_col = _detect_name_col(headers)
    price_col = _detect_price_col(headers)
    unit_col = _detect_unit_col(headers)

    from app.catalog_cache import get_snapshot

    settings = load_match_settings()
    session = get_db_session()
    try:
        all_items, item_by_id = get_snapshot()
        from app.matcher import _get_type_size_idx  # noqa: PLC0415
        ts_idx = _get_type_size_idx(all_items)

        rows_out = []
        for i, row in enumerate(data_rows, start=1):
            raw_text = row[name_col].strip() if name_col < len(row) else ""
            if not raw_text:
                continue

            price = None
            if price_col is not None and price_col < len(row):
                try:
                    price = float(str(row[price_col]).replace(",", ".").replace(" ", ""))
                except (ValueError, TypeError):
                    pass

            unit = ""
            if unit_col is not None and unit_col < len(row):
                unit = str(row[unit_col]).strip()

            parsed = parse_raw_line(raw_text)
            row_dict = {**parsed, "name_raw": raw_text, "name": raw_text}

            decision = decide_match(
                row_dict, settings,
                session=session, all_items=all_items, item_by_id=item_by_id,
                type_size_idx=ts_idx,
            )

            mode = decision.get("mode", "NONE")
            candidates_raw = decision.get("candidates", [])
            row_status, reason = _row_status_and_reason(mode, None, settings)

            # Inject price/unit into top candidate for convenience
            candidates = _build_candidate_list(candidates_raw, item_by_id)
            if candidates and price is not None:
                candidates[0]["price"] = price
                candidates[0]["currency"] = "RUB"
                candidates[0]["unit"] = unit or None

            rows_out.append({
                "row_no": i,
                "raw_text": raw_text,
                "price": price,
                "currency": "RUB" if price is not None else None,
                "unit": unit or None,
                "parsed": {
                    "item_type": parsed.get("item_type") or None,
                    "item_subtype": parsed.get("item_subtype") or None,
                    "size": parsed.get("size") or None,
                    "gost": parsed.get("gost") or None,
                },
                "row_status": row_status,
                "reason": reason or None,
                "candidates": candidates,
            })

        return {"supplier": supplier, "rows": rows_out}
    finally:
        session.close()


# ── POST /api/v1/comparison ────────────────────────────────────────────────
# Creates a quote-comparison container from a position list verified by МЗ.
# Positions arrive as catalog references (uid_1c), so nothing has to be parsed.


class ComparisonPosition(BaseModel):
    uid_1c: str
    qty: Optional[float] = None
    unit: str = ""
    weight_kg: Optional[float] = None


class CreateComparisonBody(BaseModel):
    external_ref: str = ""
    title: str = ""
    positions: List[ComparisonPosition] = []


@router.post("/comparison")
def api_create_comparison(body: CreateComparisonBody):
    """Create (or replace) a comparison from a verified position list.

    JSON body:
      external_ref — identifier on the 1C side; reused to replace positions
      title        — human-readable name
      positions    — [{uid_1c, qty, unit, weight_kg}]

    Unknown uid_1c values are reported in ``not_found`` and skipped —
    the request itself does not fail.
    """
    from app.database import get_db_session
    from app.models import InternalItem
    from app.order_models import Order, OrderItem
    from app.services.comparison_service import make_order_item

    session = get_db_session()
    try:
        order = None
        if body.external_ref:
            order = session.query(Order).filter_by(external_ref=body.external_ref).first()

        if order is None:
            order = Order(
                title=body.title or body.external_ref or "Сравнение КП",
                external_ref=body.external_ref or None,
                status="approved_catalog",
            )
            session.add(order)
            session.flush()
        else:
            # Replacing positions of an existing comparison
            session.query(OrderItem).filter_by(order_id=order.id).delete()
            # The matcher caches a MinHash index per order — drop it, or quotes
            # uploaded next would be matched against the positions we just removed
            from app.services.quote_order_matcher import invalidate_index
            invalidate_index(order.id)
            if body.title:
                order.title = body.title

        not_found: list[str] = []
        created = 0
        for pos in body.positions:
            item = session.query(InternalItem).filter_by(uid_1c=pos.uid_1c).first()
            if item is None:
                not_found.append(pos.uid_1c)
                continue
            session.add(make_order_item(
                order.id, item,
                qty=pos.qty, unit=pos.unit, weight_kg=pos.weight_kg,
            ))
            created += 1

        session.commit()
        return {
            "comparison_id": order.id,
            "positions_total": created,
            "not_found": not_found,
        }
    finally:
        session.close()


# ── POST /api/v1/comparison/{id}/quotes ────────────────────────────────────
# Supplier price list, Base64 in JSON — 1C cannot build multipart reliably.


def _ocr_quote_lines(file_bytes: bytes, filename: str, hint: str = "") -> list[dict]:
    """Read a scanned or PDF price list through Google Document AI.

    Unlike ``_parse_ocr_file`` this keeps the price — for a quote that is the
    whole point of the document.
    """
    from pathlib import Path as _Path  # noqa: PLC0415

    from app.integrations.google_document_ai import process_document  # noqa: PLC0415
    from app.services.google_ocr_extractor import extract_rows  # noqa: PLC0415

    mime_type = _OCR_MIME.get(_Path(filename).suffix.lower(), "application/pdf")
    result = extract_rows(process_document(file_bytes, mime_type), hint=hint)

    lines: list[dict] = []
    if result.structured_rows:
        for sr in result.structured_rows:
            name = (sr.get("name") or "").strip()
            if name:
                lines.append({
                    "name": name,
                    "price": sr.get("price_unit") or sr.get("price_total"),
                    "unit": sr.get("unit") or "",
                    "qty": sr.get("qty"),
                })
    else:
        for row in result.rows:
            cells = row if isinstance(row, list) else [str(row)]
            name = " ".join(c for c in cells if c.strip()).strip()
            if name:
                lines.append({"name": name, "price": None, "unit": "", "raw_cells": cells})

    return lines


class UploadQuoteBody(BaseModel):
    supplier: str
    file_base64: str
    filename: str = "quote.xlsx"
    hint: str = ""


@router.post("/comparison/{comparison_id}/quotes")
def api_upload_quote(comparison_id: int, body: UploadQuoteBody):
    """Attach a supplier quote to a comparison and match its lines.

    JSON body:
      supplier    — supplier name (created on first use)
      file_base64 — Base64-encoded price list
      filename    — original name, used to detect the format
      hint        — extraction hint for PDF/images
    """
    import base64

    from app.database import get_db_session
    from app.order_models import Order, Quote, Supplier
    from app.services.comparison_service import _to_float, make_quote_line
    from app.services.line_parser import read_tabular_file

    if not body.supplier.strip():
        return _error(400, "Не указан поставщик.")

    try:
        file_bytes = base64.b64decode(body.file_base64)
    except Exception:
        return _error(400, "Не удалось декодировать Base64.")

    session = get_db_session()
    try:
        order = session.get(Order, comparison_id)
        if order is None:
            return _error(404, "Сравнение не найдено.")

        from pathlib import Path as _Path  # noqa: PLC0415

        is_scan = _Path(body.filename).suffix.lower() in _OCR_MIME
        if is_scan:
            try:
                parsed_lines = _ocr_quote_lines(file_bytes, body.filename, body.hint)
            except Exception as exc:
                return _error(400, f"Ошибка распознавания файла: {exc}")
        else:
            try:
                all_rows = read_tabular_file(file_bytes, body.filename)
            except Exception as exc:
                return _error(400, f"Не удалось прочитать файл: {exc}")

            if not all_rows:
                return _error(400, "Файл пуст.")

            headers = all_rows[0]
            name_col = _detect_name_col(headers)
            price_col = _detect_price_col(headers)
            unit_col = _detect_unit_col(headers)

            parsed_lines = []
            for row in all_rows[1:]:
                parsed_lines.append({
                    "name": row[name_col].strip() if name_col < len(row) else "",
                    "price": _to_float(row[price_col].strip())
                             if price_col is not None and price_col < len(row) else None,
                    "unit": row[unit_col].strip()
                            if unit_col is not None and unit_col < len(row) else "",
                    "raw_cells": row,
                })

        supplier = session.query(Supplier).filter_by(name=body.supplier.strip()).first()
        if supplier is None:
            supplier = Supplier(name=body.supplier.strip())
            session.add(supplier)
            session.flush()

        quote = Quote(
            order_id=comparison_id,
            supplier_id=supplier.id,
            source_filename=body.filename,
            source_kind="pdf" if is_scan else "excel",
        )
        session.add(quote)
        session.flush()

        created = 0
        for i, line in enumerate(parsed_lines, start=1):
            name = (line.get("name") or "").strip()
            if not name:
                continue
            session.add(make_quote_line(
                quote.id, i, name,
                price=line.get("price"),
                unit=line.get("unit") or "",
                qty=line.get("qty"),
                raw_cells=line.get("raw_cells"),
            ))
            created += 1

        if order.status == "approved_catalog":
            order.status = "quotes"
        session.commit()
        quote_id = quote.id

        from app.services.quote_order_matcher import match_quote_to_order_items
        stats = match_quote_to_order_items(quote_id, session)
        session.commit()

        auto = stats.get("matched_auto", 0)
        suggested = stats.get("suggested", 0)
        return {
            "quote_id": quote_id,
            "lines_total": created,
            # Anything that produced a link shows up in the comparison table
            "matched": auto + suggested,
            "matched_auto": auto,
            "suggested": suggested,
            "unmatched": stats.get("unmatched", 0),
            "filtered": stats.get("filtered_out", 0),
        }
    finally:
        session.close()


# ── GET /api/v1/comparison/{id} ────────────────────────────────────────────


@router.get("/comparison/{comparison_id}")
def api_get_comparison(comparison_id: int):
    """Return the price comparison table: position × supplier."""
    from app.database import get_db_session
    from app.order_models import Order
    from app.services.comparison_service import serialize_comparison

    session = get_db_session()
    try:
        if session.get(Order, comparison_id) is None:
            return _error(404, "Сравнение не найдено.")
        return serialize_comparison(comparison_id, session)
    finally:
        session.close()


# ── POST /api/v1/parse-request-base64 ──────────────────────────────────────
# Accepts JSON with file_base64 + filename fields (used by 1C client).


class ParseRequestBase64Body(BaseModel):
    file_base64: str
    filename: str = "upload.xlsx"
    hint: str = ""


@router.post("/parse-request-base64")
def api_parse_request_base64(body: ParseRequestBase64Body):
    """Parse a client request file sent as Base64-encoded JSON.

    JSON body:
      file_base64 — Base64-encoded file content
      filename    — original filename with extension (used to detect format)
      hint        — extraction hint for PDF/image: "table" | "structured_text" | "free_text"

    Returns same format as /parse-request.
    """
    import base64
    import io

    from fastapi import UploadFile  # noqa: PLC0415

    try:
        file_bytes = base64.b64decode(body.file_base64)
    except Exception:
        return _error(400, "Не удалось декодировать Base64.")

    fake_file = UploadFile(filename=body.filename, file=io.BytesIO(file_bytes))
    return api_parse_request(file=fake_file, text="", hint=body.hint)


def _parse_ocr_file(file_bytes: bytes, filename: str, hint: str = "") -> list[dict]:
    """Send PDF/image to Google Document AI and return list of {name, qty, unit}."""
    from pathlib import Path as _Path  # noqa: PLC0415

    from app.integrations.google_document_ai import (  # noqa: PLC0415
        is_configured,
        process_document,
    )
    from app.services.google_ocr_extractor import extract_rows  # noqa: PLC0415

    if not is_configured():
        raise ValueError(
            "Google Document AI не настроен. "
            "Укажите параметры в настройках /settings/google-ocr."
        )

    ext = _Path(filename).suffix.lower()
    mime_type = _OCR_MIME.get(ext, "application/pdf")

    document = process_document(file_bytes, mime_type)
    result = extract_rows(document, hint=hint)

    parsed_rows: list[dict] = []
    if result.structured_rows:
        for sr in result.structured_rows:
            name = sr.get("name", "").strip()
            if name:
                parsed_rows.append({
                    "name": name,
                    "qty": sr.get("qty"),
                    "unit": sr.get("unit") or "",
                })
    else:
        for row in result.rows:
            name = " | ".join(c for c in row if c.strip())
            if name:
                parsed_rows.append({"name": name, "qty": None, "unit": ""})

    return parsed_rows


# ── POST /api/v1/parse-request ─────────────────────────────────────────────
# Unified endpoint for 1C integration: accepts text OR file (Excel/CSV/PDF/image),
# parses rows, extracts fields, matches against catalog, validates.


@router.post("/parse-request")
def api_parse_request(
    text: str = Form(default=""),
    file: Optional[UploadFile] = File(default=None),
    hint: str = Form(default=""),
):
    """Parse a client request from text or file, extract fields, match catalog.

    Multipart form-data — send ONE of:
      text  — free-form text (each line = one position)
      file  — Excel (.xlsx), CSV, PDF, or image file

    Optional:
      hint  — extraction hint for PDF/image: "table" | "structured_text" | "free_text"
               (default "table" — prefer table extraction)

    Returns unified JSON with parsed rows, extracted fields, catalog matches,
    and validation messages about missing/incomplete data.
    """
    from app.category_validator import load_base_rules, load_exceptions, validate_row
    from app.database import get_db_session
    from app.match_settings import load_match_settings
    from app.matcher import decide_match
    from app.models import InternalItem
    from app.services.line_parser import parse_client_file, parse_raw_line
    from app.text_input.parser import parse_text_to_rows

    # ── Step 1: parse input into list of {name, qty, unit} ──
    input_type = "text"
    parsed_rows: list[dict] = []

    if file is not None and file.filename:
        from pathlib import Path as _Path  # noqa: PLC0415

        filename = file.filename or "upload.xlsx"
        file_bytes = file.file.read()
        ext = _Path(filename).suffix.lower()

        if ext in _OCR_MIME:
            # PDF or image — use Google Document AI
            input_type = "file_ocr"
            try:
                parsed_rows = _parse_ocr_file(file_bytes, filename, hint=hint)
            except Exception as exc:
                return _error(400, f"Ошибка распознавания файла: {exc}")
        else:
            input_type = "file"
            try:
                parsed_rows = parse_client_file(file_bytes, filename)
            except Exception as exc:
                return _error(400, f"Не удалось прочитать файл: {exc}")
    elif text.strip():
        input_type = "text"
        from app.parsing.tail_extractor import load_active_tail_phrases
        tail_phrases = load_active_tail_phrases()
        text_rows = parse_text_to_rows(text.strip(), tail_phrases=tail_phrases)
        for tr in text_rows:
            parsed_rows.append({
                "name": tr.get("name", ""),
                "qty": tr.get("qty"),
                "unit": tr.get("uom"),
            })
    else:
        return _error(400, "Передайте текст (text) или файл (file).")

    if not parsed_rows:
        return {"input_type": input_type, "rows_total": 0, "rows": []}

    # ── Step 2: extract fields + match + validate each row ──
    from app.catalog_cache import get_snapshot

    from app.inference_engine import apply_inference, load_active_inference_rules

    settings = load_match_settings()
    cv_rules = load_base_rules()
    cv_exceptions = load_exceptions()
    inference_rules = load_active_inference_rules()

    session = get_db_session()
    try:
        all_items, item_by_id = get_snapshot()
        from app.matcher import _get_type_size_idx  # noqa: PLC0415
        ts_idx = _get_type_size_idx(all_items)

        rows_out = []
        for idx, pr in enumerate(parsed_rows, start=1):
            raw_text = pr.get("name", "").strip()
            if not raw_text:
                continue

            qty = pr.get("qty")
            unit = pr.get("unit") or ""

            parsed = parse_raw_line(raw_text)
            row_dict = {**parsed, "name_raw": raw_text, "name": raw_text}
            row_dict, _ = apply_inference(row_dict, inference_rules)

            # Category validation
            cv_result = validate_row(row_dict, rules=cv_rules, exceptions=cv_exceptions)

            # Catalog matching
            decision = decide_match(
                row_dict, settings,
                session=session, all_items=all_items, item_by_id=item_by_id,
                type_size_idx=ts_idx,
            )

            mode = decision.get("mode", "NONE")
            candidates_raw = decision.get("candidates", [])
            candidates = _build_candidate_list(candidates_raw, item_by_id)

            row_status, reason = _row_status_and_reason(mode, cv_result, settings)

            # Best match
            top = candidates[0] if candidates else None
            match_out = None
            if top:
                match_out = {
                    "uid_1c": top["uid_1c"],
                    "name": top["name"],
                    "score": top["score"],
                    "match_mode": _MODE_LABEL.get(mode, mode),
                    "candidates": candidates,
                }

            # Collect missing/incomplete fields for 1C
            missing_fields = []
            if cv_result is not None:
                # Use category validator — rules + exceptions aware
                missing_fields.extend(cv_result.missing_labels)
            else:
                # Fallback for items whose category wasn't recognized
                if not parsed.get("size") and not parsed.get("diameter"):
                    missing_fields.append("размер")
                if not parsed.get("gost") and not parsed.get("din") and not parsed.get("iso"):
                    missing_fields.append("стандарт (ГОСТ/DIN/ISO)")
                if not parsed.get("strength"):
                    missing_fields.append("класс прочности")
            if qty is None:
                missing_fields.append("количество")

            rows_out.append({
                "row_no": idx,
                "name": raw_text,
                "qty": qty,
                "unit": unit or None,
                "item_type": row_dict.get("item_type") or None,
                "item_subtype": row_dict.get("item_subtype") or None,
                "size": row_dict.get("size") or None,
                "diameter": row_dict.get("diameter") or None,
                "length": row_dict.get("length") or None,
                "gost": row_dict.get("gost") or None,
                "din": row_dict.get("din") or None,
                "iso": row_dict.get("iso") or None,
                "strength": row_dict.get("strength") or None,
                "material": row_dict.get("material") or None,
                "coating": row_dict.get("coating") or None,
                "paint_color": row_dict.get("paint_color") or None,
                "row_status": row_status,
                "reason": reason or None,
                "missing_fields": missing_fields,
                "match": match_out,
            })

        return {
            "input_type": input_type,
            "rows_total": len(rows_out),
            "rows": rows_out,
        }
    finally:
        session.close()

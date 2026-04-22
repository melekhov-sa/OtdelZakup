"""Routes for the Orders & Quote Comparison module."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session


order_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

STATUS_LABELS = {
    "draft": "Черновик",
    "matching_catalog": "Подбор номенклатуры",
    "approved_catalog": "Номенклатура утверждена",
    "quotes": "Загрузка КП",
    "done": "Готово",
}


# ── Orders CRUD ──────────────────────────────────────────────────────────────


@order_router.get("/orders", response_class=HTMLResponse)
def orders_list(request: Request):
    from sqlalchemy import func
    from app.order_models import ClientLine, Order, OrderItem, Quote

    session = get_db_session()
    try:
        orders = session.query(Order).order_by(Order.created_at.desc()).all()
        cl_counts = dict(
            session.query(ClientLine.order_id, func.count(ClientLine.id))
            .group_by(ClientLine.order_id).all()
        )
        oi_counts = dict(
            session.query(OrderItem.order_id, func.count(OrderItem.id))
            .group_by(OrderItem.order_id).all()
        )
        q_counts = dict(
            session.query(Quote.order_id, func.count(Quote.id))
            .group_by(Quote.order_id).all()
        )
        order_data = [{
            "order": o,
            "client_lines_count": cl_counts.get(o.id, 0),
            "order_items_count": oi_counts.get(o.id, 0),
            "quotes_count": q_counts.get(o.id, 0),
            "status_label": STATUS_LABELS.get(o.status, o.status),
        } for o in orders]
    finally:
        session.close()

    return templates.TemplateResponse("orders_list.html", {
        "request": request, "order_data": order_data,
    })


@order_router.get("/orders/new", response_class=HTMLResponse)
def order_new_form(request: Request):
    return templates.TemplateResponse("order_form.html", {"request": request})


@order_router.post("/orders/new")
def order_create(title: str = Form(...)):
    from app.order_models import Order

    session = get_db_session()
    try:
        order = Order(title=title.strip())
        session.add(order)
        session.commit()
        oid = order.id
    finally:
        session.close()
    return RedirectResponse(f"/orders/{oid}", status_code=303)


@order_router.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(request: Request, order_id: int):
    from app.order_models import Order

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)
    finally:
        session.close()
    return templates.TemplateResponse("order_wizard.html", {
        "request": request, "order": order,
        "status_label": STATUS_LABELS.get(order.status, order.status),
        "STATUS_LABELS": STATUS_LABELS,
    })


@order_router.post("/orders/{order_id}/delete")
def order_delete(order_id: int):
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        session.execute(text("DELETE FROM orders WHERE id = :oid"), {"oid": order_id})
        session.commit()
    finally:
        session.close()
    return RedirectResponse("/orders", status_code=303)


# ── Client Request Upload ────────────────────────────────────────────────────


@order_router.get("/orders/{order_id}/upload-client", response_class=HTMLResponse)
def upload_client_form(request: Request, order_id: int):
    from app.order_models import Order

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)
    finally:
        session.close()

    ocr_available = False
    try:
        from app.integrations.google_document_ai import is_configured
        ocr_available = is_configured()
    except Exception:
        pass

    return templates.TemplateResponse("upload_client.html", {
        "request": request, "order": order, "ocr_available": ocr_available,
    })


@order_router.post("/orders/{order_id}/upload-client")
def upload_client(
    request: Request,
    order_id: int,
    file: UploadFile = File(None),
    source_kind: str = Form("excel"),
    text_input: str = Form(""),
):
    from app.order_models import ClientLine, Order
    from app.services.line_parser import parse_client_file, parse_raw_line, read_tabular_file

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)

        rows: list[dict] = []  # [{name, qty, unit}]

        if source_kind == "text" and text_input.strip():
            from app.text_input.parser import parse_text_line
            for i, line in enumerate(text_input.strip().splitlines()):
                line = line.strip()
                if line:
                    parsed = parse_text_line(line)
                    rows.append({"name": parsed["name"], "qty": parsed["qty"], "unit": parsed["unit"] or ""})

        elif file and file.filename:
            file_bytes = file.file.read()
            ext = (file.filename.rsplit(".", 1)[-1] if "." in file.filename else "").lower()

            if source_kind in ("pdf", "photo") or ext in ("pdf", "png", "jpg", "jpeg", "tiff", "bmp"):
                # Google Document AI OCR
                from app.integrations.google_document_ai import process_document
                from app.services.google_ocr_extractor import extract_rows

                mime_map = {
                    "pdf": "application/pdf",
                    "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
                    "tiff": "image/tiff", "bmp": "image/bmp",
                }
                mime = mime_map.get(ext, "application/pdf")
                doc = process_document(file_bytes, mime)
                result = extract_rows(doc)
                if result.structured_rows:
                    for sr in result.structured_rows:
                        if sr.get("name"):
                            rows.append({
                                "name": sr["name"],
                                "qty": sr.get("qty"),
                                "unit": sr.get("unit") or "",
                            })
                else:
                    for row_cells in result.rows:
                        name = " ".join(c for c in row_cells if c.strip()) if isinstance(row_cells, list) else str(row_cells)
                        name = name.strip()
                        if name:
                            rows.append({"name": name, "qty": None, "unit": ""})
            else:
                # Excel / CSV
                rows = parse_client_file(file_bytes, file.filename)

        if not rows:
            return RedirectResponse(f"/orders/{order_id}/upload-client", status_code=303)

        # Delete existing client lines for this order (re-upload replaces)
        session.query(ClientLine).filter_by(order_id=order_id).delete()

        # ── Quality: pipeline run + parse step ────────────────────────────
        from app.services.quality_service import create_pipeline_run, track_step, compute_field_recognition
        pipeline_run = create_pipeline_run(order_id=order_id, session=session)

        parsed_list = []
        with track_step(pipeline_run, "parse_client_request", session, input_rows=len(rows)) as step:
            parsed_ok = 0
            for i, row in enumerate(rows):
                parsed = parse_raw_line(row["name"])
                parsed_list.append(parsed)
                has_fields = bool(parsed.get("item_type") or parsed.get("size_norm"))
                if has_fields:
                    parsed_ok += 1
                cl = ClientLine(
                    order_id=order_id,
                    row_no=i + 1,
                    raw_text=row["name"],
                    qty=row.get("qty"),
                    unit=row.get("unit") or None,
                    parsed_json=json.dumps(parsed, ensure_ascii=False),
                    status="ok" if has_fields else "needs_manual",
                )
                session.add(cl)
            step.output_rows = parsed_ok
            step.success_rate = parsed_ok / len(rows) if rows else 0
            step.extra = compute_field_recognition(parsed_list)

        pipeline_run.total_client_lines = len(rows)
        pipeline_run.parsed_client_lines = parsed_ok

        order.status = "matching_catalog"
        session.commit()
    finally:
        session.close()

    return RedirectResponse(f"/orders/{order_id}/client-lines", status_code=303)


# ── Client Lines ─────────────────────────────────────────────────────────────


@order_router.get("/orders/{order_id}/client-lines", response_class=HTMLResponse)
def client_lines_table(request: Request, order_id: int):
    from app.order_models import ClientLine, Order
    from app.models import InternalItem

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)
        lines = session.query(ClientLine).filter_by(order_id=order_id).order_by(ClientLine.row_no).all()

        # Load chosen catalog items
        chosen_ids = [cl.chosen_catalog_item_id for cl in lines if cl.chosen_catalog_item_id]
        items_map = {}
        if chosen_ids:
            items = session.query(InternalItem).filter(InternalItem.id.in_(chosen_ids)).all()
            items_map = {it.id: it for it in items}

        line_data = []
        for cl in lines:
            item = items_map.get(cl.chosen_catalog_item_id) if cl.chosen_catalog_item_id else None
            line_data.append({"line": cl, "catalog_item": item})
    finally:
        session.close()

    # analog_mode from query param (preserved after redirect), fallback to global setting
    analog_mode = request.query_params.get("analog_mode", "")
    if analog_mode not in ("off", "with", "only"):
        from app.match_settings import load_match_settings
        ms = load_match_settings()
        analog_mode = "with" if ms.use_standard_analogs_in_main_match else "off"

    return templates.TemplateResponse("client_lines.html", {
        "request": request, "order": order, "line_data": line_data,
        "status_label": STATUS_LABELS.get(order.status, order.status),
        "analog_mode": analog_mode,
    })


# ── Catalog Matching ─────────────────────────────────────────────────────────


@order_router.post("/orders/{order_id}/match-catalog")
def match_catalog(order_id: int, analog_mode: str = Form("off")):
    import dataclasses
    from app.order_models import ClientLine, Order
    from app.match_settings import load_match_settings
    from app.matcher import decide_match

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)

        settings = load_match_settings()
        if analog_mode == "with":
            settings = dataclasses.replace(settings, use_standard_analogs_in_main_match=True, analogs_only=False)
        elif analog_mode == "only":
            settings = dataclasses.replace(settings, use_standard_analogs_in_main_match=False, analogs_only=True)
        else:
            settings = dataclasses.replace(settings, use_standard_analogs_in_main_match=False, analogs_only=False)
        lines = session.query(ClientLine).filter_by(order_id=order_id).all()

        # Preload catalog items once for the whole batch (shared snapshot)
        from app.catalog_cache import get_snapshot
        all_items, item_by_id = get_snapshot()

        # ── Quality: pipeline run + catalog_match step ────────────────────
        from app.services.quality_service import create_pipeline_run, track_step
        from app.quality_models import PipelineRun
        # Try to find existing run for this order, or create new
        existing_run = (
            session.query(PipelineRun)
            .filter_by(order_id=order_id)
            .order_by(PipelineRun.id.desc())
            .first()
        )
        pipeline_run = existing_run or create_pipeline_run(order_id=order_id, session=session)

        auto_count = 0
        manual_count = 0
        to_match = [cl for cl in lines if not cl.chosen_catalog_item_id]

        with track_step(pipeline_run, "catalog_match", session, input_rows=len(to_match)) as step:
            for cl in to_match:
                parsed = cl.parsed
                row_dict = {
                    "name": cl.raw_text,
                    "name_raw": cl.raw_text,
                    "item_type": parsed.get("item_type", ""),
                    "size": parsed.get("size", ""),
                    "diameter": parsed.get("diameter", ""),
                    "length": parsed.get("length", ""),
                    "gost": parsed.get("gost", ""),
                    "iso": parsed.get("iso", ""),
                    "din": parsed.get("din", ""),
                    "strength": parsed.get("strength", ""),
                    "coating": parsed.get("coating", ""),
                }
                result = decide_match(row_dict, settings, session, all_items=all_items, item_by_id=item_by_id)
                if result.get("internal_item_id"):
                    cl.chosen_catalog_item_id = result["internal_item_id"]
                    cl.chosen_by = "auto"
                    cl.chosen_at = datetime.now(timezone.utc)
                    cl.status = "ok"
                    auto_count += 1
                else:
                    cl.status = "needs_manual"

            step.output_rows = auto_count
            step.success_rate = auto_count / len(to_match) if to_match else 0
            step.extra = {"analog_mode": analog_mode}

        # Count already manually matched
        manual_count = sum(1 for cl in lines if cl.chosen_by == "manual")
        pipeline_run.auto_matches = auto_count
        pipeline_run.manual_matches = manual_count

        session.commit()
    finally:
        session.close()

    return RedirectResponse(f"/orders/{order_id}/client-lines?analog_mode={analog_mode}", status_code=303)


@order_router.get("/orders/{order_id}/client-lines/{cl_id}/choose-catalog", response_class=HTMLResponse)
def choose_catalog_form(request: Request, order_id: int, cl_id: int, analog_mode: str = "off"):
    import dataclasses as _dc
    from app.order_models import ClientLine, Order
    from app.match_settings import load_match_settings
    from app.matcher import decide_match

    if analog_mode not in ("off", "with", "only"):
        analog_mode = "off"

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        cl = session.get(ClientLine, cl_id)
        if not order or not cl:
            return HTMLResponse("Not found", status_code=404)

        parsed = cl.parsed
        row_dict = {
            "name": cl.raw_text, "name_raw": cl.raw_text,
            "item_type": parsed.get("item_type", ""),
            "size": parsed.get("size", ""),
            "diameter": parsed.get("diameter", ""),
            "length": parsed.get("length", ""),
            "gost": parsed.get("gost", ""),
            "iso": parsed.get("iso", ""),
            "din": parsed.get("din", ""),
            "strength": parsed.get("strength", ""),
            "coating": parsed.get("coating", ""),
        }

        settings = load_match_settings()
        if analog_mode == "with":
            settings = _dc.replace(settings, use_standard_analogs_in_main_match=True, analogs_only=False)
        elif analog_mode == "only":
            settings = _dc.replace(settings, use_standard_analogs_in_main_match=False, analogs_only=True)
        else:
            settings = _dc.replace(settings, use_standard_analogs_in_main_match=False, analogs_only=False)
        result = decide_match(row_dict, settings, session=session)
        candidates = result.get("candidates", [])
        filter_log = result.get("filter_log") or {}
        candidates_other_size = result.get("candidates_other_size", [])

        analog_on = analog_mode in ("with", "only")
        from app.matcher import _build_analog_info  # noqa: PLC0415
        analog_info = _build_analog_info(row_dict, analog_on)
    finally:
        session.close()

    return templates.TemplateResponse("choose_catalog.html", {
        "request": request, "order": order, "client_line": cl,
        "candidates": candidates,
        "filter_log": filter_log,
        "candidates_other_size": candidates_other_size,
        "analog_mode": analog_mode,
        "analog_info": analog_info,
    })


@order_router.post("/orders/{order_id}/client-lines/{cl_id}/choose-catalog")
def choose_catalog_save(
    order_id: int, cl_id: int,
    item_id: int = Form(...),
):
    from app.order_models import ClientLine

    session = get_db_session()
    try:
        cl = session.get(ClientLine, cl_id)
        if cl and cl.order_id == order_id:
            prev_choice = cl.chosen_catalog_item_id
            prev_by = cl.chosen_by
            cl.chosen_catalog_item_id = item_id
            cl.chosen_by = "manual"
            cl.chosen_at = datetime.now(timezone.utc)
            cl.status = "ok"

            # Record feedback if user overrides a system (auto) choice
            if prev_by == "auto" and prev_choice and prev_choice != item_id:
                try:
                    from app.services.quality_service import record_feedback
                    from app.quality_models import PipelineRun
                    run = (
                        session.query(PipelineRun)
                        .filter_by(order_id=order_id)
                        .order_by(PipelineRun.id.desc())
                        .first()
                    )
                    record_feedback(
                        order_id=order_id,
                        client_line_id=cl_id,
                        system_choice_id=prev_choice,
                        user_choice_id=item_id,
                        pipeline_run_id=run.id if run else None,
                        session=session,
                    )
                    # Update run counters
                    if run:
                        run.system_match_incorrect += 1
                        run.manual_matches += 1
                except Exception:
                    pass  # don't break main flow

            session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/client-lines", status_code=303)


@order_router.post("/orders/{order_id}/client-lines/{cl_id}/clear")
def choose_catalog_clear(order_id: int, cl_id: int):
    from app.order_models import ClientLine

    session = get_db_session()
    try:
        cl = session.get(ClientLine, cl_id)
        if cl and cl.order_id == order_id:
            cl.chosen_catalog_item_id = None
            cl.chosen_by = None
            cl.chosen_at = None
            cl.status = "needs_manual"
            session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/client-lines", status_code=303)


# ── Approve ──────────────────────────────────────────────────────────────────


@order_router.post("/orders/{order_id}/approve")
def approve_catalog(order_id: int):
    from app.order_models import ClientLine, Order, OrderItem
    from app.models import InternalItem
    from app.matching.normalizer import normalize_size
    from app.matching.text_normalizer import normalize_for_minhash
    from app.matching.standard_analogs import normalize_standard

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)

        # Delete old order items if re-approving
        session.query(OrderItem).filter_by(order_id=order_id).delete()

        lines = session.query(ClientLine).filter_by(order_id=order_id).all()
        for cl in lines:
            if not cl.chosen_catalog_item_id:
                continue
            item = session.get(InternalItem, cl.chosen_catalog_item_id)
            if not item:
                continue

            # Build snapshot from catalog item
            size_norm = normalize_size(item.size) if item.size else (item.size_norm or "")
            std_norm = ""
            if item.standard_key:
                std_norm = item.standard_key
            elif item.standard_text:
                std_norm = normalize_standard(item.standard_text) or ""
            tokens_norm = normalize_for_minhash(item.name)

            oi = OrderItem(
                order_id=order_id,
                catalog_item_id=item.id,
                display_name_snapshot=item.name,
                type_norm=(item.item_type or "").lower(),
                size_norm=size_norm,
                std_norm=std_norm,
                tokens_norm=tokens_norm,
            )
            session.add(oi)

        order.status = "approved_catalog"
        session.commit()
    finally:
        session.close()

    return RedirectResponse(f"/orders/{order_id}", status_code=303)


# ── Quotes ───────────────────────────────────────────────────────────────────


@order_router.get("/orders/{order_id}/quotes", response_class=HTMLResponse)
def quotes_list(request: Request, order_id: int):
    from app.order_models import Order, Quote, QuoteLine, Supplier

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)
        quotes = session.query(Quote).filter_by(order_id=order_id).all()
        supplier_ids = [q.supplier_id for q in quotes]
        suppliers = session.query(Supplier).filter(
            Supplier.id.in_(supplier_ids)
        ).all() if supplier_ids else []
        supplier_map = {s.id: s for s in suppliers}

        quote_data = []
        for q in quotes:
            cnt = session.query(QuoteLine).filter_by(quote_id=q.id).count()
            quote_data.append({
                "quote": q,
                "supplier": supplier_map.get(q.supplier_id),
                "lines_count": cnt,
            })
    finally:
        session.close()

    ocr_available = False
    try:
        from app.integrations.google_document_ai import is_configured
        ocr_available = is_configured()
    except Exception:
        pass

    return templates.TemplateResponse("quote_upload.html", {
        "request": request, "order": order, "quote_data": quote_data,
        "ocr_available": ocr_available,
    })


@order_router.post("/orders/{order_id}/quotes/upload")
def quote_upload(
    request: Request,
    order_id: int,
    supplier_name: str = Form(...),
    file: UploadFile = File(None),
    source_kind: str = Form("excel"),
    text_input: str = Form(""),
):
    from app.order_models import Order, Quote, QuoteLine, Supplier
    from app.services.line_parser import parse_raw_line

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)

        # Get or create supplier
        supplier = session.query(Supplier).filter_by(name=supplier_name.strip()).first()
        if not supplier:
            supplier = Supplier(name=supplier_name.strip())
            session.add(supplier)
            session.flush()

        if source_kind == "text" and text_input.strip():
            # Direct text input — create quote + lines directly
            from app.text_input.parser import parse_text_line
            from app.services.quote_line_classifier import classify_quote_line

            quote = Quote(
                order_id=order_id, supplier_id=supplier.id,
                source_kind="text",
            )
            session.add(quote)
            session.flush()

            for i, line in enumerate(text_input.strip().splitlines()):
                line = line.strip()
                if not line:
                    continue
                line_class, filter_reason = classify_quote_line(line)
                tl = parse_text_line(line)
                parsed = parse_raw_line(tl["name"])
                session.add(QuoteLine(
                    quote_id=quote.id, row_no=i + 1, raw_text=line,
                    price=None, qty=tl["qty"],
                    parsed_json=json.dumps(parsed, ensure_ascii=False),
                    type_norm=parsed.get("item_type") or "",
                    size_norm=parsed.get("size_norm") or "",
                    std_norm=parsed.get("std_norm") or "",
                    tokens_norm=parsed.get("tokens_norm") or "",
                    unit=tl["unit"] or "",
                    line_class=line_class, filter_reason=filter_reason or None,
                ))

            if order.status == "approved_catalog":
                order.status = "quotes"
            session.commit()
            return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)

        if not file or not file.filename:
            return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)

        file_bytes = file.file.read()
        ext = (file.filename.rsplit(".", 1)[-1] if "." in file.filename else "").lower()

        if source_kind in ("pdf", "photo") or ext in ("pdf", "png", "jpg", "jpeg", "tiff", "bmp"):
            # OCR path — create lines directly
            from app.integrations.google_document_ai import process_document
            from app.services.google_ocr_extractor import extract_rows

            mime_map = {
                "pdf": "application/pdf",
                "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
                "tiff": "image/tiff", "bmp": "image/bmp",
            }
            mime = mime_map.get(ext, "application/pdf")
            doc = process_document(file_bytes, mime)
            result = extract_rows(doc)

            quote = Quote(
                order_id=order_id, supplier_id=supplier.id,
                source_filename=file.filename,
                source_kind=source_kind if source_kind in ("pdf", "photo") else "pdf",
            )
            session.add(quote)
            session.flush()

            from app.services.quote_line_classifier import classify_quote_line

            if result.structured_rows:
                # Product table — use structured fields
                for i, sr in enumerate(result.structured_rows):
                    name = sr.get("name", "").strip()
                    if not name:
                        continue
                    line_class, filter_reason = classify_quote_line(name)
                    parsed = parse_raw_line(name)
                    session.add(QuoteLine(
                        quote_id=quote.id, row_no=i + 1, raw_text=name,
                        price=sr.get("price_unit") or sr.get("price_total"),
                        qty=sr.get("qty"),
                        unit=sr.get("unit") or "",
                        parsed_json=json.dumps(parsed, ensure_ascii=False),
                        type_norm=parsed.get("item_type") or "",
                        size_norm=parsed.get("size_norm") or "",
                        std_norm=parsed.get("std_norm") or "",
                        tokens_norm=parsed.get("tokens_norm") or "",
                        line_class=line_class, filter_reason=filter_reason or None,
                    ))
            else:
                # Fallback — join cells into name
                for i, row_cells in enumerate(result.rows):
                    name = " ".join(c for c in row_cells if c.strip()) if isinstance(row_cells, list) else str(row_cells)
                    name = name.strip()
                    if not name:
                        continue
                    line_class, filter_reason = classify_quote_line(name)
                    parsed = parse_raw_line(name)
                    session.add(QuoteLine(
                        quote_id=quote.id, row_no=i + 1, raw_text=name,
                        parsed_json=json.dumps(parsed, ensure_ascii=False),
                        type_norm=parsed.get("item_type") or "",
                        size_norm=parsed.get("size_norm") or "",
                        std_norm=parsed.get("std_norm") or "",
                        tokens_norm=parsed.get("tokens_norm") or "",
                        line_class=line_class, filter_reason=filter_reason or None,
                    ))

            if order.status == "approved_catalog":
                order.status = "quotes"
            session.commit()
            return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)

        # Excel/CSV path → save raw table + column wizard
        from app.order_models import QuoteTable, QuoteTableRow
        from app.services.line_parser import read_tabular_file

        all_rows = read_tabular_file(file_bytes, file.filename)
        if not all_rows:
            return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)

        headers = all_rows[0]
        data_rows = all_rows[1:]

        quote = Quote(
            order_id=order_id, supplier_id=supplier.id,
            source_filename=file.filename,
            source_kind="excel",
        )
        session.add(quote)
        session.flush()
        quote_id = quote.id

        # Save raw table snapshot
        qt = QuoteTable(
            quote_id=quote_id,
            n_rows=len(all_rows),
            n_cols=len(headers),
            headers_json=json.dumps(headers, ensure_ascii=False),
            source="excel" if ext != "csv" else "csv",
        )
        session.add(qt)
        session.flush()

        for idx, row in enumerate(all_rows):
            session.add(QuoteTableRow(
                quote_table_id=qt.id,
                row_index=idx,
                cells_json=json.dumps(row, ensure_ascii=False),
            ))

        session.commit()
        qt_id = qt.id
    finally:
        session.close()

    return templates.TemplateResponse("quote_column_wizard.html", {
        "request": request,
        "order_id": order_id,
        "quote_id": quote_id,
        "supplier_name": supplier_name.strip(),
        "headers": headers,
        "all_rows": all_rows,
        "preview_rows": data_rows[:20],
        "quote_table_id": qt_id,
        "data_rows_json": json.dumps(data_rows, ensure_ascii=False),
    })


@order_router.post("/orders/{order_id}/quotes/{quote_id}/confirm")
def quote_wizard_confirm(
    request: Request, order_id: int, quote_id: int,
    col_name: int = Form(-1),
    col_qty: int = Form(-1),
    col_unit: int = Form(-1),
    col_price: int = Form(-1),
    col_sum: int = Form(-1),
    has_header: int = Form(0),
    data_rows_json: str = Form("[]"),
):
    from app.order_models import Order, QuoteLine
    from app.services.line_parser import parse_raw_line

    data_rows = json.loads(data_rows_json)

    session = get_db_session()
    try:
        from app.services.quote_line_classifier import classify_quote_line

        for i, row in enumerate(data_rows):
            name = row[col_name].strip() if 0 <= col_name < len(row) else ""
            if not name:
                continue

            # Raw cell values for debugging
            raw_qty_unit = ""
            raw_price = ""
            raw_sum = ""

            qty_val = None
            if 0 <= col_qty < len(row) and row[col_qty].strip():
                raw_qty_unit = row[col_qty].strip()
                try:
                    qty_val = float(raw_qty_unit.replace(",", ".").replace(" ", ""))
                except ValueError:
                    # Try extracting number from mixed text like "4 КГ"
                    import re
                    m = re.search(r"(\d+(?:[.,]\d+)?)", raw_qty_unit)
                    if m:
                        qty_val = float(m.group(1).replace(",", "."))

            unit_val = ""
            if 0 <= col_unit < len(row) and row[col_unit].strip():
                unit_text = row[col_unit].strip()
                if raw_qty_unit:
                    raw_qty_unit += " / " + unit_text
                else:
                    raw_qty_unit = unit_text
                unit_val = unit_text

            price_val = None
            if 0 <= col_price < len(row) and row[col_price].strip():
                raw_price = row[col_price].strip()
                try:
                    price_val = float(raw_price.replace(",", ".").replace(" ", ""))
                except ValueError:
                    pass

            sum_val = None
            if 0 <= col_sum < len(row) and row[col_sum].strip():
                raw_sum = row[col_sum].strip()
                try:
                    sum_val = float(raw_sum.replace(",", ".").replace(" ", ""))
                except ValueError:
                    pass

            line_class, filter_reason = classify_quote_line(name)
            parsed = parse_raw_line(name)
            session.add(QuoteLine(
                quote_id=quote_id, row_no=i + 1, raw_text=name,
                qty=qty_val, unit=unit_val or None,
                price=price_val, price_total=sum_val,
                parsed_json=json.dumps(parsed, ensure_ascii=False),
                type_norm=parsed.get("item_type") or "",
                size_norm=parsed.get("size_norm") or "",
                std_norm=parsed.get("std_norm") or "",
                tokens_norm=parsed.get("tokens_norm") or "",
                line_class=line_class, filter_reason=filter_reason or None,
                raw_cells_json=json.dumps(row, ensure_ascii=False),
                raw_qty_unit_text=raw_qty_unit or None,
                raw_price_text=raw_price or None,
                raw_sum_text=raw_sum or None,
            ))

        order = session.get(Order, order_id)
        if order and order.status == "approved_catalog":
            order.status = "quotes"
        session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)


@order_router.post("/orders/{order_id}/quotes/{quote_id}/match")
def quote_match_one(order_id: int, quote_id: int):
    from app.services.quote_order_matcher import match_quote_to_order_items

    session = get_db_session()
    try:
        stats = match_quote_to_order_items(quote_id, session)
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/comparison", status_code=303)


@order_router.post("/orders/{order_id}/match-all-quotes")
def quote_match_all(order_id: int):
    from app.services.quote_order_matcher import match_all_quotes_for_order

    session = get_db_session()
    try:
        match_all_quotes_for_order(order_id, session)
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/comparison", status_code=303)


@order_router.post("/orders/{order_id}/quotes/auto-match")
def quote_auto_match_json(order_id: int):
    """Auto-match all quotes for order, return JSON summary."""
    from fastapi.responses import JSONResponse
    from app.services.quote_order_matcher import match_all_quotes_for_order

    session = get_db_session()
    try:
        summary = match_all_quotes_for_order(order_id, session)
    finally:
        session.close()
    return JSONResponse(summary)


@order_router.post("/orders/{order_id}/quotes/{quote_id}/delete")
def quote_delete(order_id: int, quote_id: int):
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        session.execute(text("DELETE FROM quotes WHERE id = :qid AND order_id = :oid"),
                        {"qid": quote_id, "oid": order_id})
        session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/quotes", status_code=303)


# ── Comparison ───────────────────────────────────────────────────────────────


@order_router.get("/orders/{order_id}/comparison", response_class=HTMLResponse)
def order_comparison(request: Request, order_id: int):
    from app.order_models import Order
    from app.services.quote_order_matcher import build_comparison_table

    session = get_db_session()
    try:
        order = session.get(Order, order_id)
        if not order:
            return HTMLResponse("Order not found", status_code=404)
        table = build_comparison_table(order_id, session)
    finally:
        session.close()
    return templates.TemplateResponse("order_comparison.html", {
        "request": request, "order": order, "table": table,
    })


# ── Manual Quote Matching ────────────────────────────────────────────────────


@order_router.get("/orders/{order_id}/quotes/{quote_id}/lines/{ql_id}/match", response_class=HTMLResponse)
def manual_match_form(request: Request, order_id: int, quote_id: int, ql_id: int, use_analogs: str = "1"):
    from app.order_models import OrderItem, QuoteLine, QuoteMatch, Quote
    from app.services.line_parser import build_features, build_minhash
    from app.services.quote_order_matcher import (
        get_order_minhash_index, _types_compatible, _sizes_compatible, _standards_compatible,
        _score_exact_quote_match,
    )

    analog_on = use_analogs in ("1", "true")

    session = get_db_session()
    try:
        ql = session.get(QuoteLine, ql_id)
        if not ql:
            return HTMLResponse("QuoteLine not found", status_code=404)
        quote = session.get(Quote, quote_id)
        from app.order_models import Supplier
        supplier = session.get(Supplier, quote.supplier_id) if quote else None

        order_mhs, items_by_id = get_order_minhash_index(order_id, session)

        ql_feats = build_features(
            ql.tokens_norm or "", ql.type_norm or "",
            ql.size_norm or "", ql.std_norm or "",
        )
        ql_mh = build_minhash(ql_feats) if ql_feats else None
        ql_type = (ql.type_norm or "").strip().lower()
        ql_size = (ql.size_norm or "").strip().upper()
        ql_std = (ql.std_norm or "").strip()

        candidates = []
        for oi_id, oi in items_by_id.items():
            j = 0.0
            if ql_mh and oi_id in order_mhs:
                j = round(ql_mh.jaccard(order_mhs[oi_id]), 4)
            oi_type = (oi.type_norm or "").strip().lower()
            oi_size = (oi.size_norm or "").strip().upper()
            oi_std = (oi.std_norm or "").strip()

            type_ok = _types_compatible(ql_type, oi_type)
            size_ok = _sizes_compatible(ql_size, oi_size)
            std_ok = _standards_compatible(ql_std, oi_std, use_analogs=analog_on)

            # Exact field scoring: type+size match → high score (100-based)
            exact_score = None
            if ql_type and ql_size and oi_type == ql_type and oi_size == ql_size:
                exact_score = _score_exact_quote_match(ql_std, oi_std, use_analogs=analog_on)

            # Combined sort key: exact matches first (by exact_score), then MinHash
            sort_score = (exact_score / 100.0) if exact_score is not None else j

            candidates.append({
                "order_item": oi, "jaccard": j,
                "type_match": type_ok,
                "size_match": size_ok,
                "std_match": std_ok,
                "exact_score": exact_score,
                "sort_score": sort_score,
                "source": "exact" if exact_score is not None else "minhash",
            })
        # Exact matches first (sort_score >= 0.8 typically), then MinHash
        candidates.sort(key=lambda x: (-x["sort_score"], -x["jaccard"]))

        current_match = session.query(QuoteMatch).filter_by(quote_line_id=ql_id).first()
    finally:
        session.close()

    return templates.TemplateResponse("quote_manual_match.html", {
        "request": request,
        "order_id": order_id, "quote_id": quote_id,
        "quote_line": ql,
        "supplier_name": supplier.name if supplier else "",
        "candidates": candidates,
        "current_match": current_match,
        "use_analogs": analog_on,
    })


@order_router.post("/orders/{order_id}/quotes/{quote_id}/lines/{ql_id}/match")
def manual_match_save(
    request: Request, order_id: int, quote_id: int, ql_id: int,
    order_item_id: int = Form(...),
):
    from app.order_models import QuoteMatch

    session = get_db_session()
    try:
        existing = session.query(QuoteMatch).filter_by(quote_line_id=ql_id).first()
        if existing:
            existing.order_item_id = order_item_id
            existing.match_mode = "manual"
        else:
            session.add(QuoteMatch(
                order_item_id=order_item_id,
                quote_line_id=ql_id,
                match_mode="manual",
            ))
        session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/comparison", status_code=303)


@order_router.post("/orders/{order_id}/quotes/{quote_id}/lines/{ql_id}/unmatch")
def manual_unmatch(order_id: int, quote_id: int, ql_id: int):
    from app.order_models import QuoteMatch

    session = get_db_session()
    try:
        existing = session.query(QuoteMatch).filter_by(quote_line_id=ql_id).first()
        if existing:
            session.delete(existing)
            session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/orders/{order_id}/comparison", status_code=303)

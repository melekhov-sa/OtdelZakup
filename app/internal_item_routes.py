"""Web routes for internal catalog (Наша номенклатура) CRUD and per-row item selection."""

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import InternalItem, SupplierInternalMatch
from app.product_type_matcher import get_item_types_for_ui
from app.trace import load_traces, save_traces

internal_item_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── Internal catalog CRUD ─────────────────────────────────────────────────────


@internal_item_router.get("/internal-items", response_class=HTMLResponse)
async def internal_items_list(request: Request, q: str = ""):
    session = get_db_session()
    try:
        query = session.query(InternalItem).order_by(InternalItem.id)
        items = query.all()
        if q:
            q_lower = q.lower()
            items = [
                it for it in items
                if q_lower in (it.name or "").lower()
                or q_lower in (it.item_type or "").lower()
                or q_lower in (it.size or "").lower()
                or q_lower in (it.standard_text or "").lower()
            ]
        return templates.TemplateResponse(
            "internal_items_list.html",
            {"request": request, "items": items, "q": q},
        )
    finally:
        session.close()


@internal_item_router.get("/internal-items/new", response_class=HTMLResponse)
async def internal_item_new(request: Request):
    return templates.TemplateResponse(
        "internal_item_form.html",
        {"request": request, "item": None, "item_types": get_item_types_for_ui(), "is_edit": False},
    )


# ── Smart parse API endpoints (must be before {item_id} routes) ──────────────


@internal_item_router.post("/internal-items/parse")
async def internal_item_parse_api(name_full: str = Form(...)):
    """Parse a single name string and return extracted fields as JSON."""
    from app.item_parser import parse_internal_item_name
    result = parse_internal_item_name(name_full)
    return JSONResponse(result)


@internal_item_router.post("/internal-items/bulk-preview")
async def internal_item_bulk_preview_api(names_text: str = Form(...)):
    """Parse multiple names (one per line) and return preview as JSON."""
    from app.item_parser import bulk_parse
    names = names_text.splitlines()
    results = bulk_parse(names, skip_empty=True, dedup=False)
    return JSONResponse({"items": results, "total": len(results)})


@internal_item_router.post("/internal-items/bulk-import")
async def internal_item_bulk_import_api(names_text: str = Form(...)):
    """Create InternalItem records for each parsed name."""
    from app.item_parser import bulk_parse
    names = names_text.splitlines()
    results = bulk_parse(names, skip_empty=True, dedup=True)
    session = get_db_session()
    try:
        from app.standard_normalizer import standard_key_from_text
        created = 0
        for r in results:
            std_text = r["standard_text"] or None
            item = InternalItem(
                name=r["name_full"],
                name_full=r["name_full"],
                item_type=r["item_type"] or None,
                size=r["size"] or None,
                diameter=r["diameter"] or None,
                length=r["length"] or None,
                standard_text=std_text,
                standard_key=standard_key_from_text(std_text) if std_text else None,
                strength_class=r["strength_class"] or None,
                material_coating=r["material_coating"] or None,
                parse_status=r["parse_status"],
                parse_reason=r["parse_reason"] or None,
                is_active=True,
            )
            session.add(item)
            created += 1
        session.commit()
        return JSONResponse({"ok": True, "created": created})
    finally:
        session.close()


# ── CRUD with name_full + auto-parse ─────────────────────────────────────────


@internal_item_router.post("/internal-items/create", response_class=HTMLResponse)
async def internal_item_create(
    request: Request,
    name: str = Form(...),
    name_full: str = Form(default=""),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    parse_status = None
    parse_reason = None
    if name_full.strip():
        from app.item_parser import parse_internal_item_name
        p = parse_internal_item_name(name_full.strip())
        parse_status = p["parse_status"]
        parse_reason = p["parse_reason"] or None

    from app.standard_normalizer import standard_key_from_text
    std_text = standard_text or None
    session = get_db_session()
    try:
        item = InternalItem(
            name=name,
            name_full=name_full.strip() or None,
            item_type=item_type or None,
            size=size or None,
            diameter=diameter or None,
            length=length or None,
            standard_text=std_text,
            standard_key=standard_key_from_text(std_text) if std_text else None,
            strength_class=strength_class or None,
            material_coating=material_coating or None,
            parse_status=parse_status,
            parse_reason=parse_reason,
            is_active=True,
        )
        session.add(item)
        session.commit()
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.get("/internal-items/{item_id}/edit", response_class=HTMLResponse)
async def internal_item_edit(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return RedirectResponse(url="/internal-items", status_code=303)
        return templates.TemplateResponse(
            "internal_item_form.html",
            {"request": request, "item": item, "item_types": get_item_types_for_ui(), "is_edit": True},
        )
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/update", response_class=HTMLResponse)
async def internal_item_update(
    request: Request,
    item_id: int,
    name: str = Form(...),
    name_full: str = Form(default=""),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    parse_status = None
    parse_reason = None
    if name_full.strip():
        from app.item_parser import parse_internal_item_name
        p = parse_internal_item_name(name_full.strip())
        parse_status = p["parse_status"]
        parse_reason = p["parse_reason"] or None

    from app.standard_normalizer import standard_key_from_text
    std_text = standard_text or None
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return RedirectResponse(url="/internal-items", status_code=303)
        item.name = name
        item.name_full = name_full.strip() or None
        item.item_type = item_type or None
        item.size = size or None
        item.diameter = diameter or None
        item.length = length or None
        item.standard_text = std_text
        item.standard_key = standard_key_from_text(std_text) if std_text else None
        item.strength_class = strength_class or None
        item.material_coating = material_coating or None
        item.parse_status = parse_status
        item.parse_reason = parse_reason
        session.commit()
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/toggle", response_class=HTMLResponse)
async def internal_item_toggle(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is not None:
            item.is_active = not item.is_active
            session.commit()
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/delete", response_class=HTMLResponse)
async def internal_item_delete(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is not None:
            session.delete(item)
            session.commit()
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


# ── Confirm suggested match ───────────────────────────────────────────────────


@internal_item_router.post("/files/{file_id}/rows/{row_number}/confirm-match")
async def confirm_match(
    file_id: str,
    row_number: int,
    remember: bool = Form(default=True),
):
    """Confirm a SUGGESTED match for a row (mode → CONFIRMED).

    If remember=True, saves the fingerprint→item mapping to SupplierInternalMatch.
    Returns JSON {ok: true, name: str, mode: str}.
    """
    traces = load_traces(file_id)
    if not traces or row_number < 1 or row_number > len(traces):
        return JSONResponse({"ok": False, "error": "Строка не найдена"}, status_code=404)

    trace = traces[row_number - 1]
    matching = trace.get("matching", {})
    item_id = matching.get("internal_item_id")

    if not item_id:
        return JSONResponse({"ok": False, "error": "Нет предложенной позиции"}, status_code=400)

    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return JSONResponse({"ok": False, "error": "Позиция не найдена"}, status_code=404)

        # Update trace
        matching["mode"] = "CONFIRMED"
        matching["selected_name"] = item.name
        matching["selected_item_id"] = item_id
        trace["matching"] = matching
        save_traces(file_id, traces)

        # Save to memory if requested
        if remember:
            fp = matching.get("fingerprint", "")
            if fp:
                existing = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
                if existing:
                    existing.internal_item_id = item_id
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(SupplierInternalMatch(fingerprint=fp, internal_item_id=item_id))
                session.commit()

        return JSONResponse({"ok": True, "name": item.name, "mode": "CONFIRMED"})
    finally:
        session.close()


# ── Per-row item selection ────────────────────────────────────────────────────


@internal_item_router.get("/files/{file_id}/rows/{row_number}/select-internal", response_class=HTMLResponse)
async def select_internal_get(request: Request, file_id: str, row_number: int):
    """Show form for selecting internal catalog item for a specific result row."""
    traces = load_traces(file_id)
    if traces is None or row_number < 1 or row_number > len(traces):
        return RedirectResponse(url="/", status_code=303)

    trace = traces[row_number - 1]
    matching = trace.get("matching", {})
    candidates = matching.get("candidates", [])

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).order_by(InternalItem.name).all()
        return templates.TemplateResponse(
            "select_internal.html",
            {
                "request": request,
                "file_id": file_id,
                "row_number": row_number,
                "trace": trace,
                "candidates": candidates,
                "all_items": all_items,
                "current_match": matching.get("selected_name", "") or matching.get("candidates", [{}])[0].get("name", "") if matching.get("source") != "none" else "",
            },
        )
    finally:
        session.close()


@internal_item_router.post("/files/{file_id}/rows/{row_number}/select-internal-item")
async def select_internal_item_post(
    request: Request,
    file_id: str,
    row_number: int,
    internal_item_id: int = Form(...),
    remember: bool = Form(default=False),
):
    """Save manual internal item selection for a result row.

    If remember=True, also persists the fingerprint→item mapping in SupplierInternalMatch.
    Returns JSON {ok: true, name: str}.
    """
    session = get_db_session()
    try:
        item = session.get(InternalItem, internal_item_id)
        if item is None:
            return JSONResponse({"ok": False, "error": "Позиция не найдена"}, status_code=404)

        # Update trace
        traces = load_traces(file_id)
        if traces and 1 <= row_number <= len(traces):
            trace = traces[row_number - 1]
            if "matching" not in trace:
                trace["matching"] = {}
            trace["matching"]["selected_name"] = item.name
            trace["matching"]["selected_item_id"] = internal_item_id
            trace["matching"]["source"] = "manual"
            save_traces(file_id, traces)

        # Save fingerprint to memory if requested
        if remember:
            matching = (traces[row_number - 1].get("matching", {}) if traces else {})
            fp = matching.get("fingerprint", "")
            if fp:
                existing = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
                if existing:
                    existing.internal_item_id = internal_item_id
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(SupplierInternalMatch(
                        fingerprint=fp,
                        internal_item_id=internal_item_id,
                    ))
                session.commit()

        return JSONResponse({"ok": True, "name": item.name, "item_id": internal_item_id})
    finally:
        session.close()

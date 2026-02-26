"""Web routes for internal catalog (Наша номенклатура) CRUD and per-row item selection."""

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import InternalItem, SupplierInternalMatch
from app.trace import load_traces, save_traces

internal_item_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

ITEM_TYPES = ["болт", "винт", "гайка", "шайба", "шпилька", "саморез", "шуруп", "анкер"]


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
        {"request": request, "item": None, "item_types": ITEM_TYPES, "is_edit": False},
    )


@internal_item_router.post("/internal-items/create", response_class=HTMLResponse)
async def internal_item_create(
    request: Request,
    name: str = Form(...),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    session = get_db_session()
    try:
        item = InternalItem(
            name=name,
            item_type=item_type or None,
            size=size or None,
            diameter=diameter or None,
            length=length or None,
            standard_text=standard_text or None,
            strength_class=strength_class or None,
            material_coating=material_coating or None,
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
            {"request": request, "item": item, "item_types": ITEM_TYPES, "is_edit": True},
        )
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/update", response_class=HTMLResponse)
async def internal_item_update(
    request: Request,
    item_id: int,
    name: str = Form(...),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return RedirectResponse(url="/internal-items", status_code=303)
        item.name = name
        item.item_type = item_type or None
        item.size = size or None
        item.diameter = diameter or None
        item.length = length or None
        item.standard_text = standard_text or None
        item.strength_class = strength_class or None
        item.material_coating = material_coating or None
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

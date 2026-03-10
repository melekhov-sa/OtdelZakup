"""Web routes for standard reference CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import StandardRef
from app.product_type_matcher import get_item_types_for_ui

standard_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

STANDARD_KINDS = ["GOST", "ISO", "DIN"]


@standard_router.get("/standards", response_class=HTMLResponse)
def standards_list(request: Request, q: str = "", kind: str = "", active_only: str = ""):
    session = get_db_session()
    try:
        query = session.query(StandardRef)
        if kind:
            query = query.filter(StandardRef.standard_kind == kind)
        if active_only:
            query = query.filter(StandardRef.is_active.is_(True))
        standards = query.order_by(StandardRef.standard_kind, StandardRef.standard_code).all()
        if q:
            q_lower = q.lower()
            standards = [
                s for s in standards
                if q_lower in s.standard_code.lower()
                or q_lower in (s.title or "").lower()
                or q_lower in (s.item_type or "").lower()
            ]
        return templates.TemplateResponse(
            "standards_list.html",
            {
                "request": request,
                "standards": standards,
                "q": q,
                "kind_filter": kind,
                "active_only": active_only,
                "standard_kinds": STANDARD_KINDS,
            },
        )
    finally:
        session.close()


@standard_router.get("/standards/new", response_class=HTMLResponse)
def standard_new(request: Request):
    return templates.TemplateResponse(
        "standard_form.html",
        {
            "request": request,
            "standard": None,
            "standard_kinds": STANDARD_KINDS,
            "item_types": get_item_types_for_ui(),
            "is_edit": False,
        },
    )


@standard_router.post("/standards/create", response_class=HTMLResponse)
def standard_create(
    request: Request,
    standard_kind: str = Form(...),
    standard_code: str = Form(...),
    title: str = Form(default=""),
    item_type: str = Form(default=""),
    notes: str = Form(default=""),
    is_active: str = Form(default=""),
):
    kind = standard_kind.upper().strip()
    code = standard_code.strip()
    std_key = f"{kind}-{code}" if kind and code else None

    session = get_db_session()
    try:
        ref = StandardRef(
            standard_kind=kind,
            standard_code=code,
            standard_key=std_key,
            title=title.strip() if title.strip() else None,
            item_type=item_type.strip().lower() if item_type.strip() else None,
            notes=notes.strip() if notes.strip() else None,
            is_active=bool(is_active),
        )
        session.add(ref)
        session.commit()
        return RedirectResponse(url="/standards", status_code=303)
    except Exception:
        session.rollback()
        return RedirectResponse(url="/standards", status_code=303)
    finally:
        session.close()


@standard_router.get("/standards/{std_id}/edit", response_class=HTMLResponse)
def standard_edit(request: Request, std_id: int):
    session = get_db_session()
    try:
        ref = session.get(StandardRef, std_id)
        if ref is None:
            return RedirectResponse(url="/standards", status_code=303)
        return templates.TemplateResponse(
            "standard_form.html",
            {
                "request": request,
                "standard": ref,
                "standard_kinds": STANDARD_KINDS,
                "item_types": get_item_types_for_ui(),
                "is_edit": True,
            },
        )
    finally:
        session.close()


@standard_router.post("/standards/{std_id}/update", response_class=HTMLResponse)
def standard_update(
    request: Request,
    std_id: int,
    standard_kind: str = Form(...),
    standard_code: str = Form(...),
    title: str = Form(default=""),
    item_type: str = Form(default=""),
    notes: str = Form(default=""),
    is_active: str = Form(default=""),
):
    kind = standard_kind.upper().strip()
    code = standard_code.strip()
    std_key = f"{kind}-{code}" if kind and code else None

    session = get_db_session()
    try:
        ref = session.get(StandardRef, std_id)
        if ref is None:
            return RedirectResponse(url="/standards", status_code=303)
        ref.standard_kind = kind
        ref.standard_code = code
        ref.standard_key = std_key
        ref.title = title.strip() if title.strip() else None
        ref.item_type = item_type.strip().lower() if item_type.strip() else None
        ref.notes = notes.strip() if notes.strip() else None
        ref.is_active = bool(is_active)
        session.commit()
        return RedirectResponse(url="/standards", status_code=303)
    except Exception:
        session.rollback()
        return RedirectResponse(url="/standards", status_code=303)
    finally:
        session.close()


@standard_router.post("/standards/{std_id}/toggle", response_class=HTMLResponse)
def standard_toggle(request: Request, std_id: int):
    session = get_db_session()
    try:
        ref = session.get(StandardRef, std_id)
        if ref is not None:
            ref.is_active = not ref.is_active
            session.commit()
        return RedirectResponse(url="/standards", status_code=303)
    finally:
        session.close()

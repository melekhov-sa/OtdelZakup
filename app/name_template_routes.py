"""Web routes for NameTemplate CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import NameTemplate
from app.name_builder import TEMPLATE_VAR_HINTS, TEMPLATE_VARS

name_template_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@name_template_router.get("/name-templates", response_class=HTMLResponse)
async def name_templates_list(request: Request):
    session = get_db_session()
    try:
        tmpl_list = (
            session.query(NameTemplate)
            .order_by(NameTemplate.priority.asc(), NameTemplate.id)
            .all()
        )
        return templates.TemplateResponse(
            "name_templates_list.html",
            {
                "request": request,
                "templates_list": tmpl_list,
                "template_vars": TEMPLATE_VARS,
                "template_var_hints": TEMPLATE_VAR_HINTS,
            },
        )
    finally:
        session.close()


@name_template_router.get("/name-templates/new", response_class=HTMLResponse)
async def name_template_new(request: Request):
    return templates.TemplateResponse(
        "name_template_form.html",
        {
            "request": request,
            "tmpl": None,
            "template_vars": TEMPLATE_VARS,
            "template_var_hints": TEMPLATE_VAR_HINTS,
            "is_edit": False,
        },
    )


@name_template_router.post("/name-templates/create", response_class=HTMLResponse)
async def name_template_create(
    request: Request,
    name: str = Form(...),
    template_string: str = Form(...),
    priority: int = Form(default=1),
    is_active: str = Form(default=""),
):
    session = get_db_session()
    try:
        tmpl = NameTemplate(
            name=name,
            template_string=template_string.strip(),
            priority=priority,
            is_active=bool(is_active),
        )
        session.add(tmpl)
        session.commit()
        return RedirectResponse(url="/name-templates", status_code=303)
    finally:
        session.close()


@name_template_router.get("/name-templates/{tmpl_id}/edit", response_class=HTMLResponse)
async def name_template_edit(request: Request, tmpl_id: int):
    session = get_db_session()
    try:
        tmpl = session.get(NameTemplate, tmpl_id)
        if tmpl is None:
            return RedirectResponse(url="/name-templates", status_code=303)
        return templates.TemplateResponse(
            "name_template_form.html",
            {
                "request": request,
                "tmpl": tmpl,
                "template_vars": TEMPLATE_VARS,
                "template_var_hints": TEMPLATE_VAR_HINTS,
                "is_edit": True,
            },
        )
    finally:
        session.close()


@name_template_router.post("/name-templates/{tmpl_id}/update", response_class=HTMLResponse)
async def name_template_update(
    request: Request,
    tmpl_id: int,
    name: str = Form(...),
    template_string: str = Form(...),
    priority: int = Form(default=1),
    is_active: str = Form(default=""),
):
    session = get_db_session()
    try:
        tmpl = session.get(NameTemplate, tmpl_id)
        if tmpl is None:
            return RedirectResponse(url="/name-templates", status_code=303)
        tmpl.name = name
        tmpl.template_string = template_string.strip()
        tmpl.priority = priority
        tmpl.is_active = bool(is_active)
        session.commit()
        return RedirectResponse(url="/name-templates", status_code=303)
    finally:
        session.close()


@name_template_router.post("/name-templates/{tmpl_id}/activate", response_class=HTMLResponse)
async def name_template_activate(request: Request, tmpl_id: int):
    """Set one template as active, deactivate all others."""
    session = get_db_session()
    try:
        all_tmpls = session.query(NameTemplate).all()
        for t in all_tmpls:
            t.is_active = t.id == tmpl_id
        session.commit()
        return RedirectResponse(url="/name-templates", status_code=303)
    finally:
        session.close()


@name_template_router.post("/name-templates/{tmpl_id}/toggle", response_class=HTMLResponse)
async def name_template_toggle(request: Request, tmpl_id: int):
    session = get_db_session()
    try:
        tmpl = session.get(NameTemplate, tmpl_id)
        if tmpl is not None:
            tmpl.is_active = not tmpl.is_active
            session.commit()
        return RedirectResponse(url="/name-templates", status_code=303)
    finally:
        session.close()

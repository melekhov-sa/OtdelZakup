"""Web routes for readiness rule CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import ReadinessRule
from app.product_type_matcher import get_item_types_for_ui

readiness_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

AVAILABLE_FIELDS = [
    ("size", "Размер"),
    ("qty", "Количество"),
    ("uom", "Ед."),
    ("name", "Наименование"),
    ("code", "Код"),
    ("item_type", "Тип изделия"),
    ("strength", "Класс прочности"),
    ("coating", "Покрытие"),
    ("gost", "ГОСТ"),
    ("iso", "ISO"),
    ("din", "DIN"),
]


@readiness_router.get("/readiness", response_class=HTMLResponse)
def readiness_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(ReadinessRule)
            .order_by(ReadinessRule.priority.asc(), ReadinessRule.id)
            .all()
        )
        return templates.TemplateResponse(
            "readiness_list.html",
            {
                "request": request,
                "rules": rules,
                "available_fields": dict(AVAILABLE_FIELDS),
            },
        )
    finally:
        session.close()


@readiness_router.get("/readiness/new", response_class=HTMLResponse)
def readiness_new(request: Request):
    return templates.TemplateResponse(
        "readiness_form.html",
        {
            "request": request,
            "rule": None,
            "item_types": get_item_types_for_ui(),
            "available_fields": AVAILABLE_FIELDS,
            "is_edit": False,
        },
    )


@readiness_router.post("/readiness/create", response_class=HTMLResponse)
def readiness_create(
    request: Request,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = ReadinessRule(
            name=name,
            description=description,
            item_type=item_type if item_type else None,
            priority=priority,
            is_active=True,
        )
        rule.require_fields_list = require_fields
        session.add(rule)
        session.commit()
        return RedirectResponse(url="/readiness", status_code=303)
    finally:
        session.close()


@readiness_router.get("/readiness/{rule_id}/edit", response_class=HTMLResponse)
def readiness_edit(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(ReadinessRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/readiness", status_code=303)
        return templates.TemplateResponse(
            "readiness_form.html",
            {
                "request": request,
                "rule": rule,
                "item_types": get_item_types_for_ui(),
                "available_fields": AVAILABLE_FIELDS,
                "is_edit": True,
            },
        )
    finally:
        session.close()


@readiness_router.post("/readiness/{rule_id}/update", response_class=HTMLResponse)
def readiness_update(
    request: Request,
    rule_id: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = session.get(ReadinessRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/readiness", status_code=303)
        rule.name = name
        rule.description = description
        rule.item_type = item_type if item_type else None
        rule.require_fields_list = require_fields
        rule.priority = priority
        session.commit()
        return RedirectResponse(url="/readiness", status_code=303)
    finally:
        session.close()


@readiness_router.post("/readiness/{rule_id}/toggle", response_class=HTMLResponse)
def readiness_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(ReadinessRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
        return RedirectResponse(url="/readiness", status_code=303)
    finally:
        session.close()

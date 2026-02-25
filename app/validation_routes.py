"""Web routes for validation rule CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import ValidationRule

rules_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

ITEM_TYPES = ["болт", "винт", "гайка", "шайба", "шпилька", "саморез", "шуруп", "анкер"]

AVAILABLE_FIELDS = [
    ("size", "Размер"),
    ("qty", "Количество"),
    ("uom", "Ед."),
    ("item_type", "Тип изделия"),
    ("length", "Длина"),
    ("strength", "Класс прочности"),
    ("gost", "ГОСТ"),
    ("iso", "ISO"),
    ("din", "DIN"),
    ("coating", "Покрытие"),
    ("name", "Наименование"),
    ("code", "Код"),
]

FORCE_STATUS_OPTIONS = [
    ("", "—"),
    ("review", "Требуется просмотреть"),
    ("manual", "Требуется вручную разобрать"),
]


@rules_router.get("/rules", response_class=HTMLResponse)
async def rules_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(ValidationRule)
            .order_by(ValidationRule.priority.asc(), ValidationRule.id)
            .all()
        )
        return templates.TemplateResponse(
            "rules_list.html",
            {
                "request": request,
                "rules": rules,
                "available_fields": dict(AVAILABLE_FIELDS),
            },
        )
    finally:
        session.close()


@rules_router.get("/rules/new", response_class=HTMLResponse)
async def rule_new(request: Request):
    return templates.TemplateResponse(
        "rules_form.html",
        {
            "request": request,
            "rule": None,
            "item_types": ITEM_TYPES,
            "available_fields": AVAILABLE_FIELDS,
            "force_status_options": FORCE_STATUS_OPTIONS,
            "is_edit": False,
        },
    )


@rules_router.post("/rules/create", response_class=HTMLResponse)
async def rule_create(
    request: Request,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: list[str] = Form(default=[]),
    forbid_fields: list[str] = Form(default=[]),
    force_status: str = Form(default=""),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = ValidationRule(
            name=name,
            description=description,
            item_type=item_type if item_type else None,
            force_status=force_status if force_status else None,
            priority=priority,
            is_active=True,
        )
        rule.require_fields_list = require_fields
        rule.forbid_fields_list = forbid_fields
        session.add(rule)
        session.commit()
        return RedirectResponse(url="/rules", status_code=303)
    finally:
        session.close()


@rules_router.get("/rules/{rule_id}/edit", response_class=HTMLResponse)
async def rule_edit(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(ValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/rules", status_code=303)
        return templates.TemplateResponse(
            "rules_form.html",
            {
                "request": request,
                "rule": rule,
                "item_types": ITEM_TYPES,
                "available_fields": AVAILABLE_FIELDS,
                "force_status_options": FORCE_STATUS_OPTIONS,
                "is_edit": True,
            },
        )
    finally:
        session.close()


@rules_router.post("/rules/{rule_id}/update", response_class=HTMLResponse)
async def rule_update(
    request: Request,
    rule_id: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: list[str] = Form(default=[]),
    forbid_fields: list[str] = Form(default=[]),
    force_status: str = Form(default=""),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = session.get(ValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/rules", status_code=303)
        rule.name = name
        rule.description = description
        rule.item_type = item_type if item_type else None
        rule.require_fields_list = require_fields
        rule.forbid_fields_list = forbid_fields
        rule.force_status = force_status if force_status else None
        rule.priority = priority
        session.commit()
        return RedirectResponse(url="/rules", status_code=303)
    finally:
        session.close()


@rules_router.post("/rules/{rule_id}/toggle", response_class=HTMLResponse)
async def rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(ValidationRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
        return RedirectResponse(url="/rules", status_code=303)
    finally:
        session.close()

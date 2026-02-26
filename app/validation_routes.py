"""Web routes for validation rule CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import ValidationRule
from app.product_type_matcher import get_item_types_for_ui

rules_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

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

CONDITION_TYPE_OPTIONS = [
    ("FIELDS_REQUIRED", "Обязательные поля заполнены"),
    ("FIELDS_FORBIDDEN", "Запрещённые поля пустые"),
    ("STANDARD_MATCH", "Тип изделия соответствует стандарту"),
]

STANDARD_SOURCE_OPTIONS = [
    ("ANY", "Любой (первый найденный DIN/ISO/ГОСТ)"),
    ("DIN", "DIN"),
    ("ISO", "ISO"),
    ("GOST", "ГОСТ"),
]

EXPECTED_ITEM_TYPE_MODE_OPTIONS = [
    ("FROM_DIRECTORY", "Брать из справочника стандартов"),
    ("FIXED", "Задать вручную"),
]

_TEMPLATE_CONTEXT_EXTRAS = dict(
    condition_type_options=CONDITION_TYPE_OPTIONS,
    standard_source_options=STANDARD_SOURCE_OPTIONS,
    expected_item_type_mode_options=EXPECTED_ITEM_TYPE_MODE_OPTIONS,
    item_types=get_item_types_for_ui(),
    available_fields=AVAILABLE_FIELDS,
    force_status_options=FORCE_STATUS_OPTIONS,
)


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
        {"request": request, "rule": None, "is_edit": False, **_TEMPLATE_CONTEXT_EXTRAS},
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
    condition_type: str = Form(default="FIELDS_REQUIRED"),
    standard_source: str = Form(default="ANY"),
    expected_item_type_mode: str = Form(default="FROM_DIRECTORY"),
    expected_item_type: str = Form(default=""),
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
            condition_type=condition_type,
            standard_source=standard_source,
            expected_item_type_mode=expected_item_type_mode,
            expected_item_type=expected_item_type if expected_item_type else None,
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
            {"request": request, "rule": rule, "is_edit": True, **_TEMPLATE_CONTEXT_EXTRAS},
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
    condition_type: str = Form(default="FIELDS_REQUIRED"),
    standard_source: str = Form(default="ANY"),
    expected_item_type_mode: str = Form(default="FROM_DIRECTORY"),
    expected_item_type: str = Form(default=""),
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
        rule.condition_type = condition_type
        rule.standard_source = standard_source
        rule.expected_item_type_mode = expected_item_type_mode
        rule.expected_item_type = expected_item_type if expected_item_type else None
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

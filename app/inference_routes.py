"""Web routes for InferenceRule CRUD (field computation rules)."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import InferenceRule
from app.product_type_matcher import get_item_types_for_ui

inference_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
MODES = [
    ("DIAMETER_AS_SIZE",          "Размер = Диаметр (для гайки, шайбы)"),
    ("DIAMETER_X_LENGTH_AS_SIZE", "Размер = Диаметр × Длина (для болта, винта, анкера)"),
]


@inference_router.get("/inference-rules", response_class=HTMLResponse)
async def inference_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(InferenceRule)
            .order_by(InferenceRule.priority.asc(), InferenceRule.id)
            .all()
        )
        return templates.TemplateResponse(
            "inference_list.html",
            {"request": request, "rules": rules, "modes": dict(MODES)},
        )
    finally:
        session.close()


@inference_router.get("/inference-rules/new", response_class=HTMLResponse)
async def inference_new(request: Request):
    return templates.TemplateResponse(
        "inference_form.html",
        {
            "request": request,
            "rule": None,
            "item_types": get_item_types_for_ui(),
            "modes": MODES,
            "is_edit": False,
        },
    )


@inference_router.post("/inference-rules/create", response_class=HTMLResponse)
async def inference_create(
    request: Request,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = InferenceRule(
            name=name,
            mode=mode,
            priority=priority,
            is_active=True,
            target_field="size",
        )
        rule.item_types_list = item_types if item_types else []
        session.add(rule)
        session.commit()
        return RedirectResponse(url="/inference-rules", status_code=303)
    finally:
        session.close()


@inference_router.get("/inference-rules/{rule_id}/edit", response_class=HTMLResponse)
async def inference_edit(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(InferenceRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/inference-rules", status_code=303)
        return templates.TemplateResponse(
            "inference_form.html",
            {
                "request": request,
                "rule": rule,
                "item_types": get_item_types_for_ui(),
                "modes": MODES,
                "is_edit": True,
            },
        )
    finally:
        session.close()


@inference_router.post("/inference-rules/{rule_id}/update", response_class=HTMLResponse)
async def inference_update(
    request: Request,
    rule_id: int,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = session.get(InferenceRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/inference-rules", status_code=303)
        rule.name = name
        rule.mode = mode
        rule.item_types_list = item_types if item_types else []
        rule.priority = priority
        session.commit()
        return RedirectResponse(url="/inference-rules", status_code=303)
    finally:
        session.close()


@inference_router.post("/inference-rules/{rule_id}/toggle", response_class=HTMLResponse)
async def inference_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(InferenceRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
        return RedirectResponse(url="/inference-rules", status_code=303)
    finally:
        session.close()


@inference_router.post("/inference-rules/{rule_id}/delete", response_class=HTMLResponse)
async def inference_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(InferenceRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
        return RedirectResponse(url="/inference-rules", status_code=303)
    finally:
        session.close()

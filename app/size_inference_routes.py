"""Web routes for SizeInferenceRule CRUD."""

from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import SizeInferenceRule

size_inference_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

ITEM_TYPES = ["болт", "винт", "гайка", "шайба", "шпилька", "саморез", "шуруп", "анкер"]
MODES = [
    ("DIAMETER_AS_SIZE", "Размер = Диаметр (для гайки, шайбы)"),
    ("DIAMETER_X_LENGTH", "Размер = Диаметр × Длина (для болта, винта)"),
]


@size_inference_router.get("/size-inference", response_class=HTMLResponse)
async def size_inference_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(SizeInferenceRule)
            .order_by(SizeInferenceRule.priority.asc(), SizeInferenceRule.id)
            .all()
        )
        return templates.TemplateResponse(
            "size_inference_list.html",
            {"request": request, "rules": rules, "modes": dict(MODES)},
        )
    finally:
        session.close()


@size_inference_router.get("/size-inference/new", response_class=HTMLResponse)
async def size_inference_new(request: Request):
    return templates.TemplateResponse(
        "size_inference_form.html",
        {
            "request": request,
            "rule": None,
            "item_types": ITEM_TYPES,
            "modes": MODES,
            "is_edit": False,
        },
    )


@size_inference_router.post("/size-inference/create", response_class=HTMLResponse)
async def size_inference_create(
    request: Request,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = SizeInferenceRule(
            name=name,
            mode=mode,
            priority=priority,
            is_active=True,
        )
        rule.item_types_list = item_types if item_types else []
        session.add(rule)
        session.commit()
        return RedirectResponse(url="/size-inference", status_code=303)
    finally:
        session.close()


@size_inference_router.get("/size-inference/{rule_id}/edit", response_class=HTMLResponse)
async def size_inference_edit(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeInferenceRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/size-inference", status_code=303)
        return templates.TemplateResponse(
            "size_inference_form.html",
            {
                "request": request,
                "rule": rule,
                "item_types": ITEM_TYPES,
                "modes": MODES,
                "is_edit": True,
            },
        )
    finally:
        session.close()


@size_inference_router.post("/size-inference/{rule_id}/update", response_class=HTMLResponse)
async def size_inference_update(
    request: Request,
    rule_id: int,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: list[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    session = get_db_session()
    try:
        rule = session.get(SizeInferenceRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/size-inference", status_code=303)
        rule.name = name
        rule.mode = mode
        rule.item_types_list = item_types if item_types else []
        rule.priority = priority
        session.commit()
        return RedirectResponse(url="/size-inference", status_code=303)
    finally:
        session.close()


@size_inference_router.post("/size-inference/{rule_id}/toggle", response_class=HTMLResponse)
async def size_inference_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeInferenceRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
        return RedirectResponse(url="/size-inference", status_code=303)
    finally:
        session.close()


@size_inference_router.post("/size-inference/{rule_id}/delete", response_class=HTMLResponse)
async def size_inference_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeInferenceRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
        return RedirectResponse(url="/size-inference", status_code=303)
    finally:
        session.close()

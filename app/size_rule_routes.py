"""CRUD routes for SizeRule management."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import SizeRule

size_rule_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

MATCH_TYPE_OPTIONS = [
    ("regex", "Регулярное выражение (regex)"),
    ("contains", "Содержит (contains)"),
    ("exact", "Точное совпадение (exact)"),
]

SIZE_KIND_OPTIONS = [
    ("diameter", "Диаметр"),
    ("diameter_length", "Диаметр x Длина"),
    ("triple_size", "Тройной размер"),
    ("profile_size", "Профиль/уголок"),
    ("thread", "Резьба с допуском"),
    ("custom", "Произвольный"),
]


@size_rule_router.get("/size-rules", response_class=HTMLResponse)
def size_rules_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(SizeRule)
            .order_by(SizeRule.priority.desc(), SizeRule.id)
            .all()
        )
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse("size_rules.html", {
            "request": request,
            "rules": rules,
            "saved": saved,
        })
    finally:
        session.close()


@size_rule_router.get("/size-rules/new", response_class=HTMLResponse)
def size_rule_new_form(request: Request):
    return templates.TemplateResponse("size_rule_form.html", {
        "request": request,
        "rule": None,
        "is_edit": False,
        "match_type_options": MATCH_TYPE_OPTIONS,
        "size_kind_options": SIZE_KIND_OPTIONS,
    })


@size_rule_router.post("/size-rules/new", response_class=HTMLResponse)
def size_rule_create(
    request: Request,
    pattern_raw: str = Form(...),
    match_type: str = Form("regex"),
    size_kind: str = Form(...),
    normalize_template: str = Form(default=""),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    from datetime import datetime, timezone
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = SizeRule(
            pattern_raw=pattern_raw.strip(),
            match_type=match_type,
            size_kind=size_kind,
            normalize_template=normalize_template.strip() or None,
            priority=priority,
            is_active=bool(is_active),
            note=note.strip() or None,
            created_at=now,
            updated_at=now,
        )
        session.add(rule)
        session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/size-rules?saved=1", status_code=303)


@size_rule_router.get("/size-rules/{rule_id}/edit", response_class=HTMLResponse)
def size_rule_edit_form(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/size-rules", status_code=303)
        return templates.TemplateResponse("size_rule_form.html", {
            "request": request,
            "rule": rule,
            "is_edit": True,
            "match_type_options": MATCH_TYPE_OPTIONS,
            "size_kind_options": SIZE_KIND_OPTIONS,
        })
    finally:
        session.close()


@size_rule_router.post("/size-rules/{rule_id}/edit", response_class=HTMLResponse)
def size_rule_update(
    request: Request,
    rule_id: int,
    pattern_raw: str = Form(...),
    match_type: str = Form("regex"),
    size_kind: str = Form(...),
    normalize_template: str = Form(default=""),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    session = get_db_session()
    try:
        rule = session.get(SizeRule, rule_id)
        if rule is not None:
            rule.pattern_raw = pattern_raw.strip()
            rule.match_type = match_type
            rule.size_kind = size_kind
            rule.normalize_template = normalize_template.strip() or None
            rule.priority = priority
            rule.is_active = bool(is_active)
            rule.note = note.strip() or None
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/size-rules?saved=1", status_code=303)


@size_rule_router.post("/size-rules/{rule_id}/toggle", response_class=HTMLResponse)
def size_rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/size-rules", status_code=303)


@size_rule_router.post("/size-rules/{rule_id}/delete", response_class=HTMLResponse)
def size_rule_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(SizeRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/size-rules", status_code=303)

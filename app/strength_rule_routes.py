"""CRUD routes for StrengthRule management."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import StrengthRule

strength_rule_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

MATCH_TYPE_OPTIONS = [
    ("contains", "Содержит (contains)"),
    ("exact", "Точное совпадение (exact)"),
    ("regex", "Регулярное выражение (regex)"),
]

FAMILY_OPTIONS = [
    ("metric", "Метрический (metric)"),
    ("stainless", "Нержавейка (stainless)"),
]


@strength_rule_router.get("/strength-rules", response_class=HTMLResponse)
def strength_rules_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(StrengthRule)
            .order_by(StrengthRule.priority.desc(), StrengthRule.id)
            .all()
        )
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse("strength_rules.html", {
            "request": request,
            "rules": rules,
            "saved": saved,
        })
    finally:
        session.close()


@strength_rule_router.get("/strength-rules/new", response_class=HTMLResponse)
def strength_rule_new_form(request: Request):
    return templates.TemplateResponse("strength_rule_form.html", {
        "request": request,
        "rule": None,
        "is_edit": False,
        "match_type_options": MATCH_TYPE_OPTIONS,
        "family_options": FAMILY_OPTIONS,
    })


@strength_rule_router.post("/strength-rules/new", response_class=HTMLResponse)
def strength_rule_create(
    request: Request,
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    strength_code: str = Form(...),
    strength_name: str = Form(...),
    strength_family: str = Form("metric"),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    from datetime import datetime, timezone
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = StrengthRule(
            pattern_raw=pattern_raw.strip(),
            match_type=match_type,
            strength_code=strength_code.strip(),
            strength_name=strength_name.strip(),
            strength_family=strength_family,
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
    return RedirectResponse(url="/strength-rules?saved=1", status_code=303)


@strength_rule_router.get("/strength-rules/{rule_id}/edit", response_class=HTMLResponse)
def strength_rule_edit_form(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(StrengthRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/strength-rules", status_code=303)
        return templates.TemplateResponse("strength_rule_form.html", {
            "request": request,
            "rule": rule,
            "is_edit": True,
            "match_type_options": MATCH_TYPE_OPTIONS,
            "family_options": FAMILY_OPTIONS,
        })
    finally:
        session.close()


@strength_rule_router.post("/strength-rules/{rule_id}/edit", response_class=HTMLResponse)
def strength_rule_update(
    request: Request,
    rule_id: int,
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    strength_code: str = Form(...),
    strength_name: str = Form(...),
    strength_family: str = Form("metric"),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    session = get_db_session()
    try:
        rule = session.get(StrengthRule, rule_id)
        if rule is not None:
            rule.pattern_raw = pattern_raw.strip()
            rule.match_type = match_type
            rule.strength_code = strength_code.strip()
            rule.strength_name = strength_name.strip()
            rule.strength_family = strength_family
            rule.priority = priority
            rule.is_active = bool(is_active)
            rule.note = note.strip() or None
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/strength-rules?saved=1", status_code=303)


@strength_rule_router.post("/strength-rules/{rule_id}/toggle", response_class=HTMLResponse)
def strength_rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(StrengthRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/strength-rules", status_code=303)


@strength_rule_router.post("/strength-rules/{rule_id}/delete", response_class=HTMLResponse)
def strength_rule_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(StrengthRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/strength-rules", status_code=303)

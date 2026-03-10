"""CRUD routes for CoatingRule management."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import CoatingRule

coating_rule_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

MATCH_TYPE_OPTIONS = [
    ("contains", "Содержит (contains)"),
    ("exact", "Точное совпадение (exact)"),
    ("regex", "Регулярное выражение (regex)"),
]


@coating_rule_router.get("/coating-rules", response_class=HTMLResponse)
def coating_rules_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(CoatingRule)
            .order_by(CoatingRule.priority.desc(), CoatingRule.id)
            .all()
        )
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse("coating_rules.html", {
            "request": request,
            "rules": rules,
            "saved": saved,
            "match_type_options": MATCH_TYPE_OPTIONS,
        })
    finally:
        session.close()


@coating_rule_router.get("/coating-rules/new", response_class=HTMLResponse)
def coating_rule_new_form(request: Request):
    return templates.TemplateResponse("coating_rule_form.html", {
        "request": request,
        "rule": None,
        "is_edit": False,
        "match_type_options": MATCH_TYPE_OPTIONS,
    })


@coating_rule_router.post("/coating-rules/new", response_class=HTMLResponse)
def coating_rule_create(
    request: Request,
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    coating_code: str = Form(...),
    coating_name: str = Form(...),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    from datetime import datetime, timezone
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = CoatingRule(
            pattern_raw=pattern_raw.strip(),
            match_type=match_type,
            coating_code=coating_code.strip(),
            coating_name=coating_name.strip(),
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
    return RedirectResponse(url="/coating-rules?saved=1", status_code=303)


@coating_rule_router.get("/coating-rules/{rule_id}/edit", response_class=HTMLResponse)
def coating_rule_edit_form(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(CoatingRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/coating-rules", status_code=303)
        return templates.TemplateResponse("coating_rule_form.html", {
            "request": request,
            "rule": rule,
            "is_edit": True,
            "match_type_options": MATCH_TYPE_OPTIONS,
        })
    finally:
        session.close()


@coating_rule_router.post("/coating-rules/{rule_id}/edit", response_class=HTMLResponse)
def coating_rule_update(
    request: Request,
    rule_id: int,
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    coating_code: str = Form(...),
    coating_name: str = Form(...),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    session = get_db_session()
    try:
        rule = session.get(CoatingRule, rule_id)
        if rule is not None:
            rule.pattern_raw = pattern_raw.strip()
            rule.match_type = match_type
            rule.coating_code = coating_code.strip()
            rule.coating_name = coating_name.strip()
            rule.priority = priority
            rule.is_active = bool(is_active)
            rule.note = note.strip() or None
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/coating-rules?saved=1", status_code=303)


@coating_rule_router.post("/coating-rules/{rule_id}/toggle", response_class=HTMLResponse)
def coating_rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(CoatingRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/coating-rules", status_code=303)


@coating_rule_router.post("/coating-rules/{rule_id}/delete", response_class=HTMLResponse)
def coating_rule_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(CoatingRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/coating-rules", status_code=303)

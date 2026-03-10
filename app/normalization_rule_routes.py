"""CRUD routes for unified NormalizationRule management."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import NormalizationRule

normalization_rule_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

RULE_TYPE_OPTIONS = [
    ("coating", "Покрытие"),
    ("strength", "Класс прочности"),
    ("size", "Размер"),
]

MATCH_TYPE_OPTIONS = [
    ("contains", "Содержит (contains)"),
    ("exact", "Точное совпадение (exact)"),
    ("regex", "Регулярное выражение (regex)"),
]

RULE_TYPE_LABELS = dict(RULE_TYPE_OPTIONS)
RULE_TYPE_COLORS = {
    "coating": ("#1565c0", "#e3f2fd"),
    "strength": ("#7b1fa2", "#f3e5f5"),
    "size": ("#00838f", "#e0f7fa"),
}


@normalization_rule_router.get("/normalization-rules", response_class=HTMLResponse)
def normalization_rules_list(request: Request, type: str = ""):
    session = get_db_session()
    try:
        q = session.query(NormalizationRule)
        if type and type in dict(RULE_TYPE_OPTIONS):
            q = q.filter(NormalizationRule.rule_type == type)
        rules = q.order_by(
            NormalizationRule.rule_type,
            NormalizationRule.priority.desc(),
            NormalizationRule.id,
        ).all()
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse("normalization_rules.html", {
            "request": request,
            "rules": rules,
            "saved": saved,
            "filter_type": type,
            "rule_type_options": RULE_TYPE_OPTIONS,
            "rule_type_labels": RULE_TYPE_LABELS,
            "rule_type_colors": RULE_TYPE_COLORS,
        })
    finally:
        session.close()


@normalization_rule_router.get("/normalization-rules/new", response_class=HTMLResponse)
def normalization_rule_new_form(request: Request):
    return templates.TemplateResponse("normalization_rule_form.html", {
        "request": request,
        "rule": None,
        "is_edit": False,
        "rule_type_options": RULE_TYPE_OPTIONS,
        "match_type_options": MATCH_TYPE_OPTIONS,
    })


@normalization_rule_router.post("/normalization-rules/new", response_class=HTMLResponse)
def normalization_rule_create(
    request: Request,
    rule_type: str = Form(...),
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    normalized_code: str = Form(...),
    normalized_name: str = Form(...),
    extra_json: str = Form(default=""),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    from datetime import datetime, timezone
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = NormalizationRule(
            rule_type=rule_type,
            pattern_raw=pattern_raw.strip(),
            match_type=match_type,
            normalized_code=normalized_code.strip(),
            normalized_name=normalized_name.strip(),
            extra_json=extra_json.strip() or None,
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
    return RedirectResponse(url="/normalization-rules?saved=1", status_code=303)


@normalization_rule_router.get("/normalization-rules/{rule_id}/edit", response_class=HTMLResponse)
def normalization_rule_edit_form(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(NormalizationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/normalization-rules", status_code=303)
        return templates.TemplateResponse("normalization_rule_form.html", {
            "request": request,
            "rule": rule,
            "is_edit": True,
            "rule_type_options": RULE_TYPE_OPTIONS,
            "match_type_options": MATCH_TYPE_OPTIONS,
        })
    finally:
        session.close()


@normalization_rule_router.post("/normalization-rules/{rule_id}/edit", response_class=HTMLResponse)
def normalization_rule_update(
    request: Request,
    rule_id: int,
    rule_type: str = Form(...),
    pattern_raw: str = Form(...),
    match_type: str = Form("contains"),
    normalized_code: str = Form(...),
    normalized_name: str = Form(...),
    extra_json: str = Form(default=""),
    priority: int = Form(0),
    is_active: str = Form(default=""),
    note: str = Form(default=""),
):
    session = get_db_session()
    try:
        rule = session.get(NormalizationRule, rule_id)
        if rule is not None:
            rule.rule_type = rule_type
            rule.pattern_raw = pattern_raw.strip()
            rule.match_type = match_type
            rule.normalized_code = normalized_code.strip()
            rule.normalized_name = normalized_name.strip()
            rule.extra_json = extra_json.strip() or None
            rule.priority = priority
            rule.is_active = bool(is_active)
            rule.note = note.strip() or None
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/normalization-rules?saved=1", status_code=303)


@normalization_rule_router.post("/normalization-rules/{rule_id}/toggle", response_class=HTMLResponse)
def normalization_rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(NormalizationRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/normalization-rules", status_code=303)


@normalization_rule_router.post("/normalization-rules/{rule_id}/delete", response_class=HTMLResponse)
def normalization_rule_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(NormalizationRule, rule_id)
        if rule is not None:
            session.delete(rule)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/normalization-rules", status_code=303)

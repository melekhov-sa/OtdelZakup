"""Web routes for category-based validation rule CRUD."""

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import BaseValidationRule, ValidationRuleException, VALIDATION_FIELD_LABELS

category_rule_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

AVAILABLE_FIELDS = list(VALIDATION_FIELD_LABELS.items())


# ── Rules list ───────────────────────────────────────────────────────────────

@category_rule_router.get("/validation-rules", response_class=HTMLResponse)
def category_rules_list(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(BaseValidationRule)
            .order_by(BaseValidationRule.priority.desc(), BaseValidationRule.id)
            .all()
        )
        # Count exceptions per rule
        exc_counts = {}
        for rule in rules:
            exc_counts[rule.id] = (
                session.query(ValidationRuleException)
                .filter(ValidationRuleException.base_rule_id == rule.id)
                .count()
            )
        return templates.TemplateResponse(
            "category_rules.html",
            {
                "request": request,
                "rules": rules,
                "exc_counts": exc_counts,
                "field_labels": VALIDATION_FIELD_LABELS,
            },
        )
    finally:
        session.close()


# ── Create rule ──────────────────────────────────────────────────────────────

@category_rule_router.get("/validation-rules/new", response_class=HTMLResponse)
def category_rule_new(request: Request):
    return templates.TemplateResponse(
        "category_rule_form.html",
        {
            "request": request,
            "rule": None,
            "is_edit": False,
            "available_fields": AVAILABLE_FIELDS,
        },
    )


@category_rule_router.post("/validation-rules/new", response_class=HTMLResponse)
def category_rule_create(
    request: Request,
    category_code: str = Form(...),
    category_name: str = Form(...),
    subcategory_code: str = Form(default=""),
    subcategory_name: str = Form(default=""),
    item_type_code: str = Form(default=""),
    item_type_name: str = Form(default=""),
    required_fields: list[str] = Form(default=[]),
    priority: int = Form(default=0),
    is_active: bool = Form(default=False),
):
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        rule = BaseValidationRule(
            category_code=category_code.strip(),
            category_name=category_name.strip(),
            subcategory_code=subcategory_code.strip() or None,
            subcategory_name=subcategory_name.strip() or None,
            item_type_code=item_type_code.strip() or None,
            item_type_name=item_type_name.strip() or None,
            priority=priority,
            is_active=is_active,
            created_at=now,
            updated_at=now,
        )
        rule.required_fields_list = required_fields
        session.add(rule)
        session.commit()
        return RedirectResponse(url="/validation-rules", status_code=303)
    finally:
        session.close()


# ── Edit rule ────────────────────────────────────────────────────────────────

@category_rule_router.get("/validation-rules/{rule_id}/edit", response_class=HTMLResponse)
def category_rule_edit(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        return templates.TemplateResponse(
            "category_rule_form.html",
            {
                "request": request,
                "rule": rule,
                "is_edit": True,
                "available_fields": AVAILABLE_FIELDS,
            },
        )
    finally:
        session.close()


@category_rule_router.post("/validation-rules/{rule_id}/edit", response_class=HTMLResponse)
def category_rule_update(
    request: Request,
    rule_id: int,
    category_code: str = Form(...),
    category_name: str = Form(...),
    subcategory_code: str = Form(default=""),
    subcategory_name: str = Form(default=""),
    item_type_code: str = Form(default=""),
    item_type_name: str = Form(default=""),
    required_fields: list[str] = Form(default=[]),
    priority: int = Form(default=0),
    is_active: bool = Form(default=False),
):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        rule.category_code = category_code.strip()
        rule.category_name = category_name.strip()
        rule.subcategory_code = subcategory_code.strip() or None
        rule.subcategory_name = subcategory_name.strip() or None
        rule.item_type_code = item_type_code.strip() or None
        rule.item_type_name = item_type_name.strip() or None
        rule.required_fields_list = required_fields
        rule.priority = priority
        rule.is_active = is_active
        rule.updated_at = datetime.now(timezone.utc)
        session.commit()
        return RedirectResponse(url="/validation-rules", status_code=303)
    finally:
        session.close()


# ── Delete rule ──────────────────────────────────────────────────────────────

@category_rule_router.post("/validation-rules/{rule_id}/delete", response_class=HTMLResponse)
def category_rule_delete(request: Request, rule_id: int):
    session = get_db_session()
    try:
        # Delete exceptions first
        session.query(ValidationRuleException).filter(
            ValidationRuleException.base_rule_id == rule_id
        ).delete()
        rule = session.get(BaseValidationRule, rule_id)
        if rule is not None:
            session.delete(rule)
        session.commit()
        return RedirectResponse(url="/validation-rules", status_code=303)
    finally:
        session.close()


# ── Toggle rule active/inactive ──────────────────────────────────────────────

@category_rule_router.post("/validation-rules/{rule_id}/toggle", response_class=HTMLResponse)
def category_rule_toggle(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is not None:
            rule.is_active = not rule.is_active
            rule.updated_at = datetime.now(timezone.utc)
            session.commit()
        return RedirectResponse(url="/validation-rules", status_code=303)
    finally:
        session.close()


# ── Exceptions list (per rule) ───────────────────────────────────────────────

@category_rule_router.get("/validation-rules/{rule_id}/exceptions", response_class=HTMLResponse)
def category_rule_exceptions(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        exceptions = (
            session.query(ValidationRuleException)
            .filter(ValidationRuleException.base_rule_id == rule_id)
            .order_by(ValidationRuleException.priority.desc(), ValidationRuleException.id)
            .all()
        )
        return templates.TemplateResponse(
            "category_rule_exceptions.html",
            {
                "request": request,
                "rule": rule,
                "exceptions": exceptions,
                "field_labels": VALIDATION_FIELD_LABELS,
            },
        )
    finally:
        session.close()


# ── Create exception ─────────────────────────────────────────────────────────

@category_rule_router.get("/validation-rules/{rule_id}/exceptions/new", response_class=HTMLResponse)
def category_exception_new(request: Request, rule_id: int):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        return templates.TemplateResponse(
            "category_exception_form.html",
            {
                "request": request,
                "rule": rule,
                "exc": None,
                "is_edit": False,
                "available_fields": AVAILABLE_FIELDS,
            },
        )
    finally:
        session.close()


@category_rule_router.post("/validation-rules/{rule_id}/exceptions/new", response_class=HTMLResponse)
def category_exception_create(
    request: Request,
    rule_id: int,
    match_type_name: str = Form(default=""),
    match_standard: str = Form(default=""),
    override_required_fields: list[str] = Form(default=[]),
    note: str = Form(default=""),
    priority: int = Form(default=0),
    is_active: bool = Form(default=False),
):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        now = datetime.now(timezone.utc)
        exc = ValidationRuleException(
            base_rule_id=rule_id,
            match_type_name=match_type_name.strip() or None,
            match_standard=match_standard.strip() or None,
            note=note.strip() or None,
            priority=priority,
            is_active=is_active,
            created_at=now,
            updated_at=now,
        )
        exc.override_required_fields_list = override_required_fields
        session.add(exc)
        session.commit()
        return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
    finally:
        session.close()


# ── Edit exception ───────────────────────────────────────────────────────────

@category_rule_router.get("/validation-rules/{rule_id}/exceptions/{exc_id}/edit", response_class=HTMLResponse)
def category_exception_edit(request: Request, rule_id: int, exc_id: int):
    session = get_db_session()
    try:
        rule = session.get(BaseValidationRule, rule_id)
        if rule is None:
            return RedirectResponse(url="/validation-rules", status_code=303)
        exc = session.get(ValidationRuleException, exc_id)
        if exc is None or exc.base_rule_id != rule_id:
            return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
        return templates.TemplateResponse(
            "category_exception_form.html",
            {
                "request": request,
                "rule": rule,
                "exc": exc,
                "is_edit": True,
                "available_fields": AVAILABLE_FIELDS,
            },
        )
    finally:
        session.close()


@category_rule_router.post("/validation-rules/{rule_id}/exceptions/{exc_id}/edit", response_class=HTMLResponse)
def category_exception_update(
    request: Request,
    rule_id: int,
    exc_id: int,
    match_type_name: str = Form(default=""),
    match_standard: str = Form(default=""),
    override_required_fields: list[str] = Form(default=[]),
    note: str = Form(default=""),
    priority: int = Form(default=0),
    is_active: bool = Form(default=False),
):
    session = get_db_session()
    try:
        exc = session.get(ValidationRuleException, exc_id)
        if exc is None or exc.base_rule_id != rule_id:
            return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
        exc.match_type_name = match_type_name.strip() or None
        exc.match_standard = match_standard.strip() or None
        exc.override_required_fields_list = override_required_fields
        exc.note = note.strip() or None
        exc.priority = priority
        exc.is_active = is_active
        exc.updated_at = datetime.now(timezone.utc)
        session.commit()
        return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
    finally:
        session.close()


# ── Delete exception ─────────────────────────────────────────────────────────

@category_rule_router.post("/validation-rules/{rule_id}/exceptions/{exc_id}/delete", response_class=HTMLResponse)
def category_exception_delete(request: Request, rule_id: int, exc_id: int):
    session = get_db_session()
    try:
        exc = session.get(ValidationRuleException, exc_id)
        if exc is not None and exc.base_rule_id == rule_id:
            session.delete(exc)
            session.commit()
        return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
    finally:
        session.close()


# ── Toggle exception active/inactive ─────────────────────────────────────────

@category_rule_router.post("/validation-rules/{rule_id}/exceptions/{exc_id}/toggle", response_class=HTMLResponse)
def category_exception_toggle(request: Request, rule_id: int, exc_id: int):
    session = get_db_session()
    try:
        exc = session.get(ValidationRuleException, exc_id)
        if exc is not None and exc.base_rule_id == rule_id:
            exc.is_active = not exc.is_active
            exc.updated_at = datetime.now(timezone.utc)
            session.commit()
        return RedirectResponse(url=f"/validation-rules/{rule_id}/exceptions", status_code=303)
    finally:
        session.close()

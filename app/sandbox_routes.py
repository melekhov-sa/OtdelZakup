"""Web routes for Sandbox Mode."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.sandbox import (
    apply_snapshot_to_prod,
    create_sandbox_session,
    get_sandbox,
    get_snapshot_list,
    get_snapshot_rule,
    load_snapshot_rules,
    snapshot_add_rule,
    snapshot_delete_rule,
    snapshot_toggle_rule,
    snapshot_update_rule,
    update_sandbox_file_id,
    update_sandbox_snapshot,
)

sandbox_router = APIRouter(prefix="/sandbox")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

ITEM_TYPES = ["болт", "винт", "гайка", "шайба", "шпилька", "саморез", "шуруп", "анкер"]
AVAILABLE_FIELDS = [
    ("size", "Размер"), ("qty", "Количество"), ("uom", "Ед."),
    ("item_type", "Тип изделия"), ("length", "Длина"), ("strength", "Класс прочности"),
    ("gost", "ГОСТ"), ("iso", "ISO"), ("din", "DIN"), ("coating", "Покрытие"),
    ("name", "Наименование"), ("code", "Код"),
]
AVAILABLE_FIELDS_DICT = dict(AVAILABLE_FIELDS)
STANDARD_KINDS = ["DIN", "ISO", "GOST"]
INFERENCE_MODES = [
    ("DIAMETER_AS_SIZE", "Размер = Диаметр"),
    ("DIAMETER_X_LENGTH_AS_SIZE", "Размер = Диаметр × Длина"),
]
FORCE_STATUS_OPTIONS = [
    ("", "—"), ("review", "Требуется просмотреть"), ("manual", "Требуется вручную разобрать"),
]
CONDITION_TYPE_OPTIONS = [
    ("FIELDS_REQUIRED", "Обязательные поля заполнены"),
    ("FIELDS_FORBIDDEN", "Запрещённые поля пустые"),
    ("STANDARD_MATCH", "Тип изделия соответствует стандарту"),
]
STANDARD_SOURCE_OPTIONS = [
    ("ANY", "Любой"), ("DIN", "DIN"), ("ISO", "ISO"), ("GOST", "ГОСТ"),
]
EXPECTED_MODE_OPTIONS = [
    ("FROM_DIRECTORY", "Брать из справочника стандартов"),
    ("FIXED", "Задать вручную"),
]


def _sb(sid: int):
    """Load sandbox session or return None."""
    return get_sandbox(sid)


def _sb_ctx(sb) -> dict:
    """Common template context for sandbox pages."""
    return {
        "sandbox_session": sb,
        "sandbox_id": sb.id,
    }


# ── Main sandbox routes ───────────────────────────────────────────────────────

@sandbox_router.post("/new", response_class=HTMLResponse)
async def sandbox_new(request: Request):
    """Create a new sandbox session from current prod rules."""
    sid = create_sandbox_session()
    return RedirectResponse(url=f"/sandbox/{sid}", status_code=303)


@sandbox_router.get("/{sid}", response_class=HTMLResponse)
async def sandbox_view(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)

    snapshot = json.loads(sb.rule_snapshot_json)
    counts = {
        "readiness": len(snapshot.get("readiness_rules", [])),
        "validation": len(snapshot.get("validation_rules", [])),
        "inference": len(snapshot.get("inference_rules", [])),
        "standards": len(snapshot.get("standard_refs", [])),
    }
    return templates.TemplateResponse(
        "sandbox.html",
        {
            "request": request,
            "counts": counts,
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/upload", response_class=HTMLResponse)
async def sandbox_upload(request: Request, sid: int, file: UploadFile = File(...)):
    """Upload a file into the sandbox context and auto-transform with sandbox rules."""
    from app.cache import UPLOAD_DIR, file_id_from_bytes, save_cache
    from app.parser_excel import ParseError, parse_excel
    from app.main import _detected_to_dict

    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)

    fname = (file.filename or "").lower()
    if not fname.endswith(".xlsx"):
        return templates.TemplateResponse(
            "sandbox.html",
            {
                "request": request,
                "error": "Только файлы .xlsx допускаются.",
                "counts": _snapshot_counts(sb),
                **_sb_ctx(sb),
            },
            status_code=400,
        )

    file_bytes = await file.read()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename
    dest.write_bytes(file_bytes)
    fid = file_id_from_bytes(file_bytes)

    try:
        result = parse_excel(dest)
    except (ParseError, Exception) as exc:
        return templates.TemplateResponse(
            "sandbox.html",
            {
                "request": request,
                "error": f"Ошибка чтения файла: {exc}",
                "counts": _snapshot_counts(sb),
                **_sb_ctx(sb),
            },
            status_code=400,
        )

    if result.df is not None and not result.needs_manual_selection:
        save_cache(fid, file.filename, result.df, detected_columns=_detected_to_dict(result.detected))

    update_sandbox_file_id(sid, fid)
    return RedirectResponse(url=f"/sandbox/{sid}/transform?file_id={fid}", status_code=303)


@sandbox_router.get("/{sid}/transform", response_class=HTMLResponse)
async def sandbox_transform_get(request: Request, sid: int, file_id: str = ""):
    """Transform the given file with sandbox rules."""
    return await _do_transform(request, sid, file_id, [])


@sandbox_router.post("/{sid}/transform", response_class=HTMLResponse)
async def sandbox_transform_post(
    request: Request,
    sid: int,
    file_id: str = Form(...),
    fields: List[str] = Form(default=[]),
):
    return await _do_transform(request, sid, file_id, fields)


async def _do_transform(request: Request, sid: int, file_id: str, fields: list):
    from app.cache import load_dataframe, load_meta
    from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS, transform_dataframe
    from app.readiness import apply_readiness
    from app.parser_excel import dataframe_preview, dataframe_to_html

    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)

    df = load_dataframe(file_id)
    meta = load_meta(file_id)
    if df is None or meta is None:
        return templates.TemplateResponse(
            "sandbox.html",
            {
                "request": request,
                "error": "Файл не найден. Загрузите файл заново.",
                "counts": _snapshot_counts(sb),
                **_sb_ctx(sb),
            },
            status_code=400,
        )

    rule_ctx = load_snapshot_rules(sb.rule_snapshot_json)
    valid_fields = [f for f in fields if f in EXTRACTORS] if fields else list(EXTRACTORS.keys())

    transformed = transform_dataframe(df, valid_fields)
    transformed = apply_readiness(
        df, transformed,
        rules=rule_ctx["readiness_rules"],
        standards_cache=rule_ctx["standards_cache"],
        inference_rules=rule_ctx["inference_rules"],
        validation_rules=rule_ctx["validation_rules"],
    )

    _INTERNAL = frozenset({"raw_text", "qty_uom_source"})

    def _drop(frame):
        return frame.drop(columns=[c for c in _INTERNAL if c in frame.columns])

    stats = _compute_stats(transformed)
    result_html = _result_table_html(_drop(dataframe_preview(transformed, limit=200)))

    return templates.TemplateResponse(
        "sandbox_result.html",
        {
            "request": request,
            "filename": meta["filename"],
            "file_id": file_id,
            "stats": stats,
            "result_table": result_html,
            "extractors": EXTRACTORS,
            "field_keys": DEFAULT_FIELD_KEYS,
            **_sb_ctx(sb),
        },
    )


@sandbox_router.get("/{sid}/compare/{file_id}", response_class=HTMLResponse)
async def sandbox_compare(request: Request, sid: int, file_id: str):
    """Process the file twice (sandbox rules vs prod rules), show diff."""
    import pandas as pd
    from app.cache import load_dataframe, load_meta
    from app.extractors import EXTRACTORS, transform_dataframe
    from app.readiness import (
        apply_readiness,
        load_active_rules,
        load_active_standards,
        load_active_validation_rules,
    )
    from app.inference_engine import load_active_inference_rules
    from app.parser_excel import dataframe_preview

    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)

    df = load_dataframe(file_id)
    meta = load_meta(file_id)
    if df is None or meta is None:
        return RedirectResponse(url=f"/sandbox/{sid}", status_code=303)

    all_fields = list(EXTRACTORS.keys())

    # Sandbox run
    rule_ctx = load_snapshot_rules(sb.rule_snapshot_json)
    sb_transformed = transform_dataframe(df.copy(), all_fields)
    sb_transformed = apply_readiness(
        df, sb_transformed,
        rules=rule_ctx["readiness_rules"],
        standards_cache=rule_ctx["standards_cache"],
        inference_rules=rule_ctx["inference_rules"],
        validation_rules=rule_ctx["validation_rules"],
    )

    # Prod run
    prod_transformed = transform_dataframe(df.copy(), all_fields)
    prod_transformed = apply_readiness(
        df, prod_transformed,
        rules=load_active_rules(),
        standards_cache=load_active_standards(),
        inference_rules=load_active_inference_rules(),
        validation_rules=load_active_validation_rules(),
    )

    # Build diff
    _COMPARE_COLS = ["status", "reason"]
    # Also try size column
    from app.extractors import EXTRACTORS as _EX
    _size_col = _EX.get("size", ("Размер MxL", None))[0]
    _itype_col = _EX.get("item_type", ("Тип изделия", None))[0]

    diffs = []
    for row_num, idx in enumerate(sb_transformed.index, start=1):
        row_diff: dict = {"row": row_num}
        changed = False
        for col in ("status", "reason"):
            sb_val = str(sb_transformed.at[idx, col]) if col in sb_transformed.columns else ""
            pr_val = str(prod_transformed.at[idx, col]) if col in prod_transformed.columns else ""
            if sb_val != pr_val:
                row_diff[col] = {"sandbox": sb_val, "prod": pr_val}
                changed = True
        for col in (_size_col, _itype_col):
            if col:
                sb_val = str(sb_transformed.at[idx, col]) if col in sb_transformed.columns else ""
                pr_val = str(prod_transformed.at[idx, col]) if col in prod_transformed.columns else ""
                if sb_val != pr_val:
                    row_diff[col] = {"sandbox": sb_val, "prod": pr_val}
                    changed = True
        if changed:
            diffs.append(row_diff)

    return templates.TemplateResponse(
        "sandbox_compare.html",
        {
            "request": request,
            "filename": meta["filename"],
            "diffs": diffs,
            "total_rows": len(sb_transformed),
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/apply", response_class=HTMLResponse)
async def sandbox_apply(request: Request, sid: int):
    """Apply sandbox snapshot to prod, then close the sandbox."""
    from app.database import get_db_session
    from app.models import SandboxSession

    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)

    apply_snapshot_to_prod(sb.rule_snapshot_json, f"Применено из sandbox #{sid}")

    db = get_db_session()
    try:
        s = db.get(SandboxSession, sid)
        if s:
            s.is_active = False
            s.is_applied = True
            db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/", status_code=303)


@sandbox_router.post("/{sid}/cancel", response_class=HTMLResponse)
async def sandbox_cancel(request: Request, sid: int):
    """Discard sandbox session without applying changes."""
    from app.database import get_db_session
    from app.models import SandboxSession

    db = get_db_session()
    try:
        sb = db.get(SandboxSession, sid)
        if sb:
            sb.is_active = False
            db.commit()
    finally:
        db.close()

    return RedirectResponse(url="/", status_code=303)


# ── Sandbox Readiness Rules ───────────────────────────────────────────────────

@sandbox_router.get("/{sid}/readiness-rules", response_class=HTMLResponse)
async def sb_readiness_list(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    rules_raw = get_snapshot_list(sb.rule_snapshot_json, "readiness_rules")
    from app.sandbox import _from_dict, _SandboxReadinessRule
    rules = [_from_dict(_SandboxReadinessRule, r) for r in rules_raw]
    return templates.TemplateResponse(
        "sandbox_readiness_list.html",
        {"request": request, "rules": rules, "available_fields": AVAILABLE_FIELDS_DICT, **_sb_ctx(sb)},
    )


@sandbox_router.get("/{sid}/readiness-rules/new", response_class=HTMLResponse)
async def sb_readiness_new(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "readiness_form.html",
        {
            "request": request, "rule": None, "is_edit": False,
            "item_types": ITEM_TYPES, "available_fields": AVAILABLE_FIELDS,
            "form_action": f"/sandbox/{sid}/readiness-rules/create",
            "back_url": f"/sandbox/{sid}/readiness-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/readiness-rules/create", response_class=HTMLResponse)
async def sb_readiness_create(
    request: Request, sid: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    priority: int = Form(default=0),
    require_fields: List[str] = Form(default=[]),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    rule_dict = {
        "name": name, "description": description,
        "item_type": item_type if item_type else None,
        "require_fields": json.dumps(require_fields, ensure_ascii=False),
        "priority": priority, "is_active": True,
    }
    new_snap = snapshot_add_rule(sb.rule_snapshot_json, "readiness_rules", rule_dict)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/readiness-rules", status_code=303)


@sandbox_router.get("/{sid}/readiness-rules/{rid}/edit", response_class=HTMLResponse)
async def sb_readiness_edit(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxReadinessRule
    raw = get_snapshot_rule(sb.rule_snapshot_json, "readiness_rules", rid)
    if raw is None:
        return RedirectResponse(url=f"/sandbox/{sid}/readiness-rules", status_code=303)
    rule = _from_dict(_SandboxReadinessRule, raw)
    return templates.TemplateResponse(
        "readiness_form.html",
        {
            "request": request, "rule": rule, "is_edit": True,
            "item_types": ITEM_TYPES, "available_fields": AVAILABLE_FIELDS,
            "form_action": f"/sandbox/{sid}/readiness-rules/{rid}/update",
            "back_url": f"/sandbox/{sid}/readiness-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/readiness-rules/{rid}/update", response_class=HTMLResponse)
async def sb_readiness_update(
    request: Request, sid: int, rid: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    priority: int = Form(default=0),
    require_fields: List[str] = Form(default=[]),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    updates = {
        "name": name, "description": description,
        "item_type": item_type if item_type else None,
        "require_fields": json.dumps(require_fields, ensure_ascii=False),
        "priority": priority,
    }
    new_snap = snapshot_update_rule(sb.rule_snapshot_json, "readiness_rules", rid, updates)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/readiness-rules", status_code=303)


@sandbox_router.post("/{sid}/readiness-rules/{rid}/toggle", response_class=HTMLResponse)
async def sb_readiness_toggle(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_toggle_rule(sb.rule_snapshot_json, "readiness_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/readiness-rules", status_code=303)


@sandbox_router.post("/{sid}/readiness-rules/{rid}/delete", response_class=HTMLResponse)
async def sb_readiness_delete(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_delete_rule(sb.rule_snapshot_json, "readiness_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/readiness-rules", status_code=303)


# ── Sandbox Validation Rules ──────────────────────────────────────────────────

@sandbox_router.get("/{sid}/validation-rules", response_class=HTMLResponse)
async def sb_validation_list(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxValidationRule
    rules_raw = get_snapshot_list(sb.rule_snapshot_json, "validation_rules")
    rules = [_from_dict(_SandboxValidationRule, r) for r in rules_raw]
    return templates.TemplateResponse(
        "sandbox_validation_list.html",
        {"request": request, "rules": rules, "available_fields": AVAILABLE_FIELDS_DICT, **_sb_ctx(sb)},
    )


@sandbox_router.get("/{sid}/validation-rules/new", response_class=HTMLResponse)
async def sb_validation_new(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "rules_form.html",
        {
            "request": request, "rule": None, "is_edit": False,
            "item_types": ITEM_TYPES, "available_fields": AVAILABLE_FIELDS,
            "force_status_options": FORCE_STATUS_OPTIONS,
            "condition_type_options": CONDITION_TYPE_OPTIONS,
            "standard_source_options": STANDARD_SOURCE_OPTIONS,
            "expected_item_type_mode_options": EXPECTED_MODE_OPTIONS,
            "form_action": f"/sandbox/{sid}/validation-rules/create",
            "back_url": f"/sandbox/{sid}/validation-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/validation-rules/create", response_class=HTMLResponse)
async def sb_validation_create(
    request: Request, sid: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: List[str] = Form(default=[]),
    forbid_fields: List[str] = Form(default=[]),
    force_status: str = Form(default=""),
    priority: int = Form(default=0),
    condition_type: str = Form(default="FIELDS_REQUIRED"),
    standard_source: str = Form(default="ANY"),
    expected_item_type_mode: str = Form(default="FROM_DIRECTORY"),
    expected_item_type: str = Form(default=""),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    rule_dict = {
        "name": name, "description": description,
        "item_type": item_type if item_type else None,
        "require_fields": json.dumps(require_fields, ensure_ascii=False),
        "forbid_fields": json.dumps(forbid_fields, ensure_ascii=False),
        "force_status": force_status if force_status else None,
        "condition_type": condition_type,
        "standard_source": standard_source,
        "expected_item_type_mode": expected_item_type_mode,
        "expected_item_type": expected_item_type if expected_item_type else None,
        "priority": priority, "is_active": True,
    }
    new_snap = snapshot_add_rule(sb.rule_snapshot_json, "validation_rules", rule_dict)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/validation-rules", status_code=303)


@sandbox_router.get("/{sid}/validation-rules/{rid}/edit", response_class=HTMLResponse)
async def sb_validation_edit(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxValidationRule
    raw = get_snapshot_rule(sb.rule_snapshot_json, "validation_rules", rid)
    if raw is None:
        return RedirectResponse(url=f"/sandbox/{sid}/validation-rules", status_code=303)
    rule = _from_dict(_SandboxValidationRule, raw)
    return templates.TemplateResponse(
        "rules_form.html",
        {
            "request": request, "rule": rule, "is_edit": True,
            "item_types": ITEM_TYPES, "available_fields": AVAILABLE_FIELDS,
            "force_status_options": FORCE_STATUS_OPTIONS,
            "condition_type_options": CONDITION_TYPE_OPTIONS,
            "standard_source_options": STANDARD_SOURCE_OPTIONS,
            "expected_item_type_mode_options": EXPECTED_MODE_OPTIONS,
            "form_action": f"/sandbox/{sid}/validation-rules/{rid}/update",
            "back_url": f"/sandbox/{sid}/validation-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/validation-rules/{rid}/update", response_class=HTMLResponse)
async def sb_validation_update(
    request: Request, sid: int, rid: int,
    name: str = Form(...),
    description: str = Form(default=""),
    item_type: str = Form(default=""),
    require_fields: List[str] = Form(default=[]),
    forbid_fields: List[str] = Form(default=[]),
    force_status: str = Form(default=""),
    priority: int = Form(default=0),
    condition_type: str = Form(default="FIELDS_REQUIRED"),
    standard_source: str = Form(default="ANY"),
    expected_item_type_mode: str = Form(default="FROM_DIRECTORY"),
    expected_item_type: str = Form(default=""),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    updates = {
        "name": name, "description": description,
        "item_type": item_type if item_type else None,
        "require_fields": json.dumps(require_fields, ensure_ascii=False),
        "forbid_fields": json.dumps(forbid_fields, ensure_ascii=False),
        "force_status": force_status if force_status else None,
        "condition_type": condition_type, "standard_source": standard_source,
        "expected_item_type_mode": expected_item_type_mode,
        "expected_item_type": expected_item_type if expected_item_type else None,
        "priority": priority,
    }
    new_snap = snapshot_update_rule(sb.rule_snapshot_json, "validation_rules", rid, updates)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/validation-rules", status_code=303)


@sandbox_router.post("/{sid}/validation-rules/{rid}/toggle", response_class=HTMLResponse)
async def sb_validation_toggle(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_toggle_rule(sb.rule_snapshot_json, "validation_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/validation-rules", status_code=303)


@sandbox_router.post("/{sid}/validation-rules/{rid}/delete", response_class=HTMLResponse)
async def sb_validation_delete(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_delete_rule(sb.rule_snapshot_json, "validation_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/validation-rules", status_code=303)


# ── Sandbox Inference Rules ───────────────────────────────────────────────────

@sandbox_router.get("/{sid}/inference-rules", response_class=HTMLResponse)
async def sb_inference_list(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxInferenceRule
    rules_raw = get_snapshot_list(sb.rule_snapshot_json, "inference_rules")
    rules = [_from_dict(_SandboxInferenceRule, r) for r in rules_raw]
    return templates.TemplateResponse(
        "sandbox_inference_list.html",
        {"request": request, "rules": rules, **_sb_ctx(sb)},
    )


@sandbox_router.get("/{sid}/inference-rules/new", response_class=HTMLResponse)
async def sb_inference_new(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "inference_form.html",
        {
            "request": request, "rule": None, "is_edit": False,
            "item_types": ITEM_TYPES, "modes": INFERENCE_MODES,
            "form_action": f"/sandbox/{sid}/inference-rules/create",
            "back_url": f"/sandbox/{sid}/inference-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/inference-rules/create", response_class=HTMLResponse)
async def sb_inference_create(
    request: Request, sid: int,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: List[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    rule_dict = {
        "name": name, "is_active": True, "target_field": "size",
        "item_types": json.dumps(item_types, ensure_ascii=False) if item_types else None,
        "mode": mode, "conditions_json": None, "priority": priority,
    }
    new_snap = snapshot_add_rule(sb.rule_snapshot_json, "inference_rules", rule_dict)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/inference-rules", status_code=303)


@sandbox_router.get("/{sid}/inference-rules/{rid}/edit", response_class=HTMLResponse)
async def sb_inference_edit(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxInferenceRule
    raw = get_snapshot_rule(sb.rule_snapshot_json, "inference_rules", rid)
    if raw is None:
        return RedirectResponse(url=f"/sandbox/{sid}/inference-rules", status_code=303)
    rule = _from_dict(_SandboxInferenceRule, raw)
    return templates.TemplateResponse(
        "inference_form.html",
        {
            "request": request, "rule": rule, "is_edit": True,
            "item_types": ITEM_TYPES, "modes": INFERENCE_MODES,
            "form_action": f"/sandbox/{sid}/inference-rules/{rid}/update",
            "back_url": f"/sandbox/{sid}/inference-rules",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/inference-rules/{rid}/update", response_class=HTMLResponse)
async def sb_inference_update(
    request: Request, sid: int, rid: int,
    name: str = Form(...),
    mode: str = Form(...),
    item_types: List[str] = Form(default=[]),
    priority: int = Form(default=0),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    updates = {
        "name": name, "mode": mode,
        "item_types": json.dumps(item_types, ensure_ascii=False) if item_types else None,
        "priority": priority,
    }
    new_snap = snapshot_update_rule(sb.rule_snapshot_json, "inference_rules", rid, updates)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/inference-rules", status_code=303)


@sandbox_router.post("/{sid}/inference-rules/{rid}/toggle", response_class=HTMLResponse)
async def sb_inference_toggle(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_toggle_rule(sb.rule_snapshot_json, "inference_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/inference-rules", status_code=303)


@sandbox_router.post("/{sid}/inference-rules/{rid}/delete", response_class=HTMLResponse)
async def sb_inference_delete(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_delete_rule(sb.rule_snapshot_json, "inference_rules", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/inference-rules", status_code=303)


# ── Sandbox Standards ─────────────────────────────────────────────────────────

@sandbox_router.get("/{sid}/standards", response_class=HTMLResponse)
async def sb_standards_list(request: Request, sid: int, q: str = "", kind: str = ""):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxStandardRef
    refs_raw = get_snapshot_list(sb.rule_snapshot_json, "standard_refs")
    refs = [_from_dict(_SandboxStandardRef, r) for r in refs_raw]
    if q:
        q_lower = q.lower()
        refs = [r for r in refs if q_lower in (r.standard_code or "").lower()
                or q_lower in (r.title or "").lower()
                or q_lower in (r.item_type or "").lower()]
    if kind:
        refs = [r for r in refs if r.standard_kind == kind]
    return templates.TemplateResponse(
        "sandbox_standards_list.html",
        {
            "request": request, "standards": refs,
            "standard_kinds": STANDARD_KINDS,
            "q": q, "kind_filter": kind, "active_only": False,
            **_sb_ctx(sb),
        },
    )


@sandbox_router.get("/{sid}/standards/new", response_class=HTMLResponse)
async def sb_standards_new(request: Request, sid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "standard_form.html",
        {
            "request": request, "standard": None, "is_edit": False,
            "standard_kinds": STANDARD_KINDS, "item_types": ITEM_TYPES,
            "form_action": f"/sandbox/{sid}/standards/create",
            "back_url": f"/sandbox/{sid}/standards",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/standards/create", response_class=HTMLResponse)
async def sb_standards_create(
    request: Request, sid: int,
    standard_kind: str = Form(...),
    standard_code: str = Form(...),
    title: str = Form(default=""),
    item_type: str = Form(default=""),
    notes: str = Form(default=""),
    is_active: str = Form(default=""),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    ref_dict = {
        "standard_kind": standard_kind, "standard_code": standard_code,
        "title": title if title else None,
        "item_type": item_type if item_type else None,
        "notes": notes if notes else None,
        "is_active": is_active == "1",
    }
    new_snap = snapshot_add_rule(sb.rule_snapshot_json, "standard_refs", ref_dict)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/standards", status_code=303)


@sandbox_router.get("/{sid}/standards/{rid}/edit", response_class=HTMLResponse)
async def sb_standards_edit(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    from app.sandbox import _from_dict, _SandboxStandardRef
    raw = get_snapshot_rule(sb.rule_snapshot_json, "standard_refs", rid)
    if raw is None:
        return RedirectResponse(url=f"/sandbox/{sid}/standards", status_code=303)
    standard = _from_dict(_SandboxStandardRef, raw)
    return templates.TemplateResponse(
        "standard_form.html",
        {
            "request": request, "standard": standard, "is_edit": True,
            "standard_kinds": STANDARD_KINDS, "item_types": ITEM_TYPES,
            "form_action": f"/sandbox/{sid}/standards/{rid}/update",
            "back_url": f"/sandbox/{sid}/standards",
            **_sb_ctx(sb),
        },
    )


@sandbox_router.post("/{sid}/standards/{rid}/update", response_class=HTMLResponse)
async def sb_standards_update(
    request: Request, sid: int, rid: int,
    standard_kind: str = Form(...),
    standard_code: str = Form(...),
    title: str = Form(default=""),
    item_type: str = Form(default=""),
    notes: str = Form(default=""),
    is_active: str = Form(default=""),
):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    updates = {
        "standard_kind": standard_kind, "standard_code": standard_code,
        "title": title if title else None,
        "item_type": item_type if item_type else None,
        "notes": notes if notes else None,
        "is_active": is_active == "1",
    }
    new_snap = snapshot_update_rule(sb.rule_snapshot_json, "standard_refs", rid, updates)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/standards", status_code=303)


@sandbox_router.post("/{sid}/standards/{rid}/toggle", response_class=HTMLResponse)
async def sb_standards_toggle(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_toggle_rule(sb.rule_snapshot_json, "standard_refs", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/standards", status_code=303)


@sandbox_router.post("/{sid}/standards/{rid}/delete", response_class=HTMLResponse)
async def sb_standards_delete(request: Request, sid: int, rid: int):
    sb = _sb(sid)
    if sb is None or not sb.is_active:
        return RedirectResponse(url="/", status_code=303)
    new_snap = snapshot_delete_rule(sb.rule_snapshot_json, "standard_refs", rid)
    update_sandbox_snapshot(sid, new_snap)
    return RedirectResponse(url=f"/sandbox/{sid}/standards", status_code=303)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _snapshot_counts(sb) -> dict:
    snap = json.loads(sb.rule_snapshot_json)
    return {
        "readiness": len(snap.get("readiness_rules", [])),
        "validation": len(snap.get("validation_rules", [])),
        "inference": len(snap.get("inference_rules", [])),
        "standards": len(snap.get("standard_refs", [])),
    }


def _compute_stats(transformed) -> dict:
    if "status" not in transformed.columns:
        return {"ok": 0, "review": 0, "manual": 0, "total": len(transformed), "ok_pct": 0}
    counts = transformed["status"].value_counts()
    ok = int(counts.get("ok", 0))
    review = int(counts.get("review", 0))
    manual = int(counts.get("manual", 0))
    total = len(transformed)
    pct = round(ok / total * 100) if total else 0
    return {"ok": ok, "review": review, "manual": manual, "total": total, "ok_pct": pct}


def _result_table_html(df) -> str:
    import pandas as pd
    from app.display_labels import display_label, format_qty

    cols = [c for c in df.columns if c not in {"confidence", "status", "raw_text", "qty_uom_source"}]
    header = "<th>№</th>" + "".join(f"<th>{display_label(c)}</th>" for c in cols)
    rows_html = []
    for row_num, (_, row) in enumerate(df.iterrows(), start=1):
        status = row.get("status", "")
        cells = f"<td>{row_num}</td>" + "".join(
            f"<td>{format_qty(row[c]) if c == 'qty' else ('' if pd.isna(row[c]) else row[c])}</td>"
            for c in cols
        )
        rows_html.append(f'<tr data-status="{status}">{cells}</tr>')
    return (
        '<table class="table" id="result-table">'
        f"<thead><tr>{header}</tr></thead>"
        f'<tbody>{"".join(rows_html)}</tbody></table>'
    )

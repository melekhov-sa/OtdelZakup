"""Web routes for internal catalog (Наша номенклатура) CRUD and per-row item selection."""

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import InternalItem, NomenclatureFolder, SupplierInternalMatch
from app.product_type_matcher import get_item_types_for_ui
from app.trace import load_traces, save_traces

# ── In-memory sync task registry ─────────────────────────────────────────────
# Each entry: {"status": "running"|"complete"|"error", "done": int, "total": int,
#              "result": dict|None, "error": str|None}
_sync_tasks: dict[str, dict] = {}


def _run_sync_task(task_id: str, data: dict) -> None:
    """Background thread: run sync_from_1c and report progress via _sync_tasks."""
    session = get_db_session()
    try:
        from app.sync_1c import sync_from_1c

        def _progress(done: int, total: int) -> None:
            _sync_tasks[task_id]["done"]  = done
            _sync_tasks[task_id]["total"] = total

        result = sync_from_1c(data, session, progress_cb=_progress)
        _sync_tasks[task_id]["status"] = "complete"
        _sync_tasks[task_id]["result"] = result
    except Exception as exc:
        _sync_tasks[task_id]["status"] = "error"
        _sync_tasks[task_id]["error"]  = str(exc)
    finally:
        session.close()

internal_item_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── Internal catalog CRUD ─────────────────────────────────────────────────────


@internal_item_router.get("/internal-items", response_class=HTMLResponse)
async def internal_items_list(request: Request, q: str = ""):
    session = get_db_session()
    try:
        query = session.query(InternalItem).order_by(InternalItem.id)
        items = query.all()
        if q:
            q_lower = q.lower()
            items = [
                it for it in items
                if q_lower in (it.name or "").lower()
                or q_lower in (it.item_type or "").lower()
                or q_lower in (it.size or "").lower()
                or q_lower in (it.standard_text or "").lower()
            ]
        return templates.TemplateResponse(
            "internal_items_list.html",
            {"request": request, "items": items, "q": q},
        )
    finally:
        session.close()


@internal_item_router.get("/internal-items/new", response_class=HTMLResponse)
async def internal_item_new(request: Request):
    return templates.TemplateResponse(
        "internal_item_form.html",
        {"request": request, "item": None, "item_types": get_item_types_for_ui(), "is_edit": False},
    )


# ── 1C sync ───────────────────────────────────────────────────────────────────


@internal_item_router.get("/internal-items/sync-1c", response_class=HTMLResponse)
async def sync_1c_page(request: Request):
    return templates.TemplateResponse(
        "sync_1c.html",
        {"request": request, "result": None, "error": None},
    )


@internal_item_router.post("/internal-items/sync-1c")
async def sync_1c_upload(
    folders_file: UploadFile = File(...),
    items_file: UploadFile = File(...),
):
    """Start async sync task; returns {task_id, total} for progress polling."""
    try:
        folders_data = json.loads(await folders_file.read())
        items_data   = json.loads(await items_file.read())
    except Exception as exc:
        return JSONResponse({"error": f"Ошибка разбора JSON: {exc}"}, status_code=400)

    if not isinstance(folders_data, list):
        return JSONResponse({"error": "Файл папок должен содержать JSON-массив"}, status_code=400)
    if not isinstance(items_data, list):
        return JSONResponse({"error": "Файл номенклатуры должен содержать JSON-массив"}, status_code=400)

    task_id = uuid.uuid4().hex[:10]
    total   = len(items_data)
    _sync_tasks[task_id] = {"status": "running", "done": 0, "total": total, "result": None, "error": None}

    t = threading.Thread(
        target=_run_sync_task,
        args=(task_id, {"folders": folders_data, "items": items_data}),
        daemon=True,
    )
    t.start()
    return JSONResponse({"task_id": task_id, "total": total})


@internal_item_router.get("/internal-items/sync-1c/status/{task_id}")
async def sync_1c_status(task_id: str):
    """Return current progress of a sync task."""
    task = _sync_tasks.get(task_id)
    if task is None:
        return JSONResponse({"error": "Задача не найдена"}, status_code=404)
    return JSONResponse(task)


# ── Folder priorities ─────────────────────────────────────────────────────────


@internal_item_router.get("/internal-items/folders", response_class=HTMLResponse)
async def folder_priorities_page(request: Request):
    session = get_db_session()
    try:
        folders = (
            session.query(NomenclatureFolder)
            .order_by(NomenclatureFolder.folder_path, NomenclatureFolder.folder_name)
            .all()
        )
        return templates.TemplateResponse(
            "folder_priorities.html",
            {"request": request, "folders": folders},
        )
    finally:
        session.close()


@internal_item_router.post("/internal-items/folders/save-priorities", response_class=HTMLResponse)
async def save_folder_priorities(request: Request):
    form = await request.form()
    priorities: dict[str, int | None] = {}
    for key, value in form.items():
        if key.startswith("priority_"):
            uid = key[len("priority_"):]
            val = str(value).strip()
            priorities[uid] = int(val) if val.isdigit() and int(val) > 0 else None
    session = get_db_session()
    try:
        from app.sync_1c import update_folder_priorities
        updated = update_folder_priorities(priorities, session)
        folders = (
            session.query(NomenclatureFolder)
            .order_by(NomenclatureFolder.folder_path, NomenclatureFolder.folder_name)
            .all()
        )
        return templates.TemplateResponse(
            "folder_priorities.html",
            {"request": request, "folders": folders, "saved": updated},
        )
    finally:
        session.close()


# ── Smart parse API endpoints (must be before {item_id} routes) ──────────────


@internal_item_router.post("/internal-items/parse")
async def internal_item_parse_api(name_full: str = Form(...)):
    """Parse a single name string and return extracted fields as JSON."""
    from app.item_parser import parse_internal_item_name
    result = parse_internal_item_name(name_full)
    return JSONResponse(result)


@internal_item_router.post("/internal-items/bulk-preview")
async def internal_item_bulk_preview_api(names_text: str = Form(...)):
    """Parse multiple names (one per line) and return preview as JSON."""
    from app.item_parser import bulk_parse
    names = names_text.splitlines()
    results = bulk_parse(names, skip_empty=True, dedup=False)
    return JSONResponse({"items": results, "total": len(results)})


@internal_item_router.post("/internal-items/bulk-import")
async def internal_item_bulk_import_api(names_text: str = Form(...)):
    """Create InternalItem records for each parsed name."""
    from app.item_parser import bulk_parse
    names = names_text.splitlines()
    results = bulk_parse(names, skip_empty=True, dedup=True)
    session = get_db_session()
    try:
        from app.standard_normalizer import standard_key_from_text
        from app.matching.canonicalize import compute_canonical_key
        created = 0
        for r in results:
            std_text = r["standard_text"] or None
            item = InternalItem(
                name=r["name_full"],
                name_full=r["name_full"],
                item_type=r["item_type"] or None,
                size=r["size"] or None,
                diameter=r["diameter"] or None,
                length=r["length"] or None,
                standard_text=std_text,
                standard_key=standard_key_from_text(std_text) if std_text else None,
                strength_class=r["strength_class"] or None,
                material_coating=r["material_coating"] or None,
                parse_status=r["parse_status"],
                parse_reason=r["parse_reason"] or None,
                is_active=True,
            )
            session.add(item)
            session.flush()
            item.canonical_key = compute_canonical_key(item)
            created += 1
        session.commit()
        # Rebuild MinHash index after bulk import
        from app.matching.minhash_index import is_index_ready, rebuild_index
        if is_index_ready():
            all_items = session.query(InternalItem).filter_by(is_active=True).all()
            rebuild_index(all_items)
        return JSONResponse({"ok": True, "created": created})
    finally:
        session.close()


# ── CRUD with name_full + auto-parse ─────────────────────────────────────────


@internal_item_router.post("/internal-items/create", response_class=HTMLResponse)
async def internal_item_create(
    request: Request,
    name: str = Form(...),
    name_full: str = Form(default=""),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    from app.standard_normalizer import standard_key_from_text
    parse_status = None
    parse_reason = None
    p = {}
    if name_full.strip():
        from app.item_parser import parse_internal_item_name
        p = parse_internal_item_name(name_full.strip())
        parse_status = p["parse_status"]
        parse_reason = p["parse_reason"] or None

    # Use form values when provided; fall back to auto-parsed values
    final_item_type   = item_type.strip()        or p.get("item_type", "")        or None
    final_size        = size.strip()             or p.get("size", "")             or None
    final_diameter    = diameter.strip()         or p.get("diameter", "")         or None
    final_length      = length.strip()           or p.get("length", "")           or None
    final_strength    = strength_class.strip()   or p.get("strength_class", "")   or None
    final_coating     = material_coating.strip() or p.get("material_coating", "") or None
    std_text          = standard_text.strip()    or p.get("standard_text", "")    or None

    session = get_db_session()
    try:
        from app.matching.canonicalize import compute_canonical_key
        std_key_val = standard_key_from_text(std_text) if std_text else None
        item = InternalItem(
            name=name,
            name_full=name_full.strip() or None,
            item_type=final_item_type,
            size=final_size,
            diameter=final_diameter,
            length=final_length,
            standard_text=std_text,
            standard_key=std_key_val,
            strength_class=final_strength,
            material_coating=final_coating,
            parse_status=parse_status,
            parse_reason=parse_reason,
            is_active=True,
        )
        session.add(item)
        session.flush()  # get item.id before computing key
        item.canonical_key = compute_canonical_key(item)
        session.commit()
        # Update MinHash index
        from app.matching.minhash_index import add_to_index, is_index_ready
        if is_index_ready():
            add_to_index(item)
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.get("/internal-items/{item_id}/edit", response_class=HTMLResponse)
async def internal_item_edit(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return RedirectResponse(url="/internal-items", status_code=303)
        return templates.TemplateResponse(
            "internal_item_form.html",
            {"request": request, "item": item, "item_types": get_item_types_for_ui(), "is_edit": True},
        )
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/update", response_class=HTMLResponse)
async def internal_item_update(
    request: Request,
    item_id: int,
    name: str = Form(...),
    name_full: str = Form(default=""),
    item_type: str = Form(default=""),
    size: str = Form(default=""),
    diameter: str = Form(default=""),
    length: str = Form(default=""),
    standard_text: str = Form(default=""),
    strength_class: str = Form(default=""),
    material_coating: str = Form(default=""),
):
    from app.standard_normalizer import standard_key_from_text
    parse_status = None
    parse_reason = None
    p = {}
    if name_full.strip():
        from app.item_parser import parse_internal_item_name
        p = parse_internal_item_name(name_full.strip())
        parse_status = p["parse_status"]
        parse_reason = p["parse_reason"] or None

    # Use form values when provided; fall back to auto-parsed values
    final_item_type   = item_type.strip()        or p.get("item_type", "")        or None
    final_size        = size.strip()             or p.get("size", "")             or None
    final_diameter    = diameter.strip()         or p.get("diameter", "")         or None
    final_length      = length.strip()           or p.get("length", "")           or None
    final_strength    = strength_class.strip()   or p.get("strength_class", "")   or None
    final_coating     = material_coating.strip() or p.get("material_coating", "") or None
    std_text          = standard_text.strip()    or p.get("standard_text", "")    or None

    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return RedirectResponse(url="/internal-items", status_code=303)
        item.name = name
        item.name_full = name_full.strip() or None
        item.item_type = final_item_type
        item.size = final_size
        item.diameter = final_diameter
        item.length = final_length
        item.standard_text = std_text
        item.standard_key = standard_key_from_text(std_text) if std_text else None
        item.strength_class = final_strength
        item.material_coating = final_coating
        item.parse_status = parse_status
        item.parse_reason = parse_reason
        from app.matching.canonicalize import compute_canonical_key
        item.canonical_key = compute_canonical_key(item)
        session.commit()
        # Update MinHash index
        from app.matching.minhash_index import add_to_index, is_index_ready
        if is_index_ready():
            add_to_index(item)
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.post("/internal-items/recalculate-canonical-keys")
async def recalculate_canonical_keys():
    """Batch recalculate canonical_key for all internal items.

    Returns JSON {"ok": true, "updated": N, "total": M}.
    """
    from app.matching.canonicalize import compute_canonical_key
    session = get_db_session()
    try:
        items = session.query(InternalItem).all()
        updated = 0
        for item in items:
            new_ck = compute_canonical_key(item)
            if item.canonical_key != new_ck:
                item.canonical_key = new_ck
                updated += 1
        session.commit()
        return JSONResponse({"ok": True, "updated": updated, "total": len(items)})
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/toggle", response_class=HTMLResponse)
async def internal_item_toggle(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is not None:
            item.is_active = not item.is_active
            session.commit()
            # Update MinHash index: add if now active, remove if deactivated
            from app.matching.minhash_index import add_to_index, remove_from_index, is_index_ready
            if is_index_ready():
                if item.is_active:
                    add_to_index(item)
                else:
                    remove_from_index(item.id)
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


@internal_item_router.post("/internal-items/{item_id}/delete", response_class=HTMLResponse)
async def internal_item_delete(request: Request, item_id: int):
    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is not None:
            session.delete(item)
            session.commit()
            # Remove from MinHash index
            from app.matching.minhash_index import remove_from_index, is_index_ready
            if is_index_ready():
                remove_from_index(item_id)
        return RedirectResponse(url="/internal-items", status_code=303)
    finally:
        session.close()


# ── Confirm suggested match ───────────────────────────────────────────────────


@internal_item_router.post("/files/{file_id}/rows/{row_number}/confirm-match")
async def confirm_match(
    file_id: str,
    row_number: int,
    remember: bool = Form(default=True),
):
    """Confirm a SUGGESTED match for a row (mode → CONFIRMED).

    If remember=True, saves the fingerprint→item mapping to SupplierInternalMatch.
    Returns JSON {ok: true, name: str, mode: str}.
    """
    traces = load_traces(file_id)
    if not traces or row_number < 1 or row_number > len(traces):
        return JSONResponse({"ok": False, "error": "Строка не найдена"}, status_code=404)

    trace = traces[row_number - 1]
    matching = trace.get("matching", {})
    item_id = matching.get("internal_item_id")

    if not item_id:
        return JSONResponse({"ok": False, "error": "Нет предложенной позиции"}, status_code=400)

    session = get_db_session()
    try:
        item = session.get(InternalItem, item_id)
        if item is None:
            return JSONResponse({"ok": False, "error": "Позиция не найдена"}, status_code=404)

        # Update trace
        matching["mode"] = "CONFIRMED"
        matching["selected_name"] = item.name
        matching["selected_item_id"] = item_id
        trace["matching"] = matching
        save_traces(file_id, traces)

        # Save to memory if requested
        if remember:
            fp = matching.get("fingerprint", "")
            if fp:
                existing = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
                if existing:
                    existing.internal_item_id = item_id
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(SupplierInternalMatch(fingerprint=fp, internal_item_id=item_id))
                session.commit()

        return JSONResponse({"ok": True, "name": item.name, "mode": "CONFIRMED"})
    finally:
        session.close()


# ── Per-row item selection ────────────────────────────────────────────────────


@internal_item_router.get("/files/{file_id}/rows/{row_number}/select-internal", response_class=HTMLResponse)
async def select_internal_get(request: Request, file_id: str, row_number: int):
    """Show form for selecting internal catalog item for a specific result row."""
    traces = load_traces(file_id)
    if traces is None or row_number < 1 or row_number > len(traces):
        return RedirectResponse(url="/", status_code=303)

    trace = traces[row_number - 1]
    matching = trace.get("matching", {})
    candidates = matching.get("candidates", [])
    minhash_candidates = matching.get("minhash_candidates", [])

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).order_by(InternalItem.name).all()
        return templates.TemplateResponse(
            "select_internal.html",
            {
                "request": request,
                "file_id": file_id,
                "row_number": row_number,
                "trace": trace,
                "candidates": candidates,
                "minhash_candidates": minhash_candidates,
                "all_items": all_items,
                "current_match": matching.get("selected_name", "") or matching.get("candidates", [{}])[0].get("name", "") if matching.get("source") != "none" else "",
            },
        )
    finally:
        session.close()


@internal_item_router.post("/files/{file_id}/rows/{row_number}/select-internal-item")
async def select_internal_item_post(
    request: Request,
    file_id: str,
    row_number: int,
    internal_item_id: int = Form(...),
    remember: bool = Form(default=False),
):
    """Save manual internal item selection for a result row.

    If remember=True, also persists the fingerprint→item mapping in SupplierInternalMatch.
    Returns JSON {ok: true, name: str}.
    """
    session = get_db_session()
    try:
        item = session.get(InternalItem, internal_item_id)
        if item is None:
            return JSONResponse({"ok": False, "error": "Позиция не найдена"}, status_code=404)

        # Update trace
        traces = load_traces(file_id)
        if traces and 1 <= row_number <= len(traces):
            trace = traces[row_number - 1]
            if "matching" not in trace:
                trace["matching"] = {}
            trace["matching"]["selected_name"] = item.name
            trace["matching"]["selected_item_id"] = internal_item_id
            trace["matching"]["source"] = "manual"
            save_traces(file_id, traces)

        # Save fingerprint to memory if requested
        if remember:
            matching = (traces[row_number - 1].get("matching", {}) if traces else {})
            fp = matching.get("fingerprint", "")
            if fp:
                existing = session.query(SupplierInternalMatch).filter_by(fingerprint=fp).first()
                if existing:
                    existing.internal_item_id = internal_item_id
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(SupplierInternalMatch(
                        fingerprint=fp,
                        internal_item_id=internal_item_id,
                    ))
                session.commit()

        return JSONResponse({"ok": True, "name": item.name, "item_id": internal_item_id})
    finally:
        session.close()

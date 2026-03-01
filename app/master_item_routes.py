"""Routes for MasterItem (Объединение номенклатуры) — HTML UI + JSON API."""

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import InternalItem, MasterItem, MasterItemMember

master_item_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── Helpers ───────────────────────────────────────────────────────────────────


def _member_count(session, master_item_id: int) -> int:
    return session.query(MasterItemMember).filter_by(master_item_id=master_item_id).count()


def _find_master_by_guid(session, onec_guid: str) -> MasterItem | None:
    """Return the master_item that already contains onec_guid, or None."""
    mem = session.query(MasterItemMember).filter_by(onec_guid=onec_guid).first()
    if mem is None:
        return None
    return session.get(MasterItem, mem.master_item_id)


def _members_with_items(session, master_item_id: int) -> list[dict]:
    """Return member dicts enriched with current InternalItem data."""
    members = (
        session.query(MasterItemMember)
        .filter_by(master_item_id=master_item_id)
        .order_by(MasterItemMember.is_primary.desc(), MasterItemMember.created_at)
        .all()
    )
    result = []
    for m in members:
        # Try to find current item by uid_1c (any active)
        item = (
            session.query(InternalItem)
            .filter(InternalItem.uid_1c == m.onec_guid, InternalItem.is_active.is_(True))
            .first()
        )
        result.append({
            "id": m.id,
            "onec_guid": m.onec_guid,
            "is_primary": m.is_primary,
            "name_original": m.name_original or (item.name if item else m.onec_guid),
            "item_id": item.id if item else None,
            "folder_path": (item.folder_path or "") if item else "",
            "is_active_item": item is not None,
        })
    return result


# ── HTML routes ───────────────────────────────────────────────────────────────


@master_item_router.get("/catalog/master-items", response_class=HTMLResponse)
async def master_items_list(request: Request, q: str = ""):
    session = get_db_session()
    try:
        query = session.query(MasterItem).order_by(MasterItem.id)
        groups = query.all()
        if q:
            q_low = q.lower()
            groups = [g for g in groups if q_low in (g.name or "").lower()]
        groups_with_counts = [
            {"group": g, "count": _member_count(session, g.id)}
            for g in groups
        ]
        return templates.TemplateResponse(
            "master_items.html",
            {"request": request, "groups": groups_with_counts, "q": q},
        )
    finally:
        session.close()


@master_item_router.post("/catalog/master-items", response_class=HTMLResponse)
async def master_items_create(
    request: Request,
    name: str = Form(...),
    description: str = Form(default=""),
):
    name = name.strip()
    if not name:
        session = get_db_session()
        try:
            groups_with_counts = [
                {"group": g, "count": _member_count(session, g.id)}
                for g in session.query(MasterItem).order_by(MasterItem.id).all()
            ]
            return templates.TemplateResponse(
                "master_items.html",
                {"request": request, "groups": groups_with_counts, "q": "",
                 "error": "Название группы не может быть пустым."},
            )
        finally:
            session.close()

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        mi = MasterItem(
            name=name,
            description=description.strip() or None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        session.add(mi)
        session.commit()
        return RedirectResponse(f"/catalog/master-items/{mi.id}", status_code=303)
    finally:
        session.close()


@master_item_router.get("/catalog/master-items/{master_id}", response_class=HTMLResponse)
async def master_item_detail(request: Request, master_id: int):
    session = get_db_session()
    try:
        mi = session.get(MasterItem, master_id)
        if mi is None:
            return HTMLResponse("Группа не найдена", status_code=404)
        members = _members_with_items(session, master_id)
        return templates.TemplateResponse(
            "master_item_detail.html",
            {"request": request, "mi": mi, "members": members},
        )
    finally:
        session.close()


@master_item_router.post("/catalog/master-items/{master_id}/delete", response_class=HTMLResponse)
async def master_item_delete(master_id: int):
    session = get_db_session()
    try:
        mi = session.get(MasterItem, master_id)
        if mi:
            session.query(MasterItemMember).filter_by(master_item_id=master_id).delete()
            session.delete(mi)
            session.commit()
        return RedirectResponse("/catalog/master-items", status_code=303)
    finally:
        session.close()


@master_item_router.post("/catalog/master-items/bulk-create", response_class=HTMLResponse)
async def master_items_bulk_create(
    request: Request,
    name: str = Form(...),
    guids: str = Form(default=""),  # comma-separated uid_1c values
):
    """Create a new master group from a list of comma-separated uid_1c GUIDs."""
    name = name.strip()
    guid_list = [g.strip() for g in guids.split(",") if g.strip()]
    if not name or not guid_list:
        return RedirectResponse("/catalog/master-items", status_code=303)

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        mi = MasterItem(name=name, is_active=True, created_at=now, updated_at=now)
        session.add(mi)
        session.flush()

        for i, guid in enumerate(guid_list):
            # Skip if already in another group
            if _find_master_by_guid(session, guid):
                continue
            item = session.query(InternalItem).filter(InternalItem.uid_1c == guid).first()
            session.add(MasterItemMember(
                master_item_id=mi.id,
                onec_guid=guid,
                name_original=item.name if item else guid,
                is_primary=(i == 0),
                created_at=now,
            ))
        session.commit()
        return RedirectResponse(f"/catalog/master-items/{mi.id}", status_code=303)
    finally:
        session.close()


# ── JSON API ──────────────────────────────────────────────────────────────────


@master_item_router.get("/api/master-items/export")
async def api_master_items_export():
    """Export all active master groups with their members as JSON."""
    session = get_db_session()
    try:
        groups = session.query(MasterItem).filter_by(is_active=True).order_by(MasterItem.id).all()
        result = []
        for mi in groups:
            members_raw = (
                session.query(MasterItemMember)
                .filter_by(master_item_id=mi.id)
                .all()
            )
            members_out = []
            for m in members_raw:
                item = (
                    session.query(InternalItem)
                    .filter(InternalItem.uid_1c == m.onec_guid)
                    .first()
                )
                members_out.append({
                    "onec_guid": m.onec_guid,
                    "name": m.name_original or (item.name if item else m.onec_guid),
                    "is_primary": m.is_primary,
                })
            result.append({
                "master_id": mi.id,
                "master_name": mi.name,
                "description": mi.description or "",
                "members": members_out,
            })
        return JSONResponse(result)
    finally:
        session.close()


@master_item_router.get("/api/master-items")
async def api_master_items_list():
    session = get_db_session()
    try:
        groups = session.query(MasterItem).filter_by(is_active=True).order_by(MasterItem.id).all()
        return JSONResponse([
            {
                "id": mi.id,
                "name": mi.name,
                "description": mi.description or "",
                "member_count": _member_count(session, mi.id),
                "created_at": mi.created_at.isoformat() if mi.created_at else None,
            }
            for mi in groups
        ])
    finally:
        session.close()


@master_item_router.post("/api/master-items")
async def api_master_items_create(request: Request):
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name is required"}, status_code=400)
    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        mi = MasterItem(
            name=name,
            description=(body.get("description") or "").strip() or None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        session.add(mi)
        session.commit()
        return JSONResponse({"id": mi.id, "name": mi.name}, status_code=201)
    finally:
        session.close()


@master_item_router.get("/api/master-items/{master_id}")
async def api_master_item_get(master_id: int):
    session = get_db_session()
    try:
        mi = session.get(MasterItem, master_id)
        if mi is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        members = _members_with_items(session, master_id)
        return JSONResponse({
            "id": mi.id,
            "name": mi.name,
            "description": mi.description or "",
            "is_active": mi.is_active,
            "members": members,
        })
    finally:
        session.close()


@master_item_router.post("/api/master-items/{master_id}/add")
async def api_master_item_add_member(master_id: int, request: Request):
    """Add a member (by onec_guid) to a master group.

    Body: {"onec_guid": "...", "is_primary": false}
    The onec_guid must not already belong to another master group.
    """
    body = await request.json()
    onec_guid = (body.get("onec_guid") or "").strip()
    if not onec_guid:
        return JSONResponse({"error": "onec_guid is required"}, status_code=400)

    session = get_db_session()
    try:
        mi = session.get(MasterItem, master_id)
        if mi is None:
            return JSONResponse({"error": "master group not found"}, status_code=404)

        # Check global uniqueness: guid in another group?
        existing_master = _find_master_by_guid(session, onec_guid)
        if existing_master and existing_master.id != master_id:
            return JSONResponse(
                {"error": f"Позиция уже входит в группу «{existing_master.name}» (id={existing_master.id})"},
                status_code=409,
            )

        # Check within this group
        already = (
            session.query(MasterItemMember)
            .filter_by(master_item_id=master_id, onec_guid=onec_guid)
            .first()
        )
        if already:
            return JSONResponse({"error": "Позиция уже в этой группе"}, status_code=409)

        item = session.query(InternalItem).filter(InternalItem.uid_1c == onec_guid).first()
        is_primary = bool(body.get("is_primary", False))

        mem = MasterItemMember(
            master_item_id=master_id,
            onec_guid=onec_guid,
            name_original=item.name if item else onec_guid,
            is_primary=is_primary,
            created_at=datetime.now(timezone.utc),
        )
        session.add(mem)
        session.commit()
        return JSONResponse({"ok": True, "member_id": mem.id})
    finally:
        session.close()


@master_item_router.delete("/api/master-items/{master_id}/remove/{onec_guid:path}")
async def api_master_item_remove_member(master_id: int, onec_guid: str):
    """Remove a member by onec_guid from a master group."""
    session = get_db_session()
    try:
        deleted = (
            session.query(MasterItemMember)
            .filter_by(master_item_id=master_id, onec_guid=onec_guid)
            .delete()
        )
        session.commit()
        if not deleted:
            return JSONResponse({"error": "Позиция не найдена в этой группе"}, status_code=404)
        return JSONResponse({"ok": True})
    finally:
        session.close()


@master_item_router.post("/api/master-items/{master_id}/set-primary")
async def api_master_item_set_primary(master_id: int, request: Request):
    """Set a member as the primary (основная) for this master group.

    Body: {"onec_guid": "..."}
    Clears is_primary on all other members first.
    """
    body = await request.json()
    onec_guid = (body.get("onec_guid") or "").strip()
    if not onec_guid:
        return JSONResponse({"error": "onec_guid is required"}, status_code=400)

    session = get_db_session()
    try:
        # Clear all primaries in this group
        session.query(MasterItemMember).filter_by(master_item_id=master_id).update(
            {"is_primary": False}
        )
        updated = (
            session.query(MasterItemMember)
            .filter_by(master_item_id=master_id, onec_guid=onec_guid)
            .update({"is_primary": True})
        )
        session.commit()
        if not updated:
            return JSONResponse({"error": "Позиция не найдена"}, status_code=404)
        return JSONResponse({"ok": True})
    finally:
        session.close()


@master_item_router.get("/api/catalog/items/search")
async def api_catalog_items_search(q: str = "", limit: int = 20):
    """Search active InternalItems by name for the master-item add-member UI."""
    session = get_db_session()
    try:
        items = (
            session.query(InternalItem)
            .filter(InternalItem.is_active.is_(True))
            .order_by(InternalItem.id)
            .all()
        )
        if q:
            q_low = q.lower()
            items = [it for it in items if q_low in (it.name or "").lower()]
        items = items[:limit]
        return JSONResponse([
            {
                "id": it.id,
                "name": it.name,
                "uid_1c": it.uid_1c or "",
                "folder_path": it.folder_path or "",
                "item_type": it.item_type or "",
            }
            for it in items
        ])
    finally:
        session.close()

"""Routes for /catalog/duplicates — automatic duplicate/analog analysis page."""

import csv
import io
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.catalog_duplicates import compute_duplicate_groups
from app.database import get_db_session

catalog_dup_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_REASON_LABELS = {
    "duplicate": "Дубликат",
    "analog":    "Аналог стандарта",
}


@catalog_dup_router.get("/catalog/duplicates", response_class=HTMLResponse)
async def catalog_duplicates_form(request: Request):
    """Show the analysis form without computed results."""
    return templates.TemplateResponse(
        "catalog_duplicates.html",
        {
            "request": request,
            "groups": None,
            "computed": False,
            "include_duplicates": True,
            "include_analogs": True,
            "q": "",
            "min_size": 2,
        },
    )


@catalog_dup_router.post("/catalog/duplicates", response_class=HTMLResponse)
async def catalog_duplicates_compute(request: Request):
    """Compute and render duplicate/analog groups."""
    form = await request.form()
    include_duplicates = bool(form.get("include_duplicates"))
    include_analogs = bool(form.get("include_analogs"))
    q = (form.get("q") or "").strip().lower()
    try:
        min_size = max(2, int(form.get("min_size") or 2))
    except (TypeError, ValueError):
        min_size = 2

    session = get_db_session()
    try:
        groups = compute_duplicate_groups(
            include_duplicates=include_duplicates,
            include_analogs=include_analogs,
            session=session,
        )
    finally:
        session.close()

    # Text filter: keep groups where parent or any child matches
    if q:
        groups = [
            g for g in groups
            if q in (g["parent"].name or "").lower()
            or any(q in (ci["child"].name or "").lower() for ci in g["child_info"])
        ]

    # Minimum group size filter
    if min_size > 2:
        groups = [g for g in groups if g["size"] >= min_size]

    return templates.TemplateResponse(
        "catalog_duplicates.html",
        {
            "request": request,
            "groups": groups,
            "computed": True,
            "include_duplicates": include_duplicates,
            "include_analogs": include_analogs,
            "q": q,
            "min_size": min_size,
            "total_groups": len(groups),
            "total_items": sum(g["size"] for g in groups),
            "reason_labels": _REASON_LABELS,
        },
    )


@catalog_dup_router.get("/api/catalog/duplicates/export")
async def catalog_duplicates_export(
    include_duplicates: bool = True,
    include_analogs: bool = True,
):
    """Export duplicate/analog groups as UTF-8 BOM CSV."""
    session = get_db_session()
    try:
        groups = compute_duplicate_groups(
            include_duplicates=include_duplicates,
            include_analogs=include_analogs,
            session=session,
        )
    finally:
        session.close()

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([
        "group_num", "role",
        "id", "name", "uid_1c", "item_type", "size", "standard_key",
        "folder_path", "folder_priority",
        "reason", "detail",
    ])

    for group_num, g in enumerate(groups, 1):
        parent = g["parent"]
        writer.writerow([
            group_num, "parent",
            parent.id, parent.name,
            parent.uid_1c or "", parent.item_type or "",
            parent.size or "", parent.standard_key or "",
            parent.folder_path or "", parent.folder_priority if parent.folder_priority is not None else "",
            "", "",
        ])
        for ci in g["child_info"]:
            child = ci["child"]
            writer.writerow([
                group_num, "child",
                child.id, child.name,
                child.uid_1c or "", child.item_type or "",
                child.size or "", child.standard_key or "",
                child.folder_path or "", child.folder_priority if child.folder_priority is not None else "",
                ci["reason"], ci["detail"],
            ])

    csv_bytes = out.getvalue().encode("utf-8-sig")
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=duplicates.csv"},
    )

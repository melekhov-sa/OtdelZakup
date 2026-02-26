"""Routes for managing the product type directory (product_type table)."""
import json
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import ProductType

product_type_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@product_type_router.get("/product-types", response_class=HTMLResponse)
async def product_types_list(request: Request):
    session = get_db_session()
    try:
        types = session.query(ProductType).order_by(ProductType.name).all()
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse(
            "product_types.html",
            {"request": request, "product_types": types, "saved": saved},
        )
    finally:
        session.close()


@product_type_router.post("/product-types/add")
async def product_type_add(request: Request, name: str = Form(...)):
    name = name.strip().lower()
    if name:
        session = get_db_session()
        try:
            existing = session.query(ProductType).filter_by(name=name).first()
            if not existing:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                session.add(ProductType(
                    name=name,
                    aliases_json="[]",
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ))
                session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    return RedirectResponse(url="/product-types?saved=1", status_code=303)


@product_type_router.post("/product-types/{type_id}/toggle")
async def product_type_toggle(request: Request, type_id: int):
    session = get_db_session()
    try:
        pt = session.get(ProductType, type_id)
        if pt is not None:
            pt.is_active = not pt.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/product-types", status_code=303)


@product_type_router.post("/product-types/{type_id}/delete")
async def product_type_delete(request: Request, type_id: int):
    session = get_db_session()
    try:
        pt = session.get(ProductType, type_id)
        if pt is not None:
            session.delete(pt)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/product-types", status_code=303)


@product_type_router.post("/product-types/{type_id}/add-alias")
async def product_type_add_alias(
    request: Request,
    type_id: int,
    alias: str = Form(...),
):
    alias = alias.strip().lower()
    if alias:
        session = get_db_session()
        try:
            pt = session.get(ProductType, type_id)
            if pt is not None:
                current = pt.aliases
                if alias not in current:
                    current.append(alias)
                    pt.aliases_json = json.dumps(current, ensure_ascii=False)
                    session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    return RedirectResponse(url="/product-types", status_code=303)


@product_type_router.post("/product-types/{type_id}/remove-alias/{alias_idx}")
async def product_type_remove_alias(
    request: Request,
    type_id: int,
    alias_idx: int,
):
    session = get_db_session()
    try:
        pt = session.get(ProductType, type_id)
        if pt is not None:
            current = pt.aliases
            if 0 <= alias_idx < len(current):
                current.pop(alias_idx)
                pt.aliases_json = json.dumps(current, ensure_ascii=False)
                session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/product-types", status_code=303)

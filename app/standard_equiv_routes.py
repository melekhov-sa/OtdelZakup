"""Routes for managing standard equivalents (standard_equivalents table)."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import StandardEquivalent

standard_equiv_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@standard_equiv_router.get("/settings/standard-equivalents", response_class=HTMLResponse)
async def std_equiv_list(request: Request):
    from app.matching.standard_analogs import canonical_to_display
    session = get_db_session()
    try:
        equivs = (
            session.query(StandardEquivalent)
            .order_by(StandardEquivalent.src_canonical, StandardEquivalent.dst_canonical)
            .all()
        )
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse(
            "standard_equivalents.html",
            {"request": request, "equivs": equivs, "saved": saved,
             "std_display": canonical_to_display},
        )
    finally:
        session.close()


@standard_equiv_router.post("/settings/standard-equivalents/add", response_class=HTMLResponse)
async def std_equiv_add(
    request: Request,
    src_canonical: str = Form(...),
    dst_canonical: str = Form(...),
    confidence: int = Form(default=100),
):
    from app.matching.standard_analogs import normalize_standard
    raw_src = src_canonical.strip()
    raw_dst = dst_canonical.strip()
    # Accept both canonical (GOST-7798-70) and display (ГОСТ 7798-70) forms
    src = normalize_standard(raw_src) or raw_src
    dst = normalize_standard(raw_dst) or raw_dst
    if src and dst and src != dst:
        session = get_db_session()
        try:
            existing = (
                session.query(StandardEquivalent)
                .filter_by(src_canonical=src, dst_canonical=dst)
                .first()
            )
            if not existing:
                session.add(StandardEquivalent(
                    src_canonical=src,
                    dst_canonical=dst,
                    confidence=max(0, min(100, confidence)),
                    is_active=True,
                ))
                session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    return RedirectResponse(url="/settings/standard-equivalents?saved=1", status_code=303)


@standard_equiv_router.post(
    "/settings/standard-equivalents/{equiv_id}/toggle", response_class=HTMLResponse
)
async def std_equiv_toggle(request: Request, equiv_id: int):
    session = get_db_session()
    try:
        eq = session.get(StandardEquivalent, equiv_id)
        if eq is not None:
            eq.is_active = not eq.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/settings/standard-equivalents", status_code=303)


@standard_equiv_router.post(
    "/settings/standard-equivalents/{equiv_id}/delete", response_class=HTMLResponse
)
async def std_equiv_delete(request: Request, equiv_id: int):
    session = get_db_session()
    try:
        eq = session.get(StandardEquivalent, equiv_id)
        if eq is not None:
            session.delete(eq)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/settings/standard-equivalents", status_code=303)

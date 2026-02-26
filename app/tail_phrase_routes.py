"""Routes for managing tail-phrase stop-words (system_tail_phrase table)."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import TailPhrase

tail_phrase_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@tail_phrase_router.get("/settings/tail-phrases", response_class=HTMLResponse)
async def tail_phrases_list(request: Request):
    session = get_db_session()
    try:
        phrases = session.query(TailPhrase).order_by(TailPhrase.id).all()
        saved = request.query_params.get("saved") == "1"
        return templates.TemplateResponse(
            "tail_phrases.html",
            {"request": request, "phrases": phrases, "saved": saved},
        )
    finally:
        session.close()


@tail_phrase_router.post("/settings/tail-phrases/add", response_class=HTMLResponse)
async def tail_phrase_add(
    request: Request,
    phrase: str = Form(...),
):
    phrase = phrase.strip()
    if phrase:
        session = get_db_session()
        try:
            existing = session.query(TailPhrase).filter_by(phrase=phrase).first()
            if not existing:
                session.add(TailPhrase(phrase=phrase, is_active=True))
                session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()
    return RedirectResponse(url="/settings/tail-phrases?saved=1", status_code=303)


@tail_phrase_router.post("/settings/tail-phrases/{phrase_id}/toggle", response_class=HTMLResponse)
async def tail_phrase_toggle(request: Request, phrase_id: int):
    session = get_db_session()
    try:
        p = session.get(TailPhrase, phrase_id)
        if p is not None:
            p.is_active = not p.is_active
            session.commit()
    finally:
        session.close()
    return RedirectResponse(url="/settings/tail-phrases", status_code=303)


@tail_phrase_router.post("/settings/tail-phrases/{phrase_id}/delete", response_class=HTMLResponse)
async def tail_phrase_delete(request: Request, phrase_id: int):
    session = get_db_session()
    try:
        p = session.get(TailPhrase, phrase_id)
        if p is not None:
            session.delete(p)
            session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()
    return RedirectResponse(url="/settings/tail-phrases", status_code=303)

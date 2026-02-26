"""Routes for system settings: auto-match thresholds."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

settings_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@settings_router.get("/settings/match", response_class=HTMLResponse)
async def match_settings_get(request: Request):
    from app.match_settings import load_match_settings
    settings = load_match_settings()
    saved = request.query_params.get("saved") == "1"
    return templates.TemplateResponse(
        "match_settings.html",
        {"request": request, "settings": settings, "saved": saved},
    )


@settings_router.post("/settings/match", response_class=HTMLResponse)
async def match_settings_post(
    request: Request,
    enable_auto_match: str = Form(default=""),
    enable_auto_match_memory: str = Form(default=""),
    auto_match_threshold: int = Form(default=90),
    suggest_threshold: int = Form(default=70),
    always_require_confirmation: str = Form(default=""),
):
    from app.match_settings import MatchSettings, save_match_settings

    settings = MatchSettings(
        enable_auto_match=bool(enable_auto_match),
        enable_auto_match_memory=bool(enable_auto_match_memory),
        auto_match_threshold=max(0, min(200, auto_match_threshold)),
        suggest_threshold=max(0, min(200, suggest_threshold)),
        always_require_confirmation=bool(always_require_confirmation),
    )
    save_match_settings(settings)
    return RedirectResponse("/settings/match?saved=1", status_code=303)

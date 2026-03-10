"""Routes for /system-quality — Quality Monitoring Dashboard and API."""
from __future__ import annotations

import dataclasses
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

quality_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@quality_router.get("/api/system/quality")
def quality_api(last_n: int = 50):
    """Return quality metrics as JSON."""
    from app.services.quality_service import compute_quality_metrics

    m = compute_quality_metrics(last_n=last_n)
    return JSONResponse({
        "parse_success_rate": m.parse_success_rate,
        "field_recognition_rate": m.field_recognition_rate,
        "size_recognition_rate": m.size_recognition_rate,
        "strength_recognition_rate": m.strength_recognition_rate,
        "coating_recognition_rate": m.coating_recognition_rate,
        "standard_recognition_rate": m.standard_recognition_rate,
        "auto_match_rate": m.auto_match_rate,
        "manual_match_rate": m.manual_match_rate,
        "match_accuracy": m.match_accuracy,
        "supplier_parse_success_rate": m.supplier_parse_success_rate,
        "supplier_auto_match_rate": m.supplier_auto_match_rate,
        "correction_rate": m.correction_rate,
        "full_auto_rate": m.full_auto_rate,
        "total_runs": m.total_runs,
        "total_client_lines": m.total_client_lines,
        "total_supplier_lines": m.total_supplier_lines,
        "total_feedbacks": m.total_feedbacks,
        "history": m.history,
    })


@quality_router.get("/system-quality", response_class=HTMLResponse)
def quality_dashboard(request: Request, last_n: int = 50):
    """Render quality monitoring dashboard."""
    from app.services.quality_service import compute_quality_metrics

    m = compute_quality_metrics(last_n=last_n)
    return templates.TemplateResponse("system_quality.html", {
        "request": request,
        "m": m,
        "last_n": last_n,
    })

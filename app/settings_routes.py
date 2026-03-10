"""Routes for system settings: auto-match thresholds, weights and penalties."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

settings_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@settings_router.get("/settings/match", response_class=HTMLResponse)
def match_settings_get(request: Request):
    from app.match_settings import load_match_settings
    settings = load_match_settings()
    saved = request.query_params.get("saved") == "1"
    return templates.TemplateResponse(
        "match_settings.html",
        {"request": request, "settings": settings, "saved": saved},
    )


@settings_router.post("/settings/match", response_class=HTMLResponse)
def match_settings_post(
    request: Request,
    enable_auto_match: str = Form(default=""),
    enable_auto_match_memory: str = Form(default=""),
    always_require_confirmation: str = Form(default=""),
    auto_match_threshold: int = Form(default=90),
    suggest_threshold: int = Form(default=70),
    auto_match_delta: int = Form(default=15),
    w_type: int = Form(default=40),
    w_size: int = Form(default=35),
    w_standard: int = Form(default=20),
    w_text: int = Form(default=5),
    p_type_mismatch: int = Form(default=60),
    p_diameter_mismatch: int = Form(default=100),
    p_standard_mismatch: int = Form(default=30),
    p_kit_mismatch: int = Form(default=60),
    # Auto-apply
    auto_apply_enabled: str = Form(default=""),
    auto_apply_jaccard_threshold: float = Form(default=0.55),
    # MinHash / LSH
    enable_minhash: str = Form(default=""),
    lsh_threshold: float = Form(default=0.3),
    num_perm: int = Form(default=128),
    minhash_top_k: int = Form(default=20),
    ngram_n: int = Form(default=4),
    use_type_buckets: str = Form(default=""),
    min_candidates_before_fallback: int = Form(default=5),
    minhash_filter_size: str = Form(default=""),
    min_display_score: int = Form(default=40),
    use_standard_analogs_in_main_match: str = Form(default=""),
):
    from app.match_settings import MatchSettings, save_match_settings

    def _clamp(v, lo, hi):
        return max(lo, min(hi, v))

    def _clampf(v, lo, hi):
        return max(lo, min(hi, float(v)))

    settings = MatchSettings(
        enable_auto_match=bool(enable_auto_match),
        enable_auto_match_memory=bool(enable_auto_match_memory),
        always_require_confirmation=bool(always_require_confirmation),
        auto_match_threshold=_clamp(auto_match_threshold, 0, 100),
        suggest_threshold=_clamp(suggest_threshold, 0, 100),
        auto_match_delta=_clamp(auto_match_delta, 0, 100),
        w_type=_clamp(w_type, 0, 100),
        w_size=_clamp(w_size, 0, 100),
        w_standard=_clamp(w_standard, 0, 100),
        w_text=_clamp(w_text, 0, 100),
        p_type_mismatch=_clamp(p_type_mismatch, 0, 200),
        p_diameter_mismatch=_clamp(p_diameter_mismatch, 0, 200),
        p_standard_mismatch=_clamp(p_standard_mismatch, 0, 200),
        p_kit_mismatch=_clamp(p_kit_mismatch, 0, 200),
        auto_apply_enabled=bool(auto_apply_enabled),
        auto_apply_jaccard_threshold=_clampf(auto_apply_jaccard_threshold, 0.0, 1.0),
        enable_minhash=bool(enable_minhash),
        lsh_threshold=_clampf(lsh_threshold, 0.1, 0.9),
        num_perm=_clamp(num_perm, 32, 512),
        minhash_top_k=_clamp(minhash_top_k, 5, 100),
        ngram_n=_clamp(ngram_n, 3, 6),
        use_type_buckets=bool(use_type_buckets),
        min_candidates_before_fallback=_clamp(min_candidates_before_fallback, 1, 50),
        minhash_filter_size=bool(minhash_filter_size),
        min_display_score=_clamp(min_display_score, 0, 99),
        use_standard_analogs_in_main_match=bool(use_standard_analogs_in_main_match),
    )
    save_match_settings(settings)

    # Rebuild MinHash index if enabled (settings may have changed)
    if settings.enable_minhash:
        from app.database import get_db_session
        from app.models import InternalItem
        from app.matching.minhash_index import rebuild_index
        session = get_db_session()
        try:
            items = session.query(InternalItem).filter_by(is_active=True).all()
            rebuild_index(
                items,
                num_perm=settings.num_perm,
                threshold=settings.lsh_threshold,
                ngram_n=settings.ngram_n,
                use_type_buckets=settings.use_type_buckets,
            )
        finally:
            session.close()

    return RedirectResponse("/settings/match?saved=1", status_code=303)


# ── Google Document AI settings ───────────────────────────────────────────────

_GOOGLE_OCR_KEYS = (
    "google_ocr_project_id",
    "google_ocr_location",
    "google_ocr_processor_id",
    "google_ocr_credentials_json",
    "google_ocr_save_raw",
)


def _load_google_ocr_settings() -> dict:
    from app.database import get_db_session  # noqa: PLC0415
    from app.models import SystemSetting  # noqa: PLC0415

    session = get_db_session()
    try:
        rows = (
            session.query(SystemSetting)
            .filter(SystemSetting.key.in_(_GOOGLE_OCR_KEYS))
            .all()
        )
        return {r.key: r.value or "" for r in rows}
    finally:
        session.close()


def _save_google_ocr_setting(key: str, value: str) -> None:
    from datetime import datetime, timezone  # noqa: PLC0415

    from app.database import get_db_session  # noqa: PLC0415
    from app.models import SystemSetting  # noqa: PLC0415

    session = get_db_session()
    try:
        row = session.query(SystemSetting).filter_by(key=key).first()
        if row is None:
            row = SystemSetting(key=key, value=value,
                                updated_at=datetime.now(timezone.utc))
            session.add(row)
        else:
            row.value = value
            row.updated_at = datetime.now(timezone.utc)
        session.commit()
    finally:
        session.close()


@settings_router.get("/settings/google-ocr", response_class=HTMLResponse)
def google_ocr_settings_get(request: Request):
    cfg = _load_google_ocr_settings()
    saved = request.query_params.get("saved") == "1"
    return templates.TemplateResponse(
        "google_ocr_settings.html",
        {"request": request, "cfg": cfg, "saved": saved},
    )


@settings_router.post("/settings/google-ocr", response_class=HTMLResponse)
def google_ocr_settings_post(
    request: Request,
    project_id: str = Form(default=""),
    location: str = Form(default=""),
    processor_id: str = Form(default=""),
    credentials_json: str = Form(default=""),
    save_raw: str = Form(default=""),
):
    _save_google_ocr_setting("google_ocr_project_id", project_id.strip())
    _save_google_ocr_setting("google_ocr_location", location.strip())
    _save_google_ocr_setting("google_ocr_processor_id", processor_id.strip())
    _save_google_ocr_setting("google_ocr_save_raw", "true" if save_raw else "false")

    # Credentials: empty submission means "keep existing value"
    if credentials_json.strip():
        _save_google_ocr_setting("google_ocr_credentials_json", credentials_json.strip())

    return RedirectResponse("/settings/google-ocr?saved=1", status_code=303)

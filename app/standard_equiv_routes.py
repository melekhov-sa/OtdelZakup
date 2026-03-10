"""Routes for managing standard equivalents (standard_equivalents table)."""
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session
from app.models import StandardEquivalent

standard_equiv_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@standard_equiv_router.get("/settings/standard-equivalents", response_class=HTMLResponse)
def std_equiv_list(request: Request):
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
def std_equiv_add(
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
def std_equiv_toggle(request: Request, equiv_id: int):
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
def std_equiv_delete(request: Request, equiv_id: int):
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


# ── Analog debug page ────────────────────────────────────────────────────────


@standard_equiv_router.get("/settings/analog-debug", response_class=HTMLResponse)
def analog_debug_page(request: Request):
    return templates.TemplateResponse("analog_debug.html", {"request": request, "result": None})


@standard_equiv_router.post("/settings/analog-debug", response_class=HTMLResponse)
def analog_debug_run(request: Request, raw_text: str = Form(...)):
    result = _run_analog_debug(raw_text.strip())
    return templates.TemplateResponse("analog_debug.html", {"request": request, "result": result, "raw_text": raw_text.strip()})


@standard_equiv_router.get("/api/analog-debug")
def analog_debug_api(q: str = ""):
    """JSON API for analog debug — same logic, machine-readable."""
    if not q.strip():
        return JSONResponse({"error": "empty query"}, status_code=400)
    return JSONResponse(_run_analog_debug(q.strip()))


def _run_analog_debug(raw_text: str) -> dict:
    """Run full analog pipeline and collect diagnostics at every step."""
    from app.extractors import extract_gost, extract_din, extract_iso, extract_item_type, extract_size
    from app.matching.standard_analogs import (
        normalize_standard, canonical_to_display,
        get_standard_analogs, build_analog_queries,
        _STD_PATTERNS,
    )
    from app.standard_normalizer import extract_standards
    from app.matching.normalizer import normalize_size

    steps: list[dict] = []

    # ── Step 1: Field extraction ──────────────────────────────────────────
    item_type = extract_item_type(raw_text)
    size_raw = extract_size(raw_text)
    size_norm = normalize_size(size_raw) if size_raw else ""
    gost = extract_gost(raw_text)
    din = extract_din(raw_text)
    iso = extract_iso(raw_text)
    steps.append({
        "name": "Извлечение полей из текста",
        "detail": {
            "item_type": item_type or "(не найден)",
            "size": size_raw or "(не найден)",
            "size_norm": size_norm or "(не найден)",
            "gost": gost or "(не найден)",
            "din": din or "(не найден)",
            "iso": iso or "(не найден)",
        },
    })

    # ── Step 2: Standard detection via extract_standards ──────────────────
    std_tokens = extract_standards(raw_text)
    steps.append({
        "name": "Поиск стандартов в тексте (extract_standards)",
        "detail": [
            {"system": t.system, "number": t.number, "key": t.key, "display": t.display}
            for t in std_tokens
        ] if std_tokens else "(стандартов не найдено)",
    })

    # ── Step 3: normalize_standard for each found field ───────────────────
    canonical_keys: list[dict] = []
    for label, val in [("gost", gost), ("din", din), ("iso", iso)]:
        if val:
            canon = normalize_standard(val)
            canonical_keys.append({"field": label, "raw": val, "canonical": canon or "(не распознано)"})
    for t in std_tokens:
        if not any(ck["canonical"] == t.key for ck in canonical_keys):
            canonical_keys.append({"field": "text_scan", "raw": t.display, "canonical": t.key})
    steps.append({
        "name": "Нормализация стандартов -> canonical key",
        "detail": canonical_keys if canonical_keys else "(нет стандартов для нормализации)",
    })

    # ── Step 4: DB lookup — get_standard_analogs for each key ─────────────
    all_analogs: list[dict] = []
    for ck in canonical_keys:
        key = ck["canonical"]
        if key.startswith("("):
            continue
        analogs = get_standard_analogs(key)
        all_analogs.append({
            "canonical": key,
            "display": canonical_to_display(key),
            "analogs_canonical": analogs,
            "analogs_display": [canonical_to_display(a) for a in analogs],
        })
    steps.append({
        "name": "Поиск аналогов в БД (standard_equivalents)",
        "detail": all_analogs if all_analogs else "(нет канонических ключей для поиска)",
    })

    # ── Step 4b: Show what's actually in the DB ───────────────────────────
    db_rows = []
    session = get_db_session()
    try:
        equivs = session.query(StandardEquivalent).filter_by(is_active=True).all()
        for eq in equivs:
            db_rows.append({
                "id": eq.id,
                "src": eq.src_canonical,
                "src_display": canonical_to_display(eq.src_canonical),
                "dst": eq.dst_canonical,
                "dst_display": canonical_to_display(eq.dst_canonical),
                "confidence": eq.confidence,
            })
    finally:
        session.close()
    steps.append({
        "name": f"Справочник аналогов (active={len(db_rows)} записей)",
        "detail": db_rows,
    })

    # ── Step 5: Regex match in raw text (_STD_PATTERNS) ───────────────────
    regex_matches = []
    for i, pat in enumerate(_STD_PATTERNS):
        for m in pat.finditer(raw_text):
            regex_matches.append({
                "pattern_idx": i,
                "pattern": pat.pattern[:80],
                "matched_text": m.group(0),
                "span": [m.start(), m.end()],
                "group1": m.group(1) if m.lastindex else "",
            })
    steps.append({
        "name": "Regex-поиск стандартов (_STD_PATTERNS)",
        "detail": regex_matches if regex_matches else "(regex ничего не нашёл)",
    })

    # ── Step 6: build_analog_queries ──────────────────────────────────────
    analog_queries = build_analog_queries(raw_text)
    steps.append({
        "name": "build_analog_queries -> перезаписанные запросы",
        "detail": [
            {
                "original_canonical": aq.original_canonical,
                "analog_canonical": aq.analog_canonical,
                "analog_display": aq.analog_display,
                "rewritten_text": aq.rewritten_text,
            }
            for aq in analog_queries
        ] if analog_queries else "(аналоговых запросов не построено)",
    })

    # ── Step 7: MinHash search (if index ready) ──────────────────────────
    minhash_results = []
    from app.matching.minhash_index import is_index_ready, query_index_with_scores
    if is_index_ready():
        direct_hits = query_index_with_scores(
            raw_text, item_type=item_type or "", size=size_norm or "",
            standard_text=gost or din or iso or "",
            top_k=5,
        )
        minhash_results.append({
            "query_type": "direct (прямой запрос)",
            "query_text": raw_text[:120],
            "hits": _enrich_hits(direct_hits),
        })
        for aq in analog_queries:
            aq_hits = query_index_with_scores(
                aq.rewritten_text, item_type=item_type or "", size=size_norm or "",
                standard_text="",
                top_k=5,
            )
            minhash_results.append({
                "query_type": f"analog: {aq.analog_display}",
                "query_text": aq.rewritten_text[:120],
                "hits": _enrich_hits(aq_hits),
            })
        steps.append({
            "name": "MinHash поиск (прямой + аналоги)",
            "detail": minhash_results,
        })
    else:
        steps.append({
            "name": "MinHash поиск",
            "detail": "(индекс не построен — is_index_ready()=False)",
        })

    return {"raw_text": raw_text, "steps": steps}


def _enrich_hits(hits: list[dict]) -> list[dict]:
    """Add item names to MinHash hits."""
    if not hits:
        return []
    from app.models import InternalItem
    session = get_db_session()
    try:
        ids = [h["item_id"] for h in hits]
        items = {it.id: it for it in session.query(InternalItem).filter(InternalItem.id.in_(ids)).all()}
        result = []
        for h in hits:
            it = items.get(h["item_id"])
            result.append({
                "item_id": h["item_id"],
                "jaccard": h["jaccard"],
                "name": it.name if it else "?",
                "standard_key": (it.standard_key or "") if it else "",
                "standard_text": (it.standard_text or "") if it else "",
                "size": (it.size or "") if it else "",
            })
        return result
    finally:
        session.close()

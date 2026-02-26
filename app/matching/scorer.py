"""Match scorer: score_match() returns 0..100 int with per-signal reasons + breakdown.

Score weights (max ~130 → capped at 100):
  size exact:        60 pts
  size permutation:  50 pts  (same numerics, different order)
  size close ≤2%:    45 pts  (each dimension within 2% relative tolerance)
  size first-tok:    10 pts  (only first dimension / diameter matches)
  type exact:        20 pts
  type partial:      10 pts  (one type contains the other)
  bonus (type+size): 10 pts  (extra when both exact → 90 total without standard)
  standard exact:    25 pts  (was 15; higher weight per spec)
  strength:           6 pts
  coating:            4 pts
  volume:             5 pts
  keywords (≤2):    ≤8 pts

Auto-extraction fallback:
  When item.size/item_type/item.standard_key are empty, values are extracted
  on-the-fly from item.name/item.name_full using the standard extractors.
  When row_dict["gost"/"iso"/"din"] are all empty, standards are extracted
  from row_dict["name_raw"] as fallback.

Decision thresholds (MatchSettings defaults):
  AUTO    ≥ 90
  SUGGEST ≥ 65
  NONE    < 65
"""
from __future__ import annotations

from app.matching.normalizer import (
    extract_name_keywords,
    extract_volume_ml,
    normalize_size,
    parse_size_tokens,
    sizes_close,
)

# Max points per component (used to normalise breakdown %)
_MAX_SIZE = 60
_MAX_TYPE = 20
_MAX_STD  = 25
_MAX_KW   = 8


def _get_item_effective_fields(item) -> tuple[str, str, str | None]:
    """Return (item_type, size, standard_key) — stored fields with on-the-fly fallback.

    When stored fields are empty the scorer extracts them from item.name/item.name_full
    using the same extractors as the transform pipeline.  This makes the scorer correct
    for catalog items that were imported without running the extractor (e.g., added via
    the manual create-form with only a name entered).
    """
    itype = str(item.item_type or "").strip().lower()
    isize = str(item.size or "").strip()
    istd  = item.standard_key

    if itype and isize and istd:
        return itype, isize, istd  # all populated — fast path

    name_text = ((item.name or "") + " " + (item.name_full or "")).strip()
    if not name_text:
        return itype, isize, istd

    from app.extractors import extract_item_type, extract_size  # noqa: PLC0415
    from app.standard_normalizer import extract_standards       # noqa: PLC0415

    if not itype:
        itype = (extract_item_type(name_text) or "").strip().lower()
    if not isize:
        isize = extract_size(name_text) or ""
    if not istd:
        stds = extract_standards(name_text)
        istd = stds[0].key if stds else None

    return itype, isize, istd


def _get_row_std_keys(row_dict: dict) -> set[str]:
    """Extract standard keys from a row dict.

    Primary source: row_dict["gost"], ["iso"], ["din"] columns.
    Fallback: scan row_dict["name_raw"] / ["name"] when all three columns are empty.
    """
    from app.standard_normalizer import extract_standards, standard_key_from_text  # noqa: PLC0415

    def _n(v: object) -> str:
        return str(v or "").strip().lower()

    r_std_keys: set[str] = set()
    for k in ("gost", "iso", "din"):
        val = _n(row_dict.get(k))
        if val:
            sk = standard_key_from_text(val)
            if sk:
                r_std_keys.add(sk)

    if not r_std_keys:
        r_text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
        if r_text:
            r_std_keys = {t.key for t in extract_standards(r_text)}

    return r_std_keys


def score_match(row_dict: dict, item) -> dict:
    """Score an InternalItem against extracted row fields.

    Returns:
        {
            "score":        int (0..100),
            "reasons":      list[str],   # positive match signals
            "warn_reasons": list[str],   # soft mismatches / caveats
            "breakdown":    dict,        # component → pct 0..100
        }
    """
    reasons: list[str] = []
    warn_reasons: list[str] = []
    pts_by_component: dict[str, int] = {}
    points = 0

    def _n(val) -> str:
        return str(val or "").strip().lower()

    # ── Get effective item fields (on-the-fly extraction fallback) ─────────
    item_type, item_size_raw, item_std_key = _get_item_effective_fields(item)

    # ── 1. Item type (20 pts) ──────────────────────────────────────────────
    r_type = _n(row_dict.get("item_type"))
    type_matched = False

    if r_type and item_type:
        if r_type == item_type:
            points += 20
            pts_by_component["type"] = 20
            type_matched = True
            reasons.append(f"тип: {r_type}")
        elif r_type in item_type or item_type in r_type:
            points += 10
            pts_by_component["type"] = 10
            type_matched = True
            reasons.append(f"тип (частично): {r_type}")
        else:
            pts_by_component["type"] = 0
            warn_reasons.append(f"тип не совпал: {r_type} ≠ {item_type}")

    # ── 2. Size (up to 60 pts) ─────────────────────────────────────────────
    r_size_raw = _n(row_dict.get("size"))
    size_matched_exact = False

    if r_size_raw and item_size_raw:
        r_norm = normalize_size(r_size_raw)
        i_norm = normalize_size(item_size_raw)

        if r_norm == i_norm:
            points += 60
            pts_by_component["size"] = 60
            size_matched_exact = True
            reasons.append(f"размер точно: {r_size_raw}")
        else:
            r_tok = parse_size_tokens(r_norm)
            i_tok = parse_size_tokens(i_norm)
            if r_tok and i_tok:
                if sorted(r_tok) == sorted(i_tok):
                    points += 50
                    pts_by_component["size"] = 50
                    reasons.append(
                        f"размер (переставлен): {r_size_raw} = {item_size_raw}"
                    )
                elif sizes_close(r_tok, i_tok, tol=0.02):
                    points += 45
                    pts_by_component["size"] = 45
                    warn_reasons.append(f"размер близкий: {r_size_raw} ≈ {item_size_raw}")
                elif r_tok[:1] == i_tok[:1]:
                    points += 10
                    pts_by_component["size"] = 10
                    warn_reasons.append(f"диаметр совпал: {r_tok[0]}")
                else:
                    pts_by_component["size"] = 0
                    warn_reasons.append(f"размер не совпал: {r_size_raw} ≠ {item_size_raw}")
    elif r_size_raw and not item_size_raw:
        pts_by_component["size"] = 0
        warn_reasons.append("в каталоге нет размера")
    # else: row has no size → size component not tracked

    # ── 3. Bonus: type AND size both matched exactly (10 pts) ─────────────
    if type_matched and size_matched_exact:
        points += 10

    # ── 4. Standard (25 pts) ──────────────────────────────────────────────
    r_std_keys = _get_row_std_keys(row_dict)

    if r_std_keys:
        i_standard = _n(item.standard_text)
        std_matched = False
        matched_std = ""

        if item_std_key and item_std_key in r_std_keys:
            std_matched = True
            matched_std = item_std_key
        elif i_standard:
            for k in ("gost", "iso", "din"):
                val = _n(row_dict.get(k))
                if val and (val in i_standard or i_standard in val):
                    std_matched = True
                    matched_std = val
                    break

        if std_matched:
            points += 25
            pts_by_component["standard"] = 25
            reasons.append(f"стандарт: {matched_std}")
        else:
            pts_by_component["standard"] = 0
            warn_reasons.append("стандарт не совпал")

    # ── 5. Strength class (6 pts) ─────────────────────────────────────────
    r_str = _n(row_dict.get("strength"))
    i_str = _n(item.strength_class)
    if r_str and i_str and r_str == i_str:
        points += 6
        reasons.append(f"класс прочности: {r_str}")

    # ── 6. Coating (4 pts) ────────────────────────────────────────────────
    r_coat = _n(row_dict.get("coating"))
    i_coat = _n(item.material_coating)
    if r_coat and i_coat and (r_coat in i_coat or i_coat in r_coat):
        points += 4
        reasons.append(f"покрытие: {r_coat}")

    # ── 7. Volume match (5 pts) ───────────────────────────────────────────
    r_text = _n(row_dict.get("name_raw") or row_dict.get("name") or "")
    i_text = _n((item.name or "") + " " + (item.name_full or ""))
    r_vol = extract_volume_ml(r_text)
    i_vol = extract_volume_ml(i_text)
    if r_vol and i_vol:
        if abs(r_vol - i_vol) < 1.0:
            points += 5
            reasons.append(f"объём: {int(r_vol)} мл")
        else:
            warn_reasons.append(f"объём разный: {int(r_vol)} ≠ {int(i_vol)} мл")

    # ── 8. Keyword overlap (up to 8 pts) ─────────────────────────────────
    if r_text and i_text:
        r_kw = extract_name_keywords(r_text)
        i_kw = extract_name_keywords(i_text)
        if r_kw and i_kw:
            # Exclude type words (scored in signal 1) and standard label words
            type_words = {w for w in (r_type, item_type) if w}
            std_words  = {"гост", "gost", "din", "iso", "исо"}
            exclude    = type_words | std_words
            r_kw_clean = r_kw - exclude
            i_kw_clean = i_kw - exclude
            common = r_kw_clean & i_kw_clean
            if common:
                kw_pts = min(8, len(common) * 4)
                points += kw_pts
                pts_by_component["keywords"] = kw_pts
                sample = sorted(common)[:3]
                reasons.append(f"слова: {', '.join(sample)}")

    # If no signal matched at all → score 0
    if not reasons and not warn_reasons:
        return {"score": 0, "reasons": [], "warn_reasons": [], "breakdown": {}}

    # ── Build breakdown as % of max per component ─────────────────────────
    breakdown: dict[str, int] = {}
    if "size"     in pts_by_component:
        breakdown["size"]     = round(pts_by_component["size"]     / _MAX_SIZE * 100)
    if "type"     in pts_by_component:
        breakdown["type"]     = round(pts_by_component["type"]     / _MAX_TYPE * 100)
    if "standard" in pts_by_component:
        breakdown["standard"] = round(pts_by_component["standard"] / _MAX_STD  * 100)
    if "keywords" in pts_by_component:
        breakdown["keywords"] = round(pts_by_component["keywords"] / _MAX_KW   * 100)

    return {
        "score":        min(100, max(0, points)),
        "reasons":      reasons,
        "warn_reasons": warn_reasons,
        "breakdown":    breakdown,
    }

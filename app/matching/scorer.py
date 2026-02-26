"""Match scorer: score_match() returns 0..100 int with per-signal reasons.

Score weights (max 128 → capped at 100):
  size exact:        60 pts
  size permutation:  50 pts  (same numerics, different order)
  size close ≤2%:    45 pts  (each dimension within 2% relative tolerance)
  type exact:        20 pts
  type+size bonus:   10 pts  (extra when both match exactly → 90 total)
  standard:          15 pts
  strength:           8 pts
  coating:            5 pts
  volume:            10 pts
  keywords (≤3):    ≤12 pts

Decision thresholds (MatchSettings defaults):
  AUTO  ≥ 90
  SUGGEST ≥ 65
  NONE  < 65
"""
from __future__ import annotations

from app.matching.normalizer import (
    extract_name_keywords,
    extract_volume_ml,
    normalize_size,
    parse_size_tokens,
    sizes_close,
)


def score_match(row_dict: dict, item) -> dict:
    """Score an InternalItem against extracted row fields.

    Returns:
        {
            "score":        int (0..100),
            "reasons":      list[str],   # positive match signals
            "warn_reasons": list[str],   # soft mismatches / caveats
        }
    """
    reasons: list[str] = []
    warn_reasons: list[str] = []
    points = 0

    def _n(val) -> str:
        return str(val or "").strip().lower()

    # ── 1. Item type (20 pts) ──────────────────────────────────────────────
    r_type = _n(row_dict.get("item_type"))
    i_type = _n(item.item_type)
    type_matched = False

    if r_type and i_type:
        if r_type == i_type:
            points += 20
            type_matched = True
            reasons.append(f"тип: {r_type}")
        elif r_type in i_type or i_type in r_type:
            points += 10
            type_matched = True
            reasons.append(f"тип (частично): {r_type}")

    # ── 2. Size (up to 60 pts) ─────────────────────────────────────────────
    r_size_raw = _n(row_dict.get("size"))
    i_size_raw = _n(item.size)
    size_matched_exact = False

    if r_size_raw and i_size_raw:
        r_norm = normalize_size(r_size_raw)
        i_norm = normalize_size(i_size_raw)

        if r_norm == i_norm:
            points += 60
            size_matched_exact = True
            reasons.append(f"размер точно: {r_size_raw}")
        else:
            r_tok = parse_size_tokens(r_norm)
            i_tok = parse_size_tokens(i_norm)
            if r_tok and i_tok:
                if sorted(r_tok) == sorted(i_tok):
                    # Same numerics, different sequence (e.g., 125x22.2x1.6 vs 125x1.6x22)
                    points += 50
                    reasons.append(
                        f"размер (переставлен): {r_size_raw} = {i_size_raw}"
                    )
                elif sizes_close(r_tok, i_tok, tol=0.02):
                    points += 45
                    warn_reasons.append(f"размер близкий: {r_size_raw} ≈ {i_size_raw}")
                elif r_tok[:1] == i_tok[:1]:
                    # Only the first token (diameter) matches
                    points += 10
                    warn_reasons.append(f"диаметр совпал: {r_tok[0]}")
    elif r_size_raw and not i_size_raw:
        warn_reasons.append("в каталоге нет размера")

    # ── 3. Bonus: type AND size both matched exactly (10 pts) ─────────────
    if type_matched and size_matched_exact:
        points += 10
        # reason implied by individual signals

    # ── 4. Standard (15 pts) ──────────────────────────────────────────────
    from app.standard_normalizer import standard_key_from_text

    r_std_keys: set[str] = set()
    for k in ("gost", "iso", "din"):
        val = _n(row_dict.get(k))
        if val:
            sk = standard_key_from_text(val)
            if sk:
                r_std_keys.add(sk)

    if r_std_keys:
        i_std_key = item.standard_key
        i_standard = _n(item.standard_text)
        std_matched = False
        matched_std = ""

        if i_std_key and i_std_key in r_std_keys:
            std_matched = True
            matched_std = i_std_key
        elif i_standard:
            for k in ("gost", "iso", "din"):
                val = _n(row_dict.get(k))
                if val and (val in i_standard or i_standard in val):
                    std_matched = True
                    matched_std = val
                    break

        if std_matched:
            points += 15
            reasons.append(f"стандарт: {matched_std}")
        else:
            warn_reasons.append("стандарт не совпал")

    # ── 5. Strength class (8 pts) ─────────────────────────────────────────
    r_str = _n(row_dict.get("strength"))
    i_str = _n(item.strength_class)
    if r_str and i_str and r_str == i_str:
        points += 8
        reasons.append(f"класс прочности: {r_str}")

    # ── 6. Coating (5 pts) ────────────────────────────────────────────────
    r_coat = _n(row_dict.get("coating"))
    i_coat = _n(item.material_coating)
    if r_coat and i_coat and (r_coat in i_coat or i_coat in r_coat):
        points += 5
        reasons.append(f"покрытие: {r_coat}")

    # ── 7. Volume match (10 pts) ─────────────────────────────────────────
    r_text = _n(row_dict.get("name_raw") or row_dict.get("name") or "")
    i_text = _n((item.name or "") + " " + (item.name_full or ""))
    r_vol = extract_volume_ml(r_text)
    i_vol = extract_volume_ml(i_text)
    if r_vol and i_vol:
        if abs(r_vol - i_vol) < 1.0:
            points += 10
            reasons.append(f"объём: {int(r_vol)} мл")
        else:
            warn_reasons.append(f"объём разный: {int(r_vol)} ≠ {int(i_vol)} мл")

    # ── 8. Keyword overlap (up to 12 pts) ─────────────────────────────────
    if r_text and i_text:
        r_kw = extract_name_keywords(r_text)
        i_kw = extract_name_keywords(i_text)
        if r_kw and i_kw:
            # Exclude the type words already accounted for in signal 1
            type_words = {w for w in (r_type, i_type) if w}
            common = (r_kw & i_kw) - type_words
            if common:
                kw_pts = min(12, len(common) * 4)
                points += kw_pts
                sample = sorted(common)[:3]
                reasons.append(f"слова: {', '.join(sample)}")

    # If no signal matched at all → score 0
    if not reasons and not warn_reasons:
        return {"score": 0, "reasons": [], "warn_reasons": []}

    return {
        "score": min(100, max(0, points)),
        "reasons": reasons,
        "warn_reasons": warn_reasons,
    }

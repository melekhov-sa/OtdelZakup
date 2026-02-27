"""Match scorer: score_match() returns 0..100 int with per-signal reasons + breakdown.

Scoring model — normalized weighted components:

  Component     Weight   Description
  ─────────     ──────   ───────────
  type          0.10     item type comparison
  size          0.58     size / diameter / length comparison
  standard      0.32     standard number (ГОСТ/DIN/ISO) comparison

  Only active components are included in the denominator (normalization).
  A component is inactive when neither side has data for it.

  Example: samorez with no standard → active = type(0.10) + size(0.58) = 0.68
           exact type+size → score = 0.68/0.68 = 1.0 → 100

Additive bonuses (not part of normalized denominator):
  keywords overlap    ≤ 0.08
  volume match        ≤ 0.03
  strength class      ≤ 0.02
  coating             ≤ 0.02

Size sub-scores (0..1):
  exact match         1.00   (normalized size strings equal)
  permuted tokens     1.00   (same numerics, different order)
  close ≤ 2%         0.85   (all dimensions within 2% relative)
  diam + diff length  0.60   (diameter matches, length differs)
  diameter only       0.35   (only diameter present/matches)
  length only         0.15   (only length matches)
  item has no size    0.05   (penalty: row has size, catalog doesn't)
  no match            0.00

Standard sub-scores (0..1):
  full key match      1.00   (kind + number: "GOST-7805-70")
  kind only           0.30   (same GOST/DIN/ISO, different number)
  text substring      0.15   (raw text fallback)
  no match            0.00

Decision thresholds (MatchSettings defaults):
  AUTO    ≥ 90
  SUGGEST ≥ 65
  NONE    < 65

Expected behavior:
  bolt M12x60 ГОСТ 15589-70 (exact all):    ~100
  bolt M10x45 same ГОСТ (size mismatch):     ~42
  samorez 4.2x70 exact (no standard):       ~100
  samorez 4.2x50 vs 4.2x51 (close size):    ~87 → SUGGESTED
"""
from __future__ import annotations

import re

from app.matching.normalizer import (
    extract_name_keywords,
    extract_volume_ml,
    normalize_size,
    parse_size_tokens,
    sizes_close,
)

# ── Component weights (type + size + standard must sum to 1.0) ────────────────
_TYPE_W = 0.10
_SIZE_W = 0.58
_STD_W  = 0.32

# ── Additive bonuses (not in normalized denominator) ─────────────────────────
_KW_BONUS_MAX  = 0.08
_VOL_BONUS     = 0.03
_STR_BONUS     = 0.02
_COAT_BONUS    = 0.02

# ── Post-scoring penalties and caps ──────────────────────────────────────────
_TYPE_CONFLICT_PENALTY = 30     # subtracted when both have type and they clash
_KIT_SCORE_CAP         = 20    # max score when candidate is a kit but row isn't
_STD_CONFLICT_CAP      = 15    # max score when both have standard and they clash
_SIZE_CONFLICT_CAP     = 15    # max score when both have size data and full mismatch

_KIT_MARKERS = frozenset({"комплект", "в сборе", "набор"})
_KIT_PLUS_RE = re.compile(r"[а-яёa-z]\s*\+\s*[а-яёa-z]", re.IGNORECASE)


def _is_kit(text: str) -> bool:
    """Detect kit/combo items: 'комплект', 'в сборе', 'набор', word+word patterns."""
    t = text.lower()
    for marker in _KIT_MARKERS:
        if marker in t:
            return True
    if _KIT_PLUS_RE.search(t):
        return True
    return False


def _n(val) -> str:
    return str(val or "").strip().lower()


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


def _std_kind(key: str | None) -> str | None:
    """Extract kind prefix from a standard key: 'GOST-7798-70' → 'GOST'."""
    if not key or "-" not in key:
        return None
    return key.split("-")[0]


def _size_score(
    row_dict: dict,
    item_size_raw: str,
    item,
) -> tuple[float | None, str | None, bool]:
    """Compute size match score.

    Returns (score | None, reason | None, is_warn).
    None means the size component is inactive (no data on either side).
    """
    r_size_raw = _n(row_dict.get("size"))

    # Build row tokens — try size field, fall back to diameter column
    r_tok: list[float] = []
    if r_size_raw:
        r_tok = parse_size_tokens(normalize_size(r_size_raw))
    if not r_tok:
        r_diam = _n(row_dict.get("diameter"))
        if r_diam:
            r_tok = parse_size_tokens(normalize_size(r_diam))

    # Build item tokens — try size field, fall back to item.diameter / item.length
    i_tok: list[float] = []
    if item_size_raw:
        i_tok = parse_size_tokens(normalize_size(item_size_raw))
    if not i_tok:
        i_diam_str = str(item.diameter or "").strip()
        if i_diam_str:
            d_tok = parse_size_tokens(normalize_size(i_diam_str))
            i_len_str = str(item.length or "").strip()
            l_tok = parse_size_tokens(normalize_size(i_len_str)) if i_len_str else []
            i_tok = d_tok + l_tok

    # Neither side has size → inactive
    if not r_tok and not i_tok:
        return None, None, False

    # Row has size, item doesn't → penalise item slightly
    if r_tok and not i_tok:
        return 0.05, "в каталоге нет размера", True

    # Row has no size, item has size → treat as inactive (don't penalise)
    if not r_tok and i_tok:
        return None, None, False

    # Both sides have size — compare
    r_display = r_size_raw or _n(row_dict.get("diameter"))
    i_display = item_size_raw or str(item.diameter or "")

    # Exact normalized string match
    r_norm = normalize_size(r_size_raw) if r_size_raw else ""
    i_norm = normalize_size(item_size_raw) if item_size_raw else ""
    if r_norm and i_norm and r_norm == i_norm:
        return 1.0, f"размер точно: {r_display}", False

    # Same tokens in any order
    if sorted(r_tok) == sorted(i_tok):
        return 1.0, f"размер (переставлен): {r_display} = {i_display}", False

    # All dimensions within 2% (same count required)
    if len(r_tok) == len(i_tok) and sizes_close(r_tok, i_tok, tol=0.02):
        return 0.85, f"размер близкий: {r_display} ≈ {i_display}", True

    # Diameter (first token) comparison
    r_d = r_tok[0] if r_tok else None
    i_d = i_tok[0] if i_tok else None
    r_l = r_tok[1] if len(r_tok) > 1 else None
    i_l = i_tok[1] if len(i_tok) > 1 else None

    diam_match = (
        r_d is not None and i_d is not None
        and i_d > 0 and abs(r_d - i_d) / i_d < 0.01
    )
    if diam_match:
        if r_l is not None and i_l is not None:
            return 0.60, f"диаметр совпал: {r_d}, длина {r_l} ≠ {i_l}", True
        else:
            return 0.35, f"диаметр совпал: {r_d}", False

    len_match = (
        r_l is not None and i_l is not None
        and i_l > 0 and abs(r_l - i_l) / i_l < 0.01
    )
    if len_match:
        return 0.15, f"длина совпала: {r_l}", True

    return 0.0, f"размер не совпал: {r_display} ≠ {i_display}", True


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

    # ── Get effective item fields (on-the-fly extraction fallback) ─────────
    item_type, item_size_raw, item_std_key = _get_item_effective_fields(item)

    # ── 1. Type score (weight _TYPE_W) ────────────────────────────────────
    r_type = _n(row_dict.get("item_type"))
    type_active = bool(r_type or item_type)
    type_score = 0.0

    if type_active:
        if r_type and item_type:
            if r_type == item_type:
                type_score = 1.0
                reasons.append(f"тип: {r_type}")
            elif r_type in item_type or item_type in r_type:
                type_score = 0.5
                reasons.append(f"тип (частично): {r_type}")
            else:
                type_score = 0.0
                warn_reasons.append(f"тип не совпал: {r_type} ≠ {item_type}")
        else:
            # One side has no type → neutral (0.5), don't penalise
            type_score = 0.5

    # ── 2. Size score (weight _SIZE_W) ────────────────────────────────────
    sz_score_val, sz_reason, sz_is_warn = _size_score(row_dict, item_size_raw, item)
    size_active = sz_score_val is not None
    size_score = sz_score_val if sz_score_val is not None else 0.0

    if size_active and sz_reason:
        if sz_is_warn:
            warn_reasons.append(sz_reason)
        else:
            reasons.append(sz_reason)

    # ── 3. Standard score (weight _STD_W) ─────────────────────────────────
    r_std_keys = _get_row_std_keys(row_dict)
    std_active = bool(r_std_keys)
    std_score = 0.0

    if std_active:
        if item_std_key and item_std_key in r_std_keys:
            std_score = 1.0
            reasons.append(f"стандарт: {item_std_key}")
        else:
            # Kind-only match (GOST vs GOST, different number)
            r_std_kinds = {_std_kind(k) for k in r_std_keys} - {None}
            item_std_kind = _std_kind(item_std_key)
            if item_std_kind and item_std_kind in r_std_kinds:
                std_score = 0.30
                warn_reasons.append(f"стандарт (только вид): {item_std_kind}")
            else:
                # Text substring fallback
                i_standard = _n(item.standard_text)
                if i_standard:
                    for k in ("gost", "iso", "din"):
                        val = _n(row_dict.get(k))
                        if val and (val in i_standard or i_standard in val):
                            std_score = 0.15
                            warn_reasons.append(f"стандарт (текст): {val}")
                            break
                if std_score == 0.0:
                    warn_reasons.append("стандарт не совпал")

    # ── 4. Compute normalized weighted score ──────────────────────────────
    if not type_active and not size_active and not std_active:
        # No structural signals at all — check bonuses below
        active_w = 0.0
        base_frac = 0.0
    else:
        active_w = (
            (_TYPE_W if type_active else 0.0)
            + (_SIZE_W if size_active else 0.0)
            + (_STD_W  if std_active  else 0.0)
        )
        numerator = (
            (_TYPE_W * type_score if type_active else 0.0)
            + (_SIZE_W * size_score if size_active else 0.0)
            + (_STD_W  * std_score  if std_active  else 0.0)
        )
        base_frac = numerator / active_w if active_w > 0 else 0.0

    # ── 5. Additive bonuses ───────────────────────────────────────────────
    bonus = 0.0

    # Strength class
    r_str = _n(row_dict.get("strength"))
    i_str = _n(item.strength_class)
    if r_str and i_str and r_str == i_str:
        bonus += _STR_BONUS
        reasons.append(f"класс прочности: {r_str}")

    # Coating
    r_coat = _n(row_dict.get("coating"))
    i_coat = _n(item.material_coating)
    if r_coat and i_coat and (r_coat in i_coat or i_coat in r_coat):
        bonus += _COAT_BONUS
        reasons.append(f"покрытие: {r_coat}")

    # Volume
    r_text = _n(row_dict.get("name_raw") or row_dict.get("name") or "")
    i_text = _n((item.name or "") + " " + (item.name_full or ""))
    r_vol = extract_volume_ml(r_text)
    i_vol = extract_volume_ml(i_text)
    if r_vol and i_vol:
        if abs(r_vol - i_vol) < 1.0:
            bonus += _VOL_BONUS
            reasons.append(f"объём: {int(r_vol)} мл")
        else:
            warn_reasons.append(f"объём разный: {int(r_vol)} ≠ {int(i_vol)} мл")

    # Keywords
    if r_text and i_text:
        r_kw = extract_name_keywords(r_text)
        i_kw = extract_name_keywords(i_text)
        if r_kw and i_kw:
            type_words = {w for w in (r_type, item_type) if w}
            std_words  = {"гост", "gost", "din", "iso", "исо", "штук", "штука"}
            exclude    = type_words | std_words
            r_kw_clean = r_kw - exclude
            i_kw_clean = i_kw - exclude
            common = r_kw_clean & i_kw_clean
            if common:
                kw_bonus = min(_KW_BONUS_MAX, len(common) * 0.04)
                bonus += kw_bonus
                sample = sorted(common)[:3]
                reasons.append(f"слова: {', '.join(sample)}")

    # ── No signal at all → return 0 ───────────────────────────────────────
    if not reasons and not warn_reasons:
        return {"score": 0, "reasons": [], "warn_reasons": [], "breakdown": {}}

    final_frac = min(1.0, base_frac + bonus)
    raw_score = round(final_frac * 100)

    # ── 6. Post-scoring penalties and caps ──────────────────────────────
    score_cap = 100
    penalty   = 0

    # Kit detection: candidate is a kit, row is a single item
    i_full_text = _n((item.name or "") + " " + (item.name_full or ""))
    item_is_kit = _is_kit(i_full_text)
    row_is_kit  = _is_kit(r_text) if r_text else False

    if item_is_kit and not row_is_kit:
        score_cap = min(score_cap, _KIT_SCORE_CAP)
        warn_reasons.append("кандидат = комплект")

    # Type conflict: both have explicit type, they fully disagree
    if r_type and item_type and type_score == 0.0:
        penalty += _TYPE_CONFLICT_PENALTY

    # Standard conflict: both have standard key and they don't match at all
    if std_active and std_score == 0.0 and item_std_key:
        score_cap = min(score_cap, _STD_CONFLICT_CAP)

    # Size complete mismatch (both have data, no dimensional match at all)
    if size_active and size_score == 0.0:
        score_cap = min(score_cap, _SIZE_CONFLICT_CAP)

    final_score = max(0, min(raw_score - penalty, score_cap))

    # ── Build breakdown (each component as % of its theoretical max) ──────
    breakdown: dict[str, int] = {}
    if type_active:
        breakdown["type"]     = round(type_score * 100)
    if size_active:
        breakdown["size"]     = round(size_score * 100)
    if std_active:
        breakdown["standard"] = round(std_score * 100)
    if bonus > 0:
        # Show keyword/bonus contribution relative to max bonus pool
        max_bonus = _KW_BONUS_MAX + _VOL_BONUS + _STR_BONUS + _COAT_BONUS
        breakdown["keywords"] = round(min(bonus, max_bonus) / max_bonus * 100)
    if penalty > 0:
        breakdown["penalty"] = penalty
    if score_cap < 100:
        breakdown["cap"] = score_cap

    return {
        "score":        final_score,
        "reasons":      reasons,
        "warn_reasons": warn_reasons,
        "breakdown":    breakdown,
    }

"""Match scorer: score_match() returns 0..100 int with per-signal reasons + breakdown.

Scoring model — configurable weighted components (MatchSettings):

  Component     Default Weight   Description
  ---------     --------------   -----------
  type          40               item type comparison
  size          35               size / diameter / length comparison
  standard      20               standard number (GOST/DIN/ISO) comparison
  text           5               keywords / bonus signals

  Only active components participate in the denominator (normalization).
  A component is inactive when neither side has data for it.

  score_raw = sum(w_i * f_i) / sum(active w_i) * 100

Penalties (configurable in MatchSettings):
  p_type_mismatch     -60   both have type, they fully disagree
  p_diameter_mismatch -100  diameters differ (effective gate)
  p_standard_mismatch -30   both have standard, they conflict
  p_kit_mismatch      -60   candidate is kit, row isn't

Size sub-scores (f_size: 0..1):
  exact match         1.00   (normalized size strings equal)
  permuted tokens     1.00   (same numerics, different order)
  close <= 2%         0.85   (all dimensions within 2% relative)
  diam + diff length  0.60   (diameter matches, length differs)
  diameter only       0.35   (only diameter present/matches)
  length only         0.15   (only length matches)
  item has no size    0.05   (penalty: row has size, catalog doesn't)
  no match            0.00

Standard sub-scores (f_standard: 0..1):
  full key match      1.00   (kind + number: "GOST-7805-70")
  kind only           0.30   (same GOST/DIN/ISO, different number)
  text substring      0.15   (raw text fallback)
  no match            0.00

Text sub-scores (f_text: 0..1):
  Based on keyword overlap, volume match, strength class, coating.
  Max contribution: 1.0 (all text signals match).
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

# ── Text bonus sub-weights (contribute to f_text factor 0..1) ────────────────
_KW_BONUS_MAX = 0.50   # keyword overlap up to 50% of text factor
_VOL_BONUS    = 0.20   # volume match
_STR_BONUS    = 0.15   # strength class match
_COAT_BONUS   = 0.15   # coating match

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
    """Return (item_type, size, standard_key) — stored fields with on-the-fly fallback."""
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
    """Extract standard keys from a row dict."""
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
    """Extract kind prefix from a standard key: 'GOST-7798-70' -> 'GOST'."""
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

    # Neither side has size -> inactive
    if not r_tok and not i_tok:
        return None, None, False

    # Row has size, item doesn't -> penalise item slightly
    if r_tok and not i_tok:
        return 0.05, "в каталоге нет размера", True

    # Row has no size, item has size -> treat as inactive (don't penalise)
    if not r_tok and i_tok:
        return None, None, False

    # Both sides have size -- compare
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
        return 0.85, f"размер близкий: {r_display} ~ {i_display}", True

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
            return 0.60, f"диаметр совпал: {r_d}, длина {r_l} != {i_l}", True
        else:
            return 0.35, f"диаметр совпал: {r_d}", False

    len_match = (
        r_l is not None and i_l is not None
        and i_l > 0 and abs(r_l - i_l) / i_l < 0.01
    )
    if len_match:
        return 0.15, f"длина совпала: {r_l}", True

    return 0.0, f"размер не совпал: {r_display} != {i_display}", True


def _diameter_mismatch(row_dict: dict, item_size_raw: str, item) -> bool:
    """Check if diameters are known on both sides and clearly different.

    Used as a gating signal: when diameters are both present and differ,
    the candidate gets a large penalty (effectively excluded).
    """
    r_size_raw = _n(row_dict.get("size"))
    r_tok: list[float] = []
    if r_size_raw:
        r_tok = parse_size_tokens(normalize_size(r_size_raw))
    if not r_tok:
        r_diam = _n(row_dict.get("diameter"))
        if r_diam:
            r_tok = parse_size_tokens(normalize_size(r_diam))

    i_tok: list[float] = []
    if item_size_raw:
        i_tok = parse_size_tokens(normalize_size(item_size_raw))
    if not i_tok:
        i_diam_str = str(item.diameter or "").strip()
        if i_diam_str:
            i_tok = parse_size_tokens(normalize_size(i_diam_str))

    if not r_tok or not i_tok:
        return False  # can't determine — not a mismatch

    r_d = r_tok[0]
    i_d = i_tok[0]
    if i_d == 0:
        return False
    return abs(r_d - i_d) / max(i_d, 0.01) >= 0.01


def score_match(row_dict: dict, item, settings=None) -> dict:
    """Score an InternalItem against extracted row fields.

    Args:
        row_dict: dict with keys item_type, size, diameter, length,
                  gost, iso, din, strength, coating, name_raw, name.
        item: InternalItem instance.
        settings: MatchSettings (optional, loaded from DB if None).

    Returns:
        {
            "score":        int (0..100),
            "reasons":      list[str],   # positive match signals
            "warn_reasons": list[str],   # soft mismatches / caveats
            "breakdown":    dict,        # component -> contribution info
        }
    """
    if settings is None:
        from app.match_settings import load_match_settings
        settings = load_match_settings()

    reasons: list[str] = []
    warn_reasons: list[str] = []

    # ── Get effective item fields (on-the-fly extraction fallback) ─────────
    item_type, item_size_raw, item_std_key = _get_item_effective_fields(item)

    # ── 1. Type factor (f_type: 0..1) ──────────────────────────────────────
    r_type = _n(row_dict.get("item_type"))
    type_active = bool(r_type or item_type)
    f_type = 0.0

    if type_active:
        if r_type and item_type:
            if r_type == item_type:
                f_type = 1.0
                reasons.append(f"тип: {r_type}")
            elif r_type in item_type or item_type in r_type:
                f_type = 0.5
                reasons.append(f"тип (частично): {r_type}")
            else:
                f_type = 0.0
                warn_reasons.append(f"тип не совпал: {r_type} != {item_type}")
        else:
            # One side has no type -> neutral (0.5), don't penalise
            f_type = 0.5

    # ── 2. Size factor (f_size: 0..1) ──────────────────────────────────────
    sz_score_val, sz_reason, sz_is_warn = _size_score(row_dict, item_size_raw, item)
    size_active = sz_score_val is not None
    f_size = sz_score_val if sz_score_val is not None else 0.0

    if size_active and sz_reason:
        if sz_is_warn:
            warn_reasons.append(sz_reason)
        else:
            reasons.append(sz_reason)

    # ── 3. Standard factor (f_standard: 0..1) ─────────────────────────────
    r_std_keys = _get_row_std_keys(row_dict)
    std_active = bool(r_std_keys)
    f_standard = 0.0

    if std_active:
        if item_std_key and item_std_key in r_std_keys:
            f_standard = 1.0
            reasons.append(f"стандарт: {item_std_key}")
        else:
            # Kind-only match (GOST vs GOST, different number)
            r_std_kinds = {_std_kind(k) for k in r_std_keys} - {None}
            item_std_kind = _std_kind(item_std_key)
            if item_std_kind and item_std_kind in r_std_kinds:
                f_standard = 0.30
                warn_reasons.append(f"стандарт (только вид): {item_std_kind}")
            else:
                # Text substring fallback
                i_standard = _n(item.standard_text)
                if i_standard:
                    for k in ("gost", "iso", "din"):
                        val = _n(row_dict.get(k))
                        if val and (val in i_standard or i_standard in val):
                            f_standard = 0.15
                            warn_reasons.append(f"стандарт (текст): {val}")
                            break
                if f_standard == 0.0:
                    warn_reasons.append("стандарт не совпал")

    # ── 4. Text factor (f_text: 0..1) ─────────────────────────────────────
    f_text = 0.0
    text_bonus = 0.0

    # Strength class
    r_str = _n(row_dict.get("strength"))
    i_str = _n(item.strength_class)
    if r_str and i_str and r_str == i_str:
        text_bonus += _STR_BONUS
        reasons.append(f"класс прочности: {r_str}")

    # Coating
    r_coat = _n(row_dict.get("coating"))
    i_coat = _n(item.material_coating)
    if r_coat and i_coat and (r_coat in i_coat or i_coat in r_coat):
        text_bonus += _COAT_BONUS
        reasons.append(f"покрытие: {r_coat}")

    # Volume
    r_text = _n(row_dict.get("name_raw") or row_dict.get("name") or "")
    i_text = _n((item.name or "") + " " + (item.name_full or ""))
    r_vol = extract_volume_ml(r_text)
    i_vol = extract_volume_ml(i_text)
    if r_vol and i_vol:
        if abs(r_vol - i_vol) < 1.0:
            text_bonus += _VOL_BONUS
            reasons.append(f"объём: {int(r_vol)} мл")
        else:
            warn_reasons.append(f"объём разный: {int(r_vol)} != {int(i_vol)} мл")

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
                kw_frac = min(_KW_BONUS_MAX, len(common) * 0.15)
                text_bonus += kw_frac
                sample = sorted(common)[:3]
                reasons.append(f"слова: {', '.join(sample)}")

    f_text = min(1.0, text_bonus)
    text_active = f_text > 0

    # ── 5. Compute weighted score ──────────────────────────────────────────
    w_type = settings.w_type if type_active else 0
    w_size = settings.w_size if size_active else 0
    w_std  = settings.w_standard if std_active else 0
    w_text = settings.w_text if text_active else 0
    total_w = w_type + w_size + w_std + w_text

    if total_w == 0:
        # No signals at all
        if not reasons and not warn_reasons:
            return {"score": 0, "reasons": [], "warn_reasons": [], "breakdown": {}}
        raw_score = 0
    else:
        numerator = (
            w_type * f_type
            + w_size * f_size
            + w_std  * f_standard
            + w_text * f_text
        )
        raw_score = round(numerator / total_w * 100)

    # ── 6. Penalties ───────────────────────────────────────────────────────
    penalty = 0
    penalty_reasons: list[str] = []

    # Kit detection: candidate is a kit, row is a single item
    i_full_text = _n((item.name or "") + " " + (item.name_full or ""))
    item_is_kit = _is_kit(i_full_text)
    row_is_kit  = _is_kit(r_text) if r_text else False

    if item_is_kit and not row_is_kit:
        penalty += settings.p_kit_mismatch
        warn_reasons.append("кандидат = комплект")
        penalty_reasons.append(f"комплект: -{settings.p_kit_mismatch}")

    # Type conflict: both have explicit type, they fully disagree
    if r_type and item_type and f_type == 0.0:
        penalty += settings.p_type_mismatch
        penalty_reasons.append(f"тип: -{settings.p_type_mismatch}")

    # Diameter mismatch gate
    if size_active and _diameter_mismatch(row_dict, item_size_raw, item):
        penalty += settings.p_diameter_mismatch
        penalty_reasons.append(f"диаметр: -{settings.p_diameter_mismatch}")

    # Standard conflict: both have standard key and they don't match at all
    if std_active and f_standard == 0.0 and item_std_key:
        penalty += settings.p_standard_mismatch
        penalty_reasons.append(f"стандарт: -{settings.p_standard_mismatch}")

    final_score = max(0, min(100, raw_score - penalty))

    # ── 7. Folder priority bonus ────────────────────────────────────────────
    _PRIORITY_BONUS = {1: 8, 2: 4, 3: 2, 4: 1}
    folder_priority = getattr(item, "folder_priority", None)
    priority_bonus = _PRIORITY_BONUS.get(folder_priority, 0) if folder_priority is not None else 0
    if priority_bonus:
        final_score = min(100, final_score + priority_bonus)
        reasons.append(f"приоритет папки {folder_priority}: +{priority_bonus}")

    # ── Build breakdown ────────────────────────────────────────────────────
    breakdown: dict[str, str] = {}
    if type_active:
        contrib = round(w_type * f_type / total_w * 100) if total_w > 0 else 0
        breakdown["type"] = f"{round(f_type * 100)}% (вклад {contrib})"
    if size_active:
        contrib = round(w_size * f_size / total_w * 100) if total_w > 0 else 0
        breakdown["size"] = f"{round(f_size * 100)}% (вклад {contrib})"
    if std_active:
        contrib = round(w_std * f_standard / total_w * 100) if total_w > 0 else 0
        breakdown["standard"] = f"{round(f_standard * 100)}% (вклад {contrib})"
    if text_active:
        contrib = round(w_text * f_text / total_w * 100) if total_w > 0 else 0
        breakdown["text"] = f"{round(f_text * 100)}% (вклад {contrib})"
    if penalty > 0:
        breakdown["penalty"] = f"-{penalty} ({'; '.join(penalty_reasons)})"
    if priority_bonus:
        breakdown["folder_priority"] = f"+{priority_bonus} (приоритет {folder_priority})"

    return {
        "score":        final_score,
        "reasons":      reasons,
        "warn_reasons": warn_reasons,
        "breakdown":    breakdown,
    }

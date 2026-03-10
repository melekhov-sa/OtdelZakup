"""Auto-match settings: load/save thresholds, weights and penalties from DB."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class MatchSettings:
    """Thresholds, scoring weights and penalties for the auto-match engine.

    Weights (w_*) define the relative importance of each scoring factor.
    The score is computed as:  score = sum(w_i * f_i) / sum(active w_i) * 100
    where f_i is the factor value (0..1) and active w_i are weights for
    factors that have data on at least one side.

    Penalties (p_*) are subtracted from the raw score when conflicts are detected.
    Caps limit the maximum score for specific conflict types.
    """

    # ── General flags ─────────────────────────────────────────────────────────
    enable_auto_match: bool = True
    enable_auto_match_memory: bool = True
    always_require_confirmation: bool = False  # forces AUTO → SUGGESTED

    # ── Thresholds ────────────────────────────────────────────────────────────
    auto_match_threshold: int = 90   # score >= this → AUTO_SCORE
    suggest_threshold: int = 70       # score >= this → SUGGESTED
    auto_match_delta: int = 15        # auto only when score_1 - score_2 >= delta

    # ── Scoring weights (relative; normalized internally) ─────────────────────
    w_type: int = 40       # item type match
    w_size: int = 35       # size / diameter / length match
    w_standard: int = 20   # standard number (GOST/DIN/ISO) match
    w_text: int = 5        # keywords / bonus signals

    # ── Penalties (subtracted from raw score 0..100) ──────────────────────────
    p_type_mismatch: int = 60       # both have type but they disagree
    p_diameter_mismatch: int = 100  # diameter completely different (effectively gates)
    p_standard_mismatch: int = 30   # both have standard but they conflict
    p_kit_mismatch: int = 60        # candidate is a kit, row is a single item

    # ── Auto-apply via MinHash Jaccard threshold ───────────────────────────────
    auto_apply_enabled: bool = True     # enable auto-apply when best J >= threshold
    auto_apply_jaccard_threshold: float = 0.40  # auto-apply when best_J >= this
    suggest_jaccard_threshold: float = 0.25     # show as suggestion when J >= this
    auto_match_delta_jaccard: float = 0.05      # auto only when J1-J2 >= this (or no J2)

    # ── Minimum display score ──────────────────────────────────────────────────
    # Candidates with score (%) below this value are hidden from the candidate
    # list and never auto-matched or suggested.  Set to 0 to disable filtering.
    min_display_score: int = 40

    # ── MinHash / LSH ──────────────────────────────────────────────────────────
    enable_minhash: bool = True         # enable MinHash candidate channel
    lsh_threshold: float = 0.3          # Jaccard threshold for LSH
    num_perm: int = 128                 # number of hash permutations
    minhash_top_k: int = 20             # max candidates from LSH per query
    ngram_n: int = 4                    # char n-gram size for MinHash signatures
    use_type_buckets: bool = True       # separate LSH indices per item type
    min_candidates_before_fallback: int = 5  # fallback to global when type bucket has fewer
    minhash_filter_size: bool = False   # post-filter LSH results by diameter

    # ── Standard analogs ──────────────────────────────────────────────────────
    use_standard_analogs_in_main_match: bool = False  # augment MinHash with analog standards
    analogs_only: bool = False  # search ONLY via analog standards (skip direct query)


_SETTING_KEYS = [
    "enable_auto_match",
    "enable_auto_match_memory",
    "auto_match_threshold",
    "suggest_threshold",
    "always_require_confirmation",
    "auto_match_delta",
    "w_type", "w_size", "w_standard", "w_text",
    "p_type_mismatch", "p_diameter_mismatch", "p_standard_mismatch", "p_kit_mismatch",
    "auto_apply_enabled", "auto_apply_jaccard_threshold",
    "suggest_jaccard_threshold", "auto_match_delta_jaccard",
    "enable_minhash", "lsh_threshold", "num_perm", "minhash_top_k",
    "ngram_n", "use_type_buckets", "min_candidates_before_fallback",
    "minhash_filter_size",
    "use_standard_analogs_in_main_match",
    "min_display_score",
]

# Defaults for _int() lookup
_INT_DEFAULTS = {
    "auto_match_threshold": 90,
    "suggest_threshold": 70,
    "auto_match_delta": 15,
    "w_type": 40, "w_size": 35, "w_standard": 20, "w_text": 5,
    "p_type_mismatch": 60, "p_diameter_mismatch": 100,
    "p_standard_mismatch": 30, "p_kit_mismatch": 60,
    "num_perm": 128, "minhash_top_k": 20,
    "ngram_n": 4, "min_candidates_before_fallback": 5,
    "min_display_score": 40,
}

_FLOAT_DEFAULTS = {
    "lsh_threshold": 0.3,
    "auto_apply_jaccard_threshold": 0.40,
    "suggest_jaccard_threshold": 0.25,
    "auto_match_delta_jaccard": 0.05,
}


def load_match_settings() -> MatchSettings:
    """Read settings from DB; fall back to defaults when not set."""
    from app.database import get_db_session
    from app.models import SystemSetting

    session = get_db_session()
    try:
        rows = session.query(SystemSetting).filter(
            SystemSetting.key.in_(_SETTING_KEYS)
        ).all()
        data = {r.key: r.value for r in rows}
    finally:
        session.close()

    def _bool(key: str, default: bool) -> bool:
        v = data.get(key)
        return default if v is None else v.lower() in ("1", "true", "yes", "on")

    def _int(key: str, default: int) -> int:
        v = data.get(key)
        if v is None:
            return default
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    def _float(key: str, default: float) -> float:
        v = data.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    return MatchSettings(
        enable_auto_match=_bool("enable_auto_match", True),
        enable_auto_match_memory=_bool("enable_auto_match_memory", True),
        always_require_confirmation=_bool("always_require_confirmation", False),
        auto_match_threshold=_int("auto_match_threshold", 90),
        suggest_threshold=_int("suggest_threshold", 70),
        auto_match_delta=_int("auto_match_delta", 15),
        w_type=_int("w_type", 40),
        w_size=_int("w_size", 35),
        w_standard=_int("w_standard", 20),
        w_text=_int("w_text", 5),
        p_type_mismatch=_int("p_type_mismatch", 60),
        p_diameter_mismatch=_int("p_diameter_mismatch", 100),
        p_standard_mismatch=_int("p_standard_mismatch", 30),
        p_kit_mismatch=_int("p_kit_mismatch", 60),
        auto_apply_enabled=_bool("auto_apply_enabled", True),
        auto_apply_jaccard_threshold=_float("auto_apply_jaccard_threshold", 0.40),
        suggest_jaccard_threshold=_float("suggest_jaccard_threshold", 0.25),
        auto_match_delta_jaccard=_float("auto_match_delta_jaccard", 0.05),
        enable_minhash=_bool("enable_minhash", True),
        lsh_threshold=_float("lsh_threshold", 0.3),
        num_perm=_int("num_perm", 128),
        minhash_top_k=_int("minhash_top_k", 20),
        ngram_n=_int("ngram_n", 4),
        use_type_buckets=_bool("use_type_buckets", True),
        min_candidates_before_fallback=_int("min_candidates_before_fallback", 5),
        minhash_filter_size=_bool("minhash_filter_size", False),
        use_standard_analogs_in_main_match=_bool("use_standard_analogs_in_main_match", False),
        min_display_score=_int("min_display_score", 40),
    )


def save_match_settings(settings: MatchSettings) -> None:
    """Persist all settings to DB (upsert)."""
    from app.database import get_db_session
    from app.models import SystemSetting

    values = {
        "enable_auto_match": "true" if settings.enable_auto_match else "false",
        "enable_auto_match_memory": "true" if settings.enable_auto_match_memory else "false",
        "always_require_confirmation": "true" if settings.always_require_confirmation else "false",
        "auto_match_threshold": str(settings.auto_match_threshold),
        "suggest_threshold": str(settings.suggest_threshold),
        "auto_match_delta": str(settings.auto_match_delta),
        "w_type": str(settings.w_type),
        "w_size": str(settings.w_size),
        "w_standard": str(settings.w_standard),
        "w_text": str(settings.w_text),
        "p_type_mismatch": str(settings.p_type_mismatch),
        "p_diameter_mismatch": str(settings.p_diameter_mismatch),
        "p_standard_mismatch": str(settings.p_standard_mismatch),
        "p_kit_mismatch": str(settings.p_kit_mismatch),
        "auto_apply_enabled": "true" if settings.auto_apply_enabled else "false",
        "auto_apply_jaccard_threshold": str(settings.auto_apply_jaccard_threshold),
        "suggest_jaccard_threshold": str(settings.suggest_jaccard_threshold),
        "auto_match_delta_jaccard": str(settings.auto_match_delta_jaccard),
        "enable_minhash": "true" if settings.enable_minhash else "false",
        "lsh_threshold": str(settings.lsh_threshold),
        "num_perm": str(settings.num_perm),
        "minhash_top_k": str(settings.minhash_top_k),
        "ngram_n": str(settings.ngram_n),
        "use_type_buckets": "true" if settings.use_type_buckets else "false",
        "min_candidates_before_fallback": str(settings.min_candidates_before_fallback),
        "minhash_filter_size": "true" if settings.minhash_filter_size else "false",
        "use_standard_analogs_in_main_match": "true" if settings.use_standard_analogs_in_main_match else "false",
        "min_display_score": str(settings.min_display_score),
    }

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        for key, value in values.items():
            existing = session.get(SystemSetting, key)
            if existing:
                existing.value = value
                existing.updated_at = now
            else:
                session.add(SystemSetting(key=key, value=value, updated_at=now))
        session.commit()
    finally:
        session.close()

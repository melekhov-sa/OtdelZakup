"""Auto-match settings: load/save thresholds and flags from DB."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class MatchSettings:
    """Thresholds and flags governing the auto-match decision engine."""

    enable_auto_match: bool = True
    enable_auto_match_memory: bool = True
    auto_match_threshold: int = 90   # score >= this → AUTO_SCORE
    suggest_threshold: int = 70       # score >= this → SUGGESTED
    always_require_confirmation: bool = False  # forces AUTO → SUGGESTED


_SETTING_KEYS = [
    "enable_auto_match",
    "enable_auto_match_memory",
    "auto_match_threshold",
    "suggest_threshold",
    "always_require_confirmation",
]


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

    return MatchSettings(
        enable_auto_match=_bool("enable_auto_match", True),
        enable_auto_match_memory=_bool("enable_auto_match_memory", True),
        auto_match_threshold=_int("auto_match_threshold", 90),
        suggest_threshold=_int("suggest_threshold", 70),
        always_require_confirmation=_bool("always_require_confirmation", False),
    )


def save_match_settings(settings: MatchSettings) -> None:
    """Persist all settings to DB (upsert)."""
    from app.database import get_db_session
    from app.models import SystemSetting

    values = {
        "enable_auto_match": "true" if settings.enable_auto_match else "false",
        "enable_auto_match_memory": "true" if settings.enable_auto_match_memory else "false",
        "auto_match_threshold": str(settings.auto_match_threshold),
        "suggest_threshold": str(settings.suggest_threshold),
        "always_require_confirmation": "true" if settings.always_require_confirmation else "false",
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

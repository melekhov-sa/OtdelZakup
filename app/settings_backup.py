"""Snapshot of the user-editable reference data: rules, directories, settings.

The catalog comes from 1C and is restored by syncing again; the match memory is
keyed by local item ids that differ on another server.  Neither belongs in a
snapshot — see docs/specs/2026-09-12-vygruzka-nastroek.md.
"""
from __future__ import annotations

import json
import socket
import subprocess
from datetime import datetime
from pathlib import Path

from app.database import get_db_session
from app.models import (
    BaseValidationRule,
    CoatingRule,
    InferenceRule,
    MasterItem,
    MasterItemMember,
    NameTemplate,
    NormalizationRule,
    ProductType,
    ReadinessRule,
    SizeRule,
    StandardEquivalent,
    StandardRef,
    StrengthRule,
    SystemSetting,
    TailPhrase,
    ValidationRule,
    ValidationRuleException,
)

FORMAT_VERSION = 1

# Parents before children: restore inserts in this order and deletes in the
# reverse one, so a row never points at something that is not there yet.
SNAPSHOT_MODELS = [
    StandardEquivalent,
    StandardRef,
    ProductType,
    TailPhrase,
    ReadinessRule,
    BaseValidationRule,
    ValidationRule,
    ValidationRuleException,
    CoatingRule,
    StrengthRule,
    SizeRule,
    NormalizationRule,
    InferenceRule,
    NameTemplate,
    SystemSetting,
    MasterItem,
    MasterItemMember,
]


class SnapshotError(Exception):
    """The file cannot be used as a settings snapshot."""


def _row_to_dict(row) -> dict:
    out: dict = {}
    for col in row.__table__.columns:
        value = getattr(row, col.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        out[col.name] = value
    return out


def _current_commit() -> str:
    """Short commit of the running code, or "" when git is not available."""
    try:
        done = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        return done.stdout.strip() if done.returncode == 0 else ""
    except Exception:
        return ""


def build_snapshot() -> dict:
    """Collect every snapshot table into a plain dict ready for JSON."""
    session = get_db_session()
    try:
        tables = {
            model.__tablename__: [_row_to_dict(r) for r in session.query(model).all()]
            for model in SNAPSHOT_MODELS
        }
    finally:
        session.close()

    return {
        "format_version": FORMAT_VERSION,
        "exported_at": datetime.now().astimezone().isoformat(),
        "app_commit": _current_commit(),
        "source_host": socket.gethostname(),
        "tables": tables,
    }


def dump_snapshot(snapshot: dict) -> str:
    """Render a snapshot as the text that goes into the file."""
    return json.dumps(snapshot, ensure_ascii=False, indent=2)

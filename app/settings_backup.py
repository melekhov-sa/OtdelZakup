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


def _dict_to_kwargs(model, data: dict) -> dict:
    """Turn one exported row back into constructor arguments for *model*."""
    from sqlalchemy import DateTime  # noqa: PLC0415

    kwargs: dict = {}
    for col in model.__table__.columns:
        if col.name not in data:
            continue
        value = data[col.name]
        if isinstance(value, str) and isinstance(col.type, DateTime):
            value = datetime.fromisoformat(value)
        kwargs[col.name] = value
    return kwargs


def write_backup_file(directory: Path) -> Path:
    """Save the current state as a snapshot file and return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"settings-{datetime.now():%Y-%m-%d-%H%M%S}.json"
    path.write_text(dump_snapshot(build_snapshot()), encoding="utf-8")
    return path


def restore_snapshot(payload: dict, backup_dir: Path | None = None) -> dict:
    """Replace the snapshot tables with the contents of *payload*.

    A table absent from the payload is left alone: an older snapshot must not
    wipe data it never knew about.  A table present as an empty list is
    emptied — that is a deliberate "nothing here".

    Everything happens in one transaction: a failure half-way leaves the
    database exactly as it was.
    """
    if not isinstance(payload, dict):
        raise SnapshotError("Файл не является снимком настроек.")

    version = payload.get("format_version")
    if version != FORMAT_VERSION:
        raise SnapshotError(
            f"Формат файла ({version}) не поддерживается, нужен {FORMAT_VERSION}."
        )

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise SnapshotError("В файле нет раздела tables.")

    backup_path = write_backup_file(backup_dir) if backup_dir is not None else None

    session = get_db_session()
    report: dict = {}
    try:
        for model in SNAPSHOT_MODELS:
            report[model.__tablename__] = {
                "before": session.query(model).count(),
                "after": 0,
            }

        for model in reversed(SNAPSHOT_MODELS):
            if model.__tablename__ in tables:
                session.query(model).delete()

        for model in SNAPSHOT_MODELS:
            name = model.__tablename__
            rows = tables.get(name)
            if rows is None:
                report[name]["after"] = report[name]["before"]
                continue
            for row in rows:
                session.add(model(**_dict_to_kwargs(model, row)))
            report[name]["after"] = len(rows)

        session.commit()
    except SnapshotError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise SnapshotError(f"Загрузка не удалась: {exc}") from exc
    finally:
        session.close()

    invalidate_all_caches()

    return {
        "tables": report,
        "backup_path": str(backup_path) if backup_path else None,
    }


def invalidate_all_caches() -> None:
    """Drop every in-process cache that holds snapshot data.

    Without this the file lands in the database and the service keeps serving
    the previous rules until each TTL runs out.
    """
    from app.category_validator import invalidate_category_validator_cache  # noqa: PLC0415
    from app.inference_engine import invalidate_inference_cache  # noqa: PLC0415
    from app.match_settings import invalidate_settings_cache  # noqa: PLC0415
    from app.matcher import (  # noqa: PLC0415
        invalidate_master_guid_cache,
        invalidate_match_memory_cache,
    )
    from app.matching.standard_analogs import invalidate_standard_analogs_cache  # noqa: PLC0415
    from app.parsing.tail_extractor import invalidate_tail_phrases_cache  # noqa: PLC0415
    from app.product_type_matcher import invalidate_product_types_cache  # noqa: PLC0415
    from app.readiness import invalidate_readiness_caches  # noqa: PLC0415
    from app.services.coating_detector import invalidate_coating_cache  # noqa: PLC0415
    from app.services.normalization_service import invalidate_normalization_cache  # noqa: PLC0415
    from app.services.size_detector import invalidate_size_cache  # noqa: PLC0415
    from app.services.strength_detector import invalidate_strength_cache  # noqa: PLC0415

    for drop in (
        invalidate_standard_analogs_cache,
        invalidate_readiness_caches,
        invalidate_category_validator_cache,
        invalidate_inference_cache,
        invalidate_settings_cache,
        invalidate_product_types_cache,
        invalidate_tail_phrases_cache,
        invalidate_coating_cache,
        invalidate_strength_cache,
        invalidate_size_cache,
        invalidate_normalization_cache,
        invalidate_match_memory_cache,
        invalidate_master_guid_cache,
    ):
        # One cache module refusing to load must not leave the rest stale.
        try:
            drop()
        except Exception:
            pass

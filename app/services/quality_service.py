"""Quality Monitoring Pipeline — metrics collection and computation.

Provides:
- Pipeline run/step tracking helpers
- Metrics computation from stored pipeline data
- Match feedback recording
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.database import get_db_session
from app.quality_models import MatchFeedback, PipelineRun, PipelineStep, SettingsVersion


# ── Expected fields per parsed line (for field recognition rate) ─────────────

_EXPECTED_FIELDS = ("item_type", "size", "diameter", "length", "strength", "coating", "gost", "din", "iso")
_RECOGNITION_FIELDS = {
    "size": ("size",),
    "strength": ("strength",),
    "coating": ("coating",),
    "standard": ("gost", "din", "iso"),
}


# ── Pipeline run helpers ────────────────────────────────────────────────────

def create_pipeline_run(order_id: int | None = None, session=None) -> PipelineRun:
    close = session is None
    if session is None:
        session = get_db_session()
    try:
        # Snapshot current settings version
        sv = _current_settings_version(session)
        run = PipelineRun(
            created_at=datetime.now(timezone.utc),
            order_id=order_id,
            settings_version=sv,
        )
        session.add(run)
        session.flush()
        return run
    finally:
        if close:
            session.close()


def _current_settings_version(session) -> str | None:
    latest = (
        session.query(SettingsVersion)
        .order_by(SettingsVersion.id.desc())
        .first()
    )
    return latest.version_code if latest else None


@contextmanager
def track_step(run: PipelineRun, step_name: str, session, input_rows: int = 0):
    """Context manager that creates a PipelineStep and records duration.

    Usage::

        with track_step(run, "parse_client_request", session, input_rows=10) as step:
            # do work ...
            step.output_rows = 8
            step.success_rate = 0.8
            step.extra = {"key": "value"}
    """
    step = PipelineStep(
        pipeline_run_id=run.id,
        step_name=step_name,
        started_at=datetime.now(timezone.utc),
        input_rows=input_rows,
    )
    session.add(step)
    session.flush()
    t0 = time.perf_counter_ns()
    try:
        yield step
    finally:
        elapsed_ms = int((time.perf_counter_ns() - t0) / 1_000_000)
        step.finished_at = datetime.now(timezone.utc)
        step.duration_ms = elapsed_ms
        session.flush()


# ── Match feedback ──────────────────────────────────────────────────────────

def record_feedback(
    order_id: int,
    client_line_id: int,
    system_choice_id: int | None,
    user_choice_id: int,
    match_score: float | None = None,
    pipeline_run_id: int | None = None,
    session=None,
) -> MatchFeedback:
    """Record a user correction to system matching.

    is_correct = True when system_choice_id == user_choice_id.
    """
    close = session is None
    if session is None:
        session = get_db_session()
    try:
        fb = MatchFeedback(
            pipeline_run_id=pipeline_run_id,
            order_id=order_id,
            client_line_id=client_line_id,
            system_choice_id=system_choice_id,
            user_choice_id=user_choice_id,
            match_score=match_score,
            is_correct=(system_choice_id is not None and system_choice_id == user_choice_id),
            created_at=datetime.now(timezone.utc),
        )
        session.add(fb)
        session.flush()
        return fb
    finally:
        if close:
            session.commit()
            session.close()


# ── Settings version snapshot ───────────────────────────────────────────────

def save_settings_version(version_code: str, description: str = "", snapshot: dict | None = None, session=None):
    close = session is None
    if session is None:
        session = get_db_session()
    try:
        existing = session.query(SettingsVersion).filter_by(version_code=version_code).first()
        if existing:
            return existing
        sv = SettingsVersion(
            version_code=version_code,
            created_at=datetime.now(timezone.utc),
            description=description,
            settings_snapshot_json=json.dumps(snapshot or {}, ensure_ascii=False),
        )
        session.add(sv)
        session.flush()
        if close:
            session.commit()
        return sv
    finally:
        if close:
            session.close()


# ── Metrics computation ─────────────────────────────────────────────────────

@dataclass
class QualityMetrics:
    # Parsing
    parse_success_rate: float = 0.0
    field_recognition_rate: float = 0.0
    size_recognition_rate: float = 0.0
    strength_recognition_rate: float = 0.0
    coating_recognition_rate: float = 0.0
    standard_recognition_rate: float = 0.0

    # Catalog matching
    auto_match_rate: float = 0.0
    manual_match_rate: float = 0.0
    match_accuracy: float = 0.0

    # Supplier parsing
    supplier_parse_success_rate: float = 0.0

    # Supplier matching
    supplier_auto_match_rate: float = 0.0

    # Correction
    correction_rate: float = 0.0

    # Overall KPI
    full_auto_rate: float = 0.0

    # Counts for context
    total_runs: int = 0
    total_client_lines: int = 0
    total_supplier_lines: int = 0
    total_feedbacks: int = 0

    # Per-run history (for charts)
    history: list[dict] = field(default_factory=list)


def _safe_div(a: float, b: float) -> float:
    return round(a / b, 4) if b > 0 else 0.0


def compute_quality_metrics(last_n: int = 50, session=None) -> QualityMetrics:
    """Compute aggregate quality metrics from the last N pipeline runs."""
    close = session is None
    if session is None:
        session = get_db_session()
    try:
        runs = (
            session.query(PipelineRun)
            .order_by(PipelineRun.created_at.desc())
            .limit(last_n)
            .all()
        )
        if not runs:
            return QualityMetrics()

        m = QualityMetrics(total_runs=len(runs))

        sum_client = 0
        sum_parsed_client = 0
        sum_supplier = 0
        sum_parsed_supplier = 0
        sum_auto = 0
        sum_manual = 0
        sum_auto_sup = 0
        sum_manual_sup = 0
        sum_correct = 0
        sum_incorrect = 0

        history = []

        for run in reversed(runs):  # oldest first for charts
            sum_client += run.total_client_lines
            sum_parsed_client += run.parsed_client_lines
            sum_supplier += run.total_supplier_lines
            sum_parsed_supplier += run.parsed_supplier_lines
            sum_auto += run.auto_matches
            sum_manual += run.manual_matches
            sum_auto_sup += run.auto_supplier_matches
            sum_manual_sup += run.manual_supplier_matches
            sum_correct += run.system_match_correct
            sum_incorrect += run.system_match_incorrect

            history.append({
                "id": run.id,
                "date": run.created_at.strftime("%Y-%m-%d %H:%M") if run.created_at else "",
                "order_id": run.order_id,
                "parse_success_rate": _safe_div(run.parsed_client_lines, run.total_client_lines),
                "auto_match_rate": _safe_div(run.auto_matches, run.total_client_lines),
                "match_accuracy": _safe_div(
                    run.system_match_correct,
                    run.system_match_correct + run.system_match_incorrect,
                ),
                "settings_version": run.settings_version,
            })

        m.total_client_lines = sum_client
        m.total_supplier_lines = sum_supplier
        m.parse_success_rate = _safe_div(sum_parsed_client, sum_client)
        m.auto_match_rate = _safe_div(sum_auto, sum_client)
        m.manual_match_rate = _safe_div(sum_manual, sum_client)
        m.match_accuracy = _safe_div(sum_correct, sum_correct + sum_incorrect)
        m.supplier_parse_success_rate = _safe_div(sum_parsed_supplier, sum_supplier)
        m.supplier_auto_match_rate = _safe_div(sum_auto_sup, sum_supplier)
        m.full_auto_rate = _safe_div(sum_auto, sum_client)
        m.history = history

        # Field recognition rates from pipeline steps
        parse_steps = (
            session.query(PipelineStep)
            .filter(
                PipelineStep.pipeline_run_id.in_([r.id for r in runs]),
                PipelineStep.step_name == "parse_client_request",
            )
            .all()
        )
        if parse_steps:
            total_fr = 0.0
            total_size = 0.0
            total_strength = 0.0
            total_coating = 0.0
            total_standard = 0.0
            n_steps = 0
            for ps in parse_steps:
                ex = ps.extra
                if ex:
                    n_steps += 1
                    total_fr += ex.get("field_recognition_rate", 0.0)
                    total_size += ex.get("size_recognition_rate", 0.0)
                    total_strength += ex.get("strength_recognition_rate", 0.0)
                    total_coating += ex.get("coating_recognition_rate", 0.0)
                    total_standard += ex.get("standard_recognition_rate", 0.0)
            if n_steps:
                m.field_recognition_rate = round(total_fr / n_steps, 4)
                m.size_recognition_rate = round(total_size / n_steps, 4)
                m.strength_recognition_rate = round(total_strength / n_steps, 4)
                m.coating_recognition_rate = round(total_coating / n_steps, 4)
                m.standard_recognition_rate = round(total_standard / n_steps, 4)

        # Correction rate from match_feedback
        run_ids = [r.id for r in runs]
        feedbacks = (
            session.query(MatchFeedback)
            .filter(MatchFeedback.pipeline_run_id.in_(run_ids))
            .all()
        ) if run_ids else []
        m.total_feedbacks = len(feedbacks)
        overrides = sum(1 for f in feedbacks if not f.is_correct)
        m.correction_rate = _safe_div(overrides, sum_auto) if sum_auto else 0.0

        return m
    finally:
        if close:
            session.close()


def compute_field_recognition(parsed_lines: list[dict]) -> dict:
    """Compute field recognition rates for a batch of parsed lines.

    Returns dict with field_recognition_rate, size/strength/coating/standard rates.
    """
    if not parsed_lines:
        return {}

    total = len(parsed_lines)
    total_fields_found = 0
    total_fields_expected = total * len(_EXPECTED_FIELDS)

    counts = {k: 0 for k in _RECOGNITION_FIELDS}

    for p in parsed_lines:
        for f in _EXPECTED_FIELDS:
            if p.get(f):
                total_fields_found += 1
        for key, fields in _RECOGNITION_FIELDS.items():
            if any(p.get(f) for f in fields):
                counts[key] += 1

    return {
        "field_recognition_rate": _safe_div(total_fields_found, total_fields_expected),
        "size_recognition_rate": _safe_div(counts["size"], total),
        "strength_recognition_rate": _safe_div(counts["strength"], total),
        "coating_recognition_rate": _safe_div(counts["coating"], total),
        "standard_recognition_rate": _safe_div(counts["standard"], total),
    }

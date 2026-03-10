"""ORM models for the Quality Monitoring Pipeline."""

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text,
    ForeignKey, Index,
)

from app.database import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    order_id = Column(Integer, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    settings_version = Column(String(50), nullable=True)

    # Client request parsing
    total_client_lines = Column(Integer, nullable=False, default=0)
    parsed_client_lines = Column(Integer, nullable=False, default=0)

    # Supplier quote parsing
    total_supplier_lines = Column(Integer, nullable=False, default=0)
    parsed_supplier_lines = Column(Integer, nullable=False, default=0)

    # Catalog matching
    auto_matches = Column(Integer, nullable=False, default=0)
    manual_matches = Column(Integer, nullable=False, default=0)

    # Supplier quote matching
    auto_supplier_matches = Column(Integer, nullable=False, default=0)
    manual_supplier_matches = Column(Integer, nullable=False, default=0)

    # Accuracy (filled after user feedback)
    system_match_correct = Column(Integer, nullable=False, default=0)
    system_match_incorrect = Column(Integer, nullable=False, default=0)

    notes = Column(Text, nullable=True)


class PipelineStep(Base):
    __tablename__ = "pipeline_steps"
    __table_args__ = (
        Index("ix_pipeline_steps_run_step", "pipeline_run_id", "step_name"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(Integer, ForeignKey("pipeline_runs.id", ondelete="CASCADE"),
                             nullable=False, index=True)
    step_name = Column(String(50), nullable=False)

    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)

    input_rows = Column(Integer, nullable=False, default=0)
    output_rows = Column(Integer, nullable=False, default=0)
    success_rate = Column(Float, nullable=True)

    extra_json = Column(Text, nullable=True)

    @property
    def extra(self) -> dict:
        try:
            return json.loads(self.extra_json or "{}")
        except (ValueError, TypeError):
            return {}

    @extra.setter
    def extra(self, value: dict) -> None:
        self.extra_json = json.dumps(value, ensure_ascii=False)


class MatchFeedback(Base):
    __tablename__ = "match_feedback"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(Integer, ForeignKey("pipeline_runs.id", ondelete="SET NULL"),
                             nullable=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    client_line_id = Column(Integer, nullable=True)

    system_choice_id = Column(Integer, nullable=True)
    user_choice_id = Column(Integer, nullable=True)
    match_score = Column(Float, nullable=True)
    is_correct = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class SettingsVersion(Base):
    __tablename__ = "system_settings_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version_code = Column(String(50), nullable=False, unique=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    description = Column(Text, nullable=True)
    settings_snapshot_json = Column(Text, nullable=True)

    @property
    def snapshot(self) -> dict:
        try:
            return json.loads(self.settings_snapshot_json or "{}")
        except (ValueError, TypeError):
            return {}

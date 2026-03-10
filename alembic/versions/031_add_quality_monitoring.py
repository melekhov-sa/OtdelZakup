"""Add quality monitoring tables: pipeline_runs, pipeline_steps, match_feedback, system_settings_versions.

Revision ID: 031
Revises: 030
"""
from alembic import op
import sqlalchemy as sa

revision = "031"
down_revision = "030"


def upgrade():
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="SET NULL"), nullable=True),
        sa.Column("settings_version", sa.String(50), nullable=True),
        sa.Column("total_client_lines", sa.Integer, nullable=False, server_default="0"),
        sa.Column("parsed_client_lines", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_supplier_lines", sa.Integer, nullable=False, server_default="0"),
        sa.Column("parsed_supplier_lines", sa.Integer, nullable=False, server_default="0"),
        sa.Column("auto_matches", sa.Integer, nullable=False, server_default="0"),
        sa.Column("manual_matches", sa.Integer, nullable=False, server_default="0"),
        sa.Column("auto_supplier_matches", sa.Integer, nullable=False, server_default="0"),
        sa.Column("manual_supplier_matches", sa.Integer, nullable=False, server_default="0"),
        sa.Column("system_match_correct", sa.Integer, nullable=False, server_default="0"),
        sa.Column("system_match_incorrect", sa.Integer, nullable=False, server_default="0"),
        sa.Column("notes", sa.Text, nullable=True),
    )
    op.create_index("ix_pipeline_runs_order_id", "pipeline_runs", ["order_id"])

    op.create_table(
        "pipeline_steps",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pipeline_run_id", sa.Integer, sa.ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("step_name", sa.String(50), nullable=False),
        sa.Column("started_at", sa.DateTime, nullable=True),
        sa.Column("finished_at", sa.DateTime, nullable=True),
        sa.Column("duration_ms", sa.Integer, nullable=True),
        sa.Column("input_rows", sa.Integer, nullable=False, server_default="0"),
        sa.Column("output_rows", sa.Integer, nullable=False, server_default="0"),
        sa.Column("success_rate", sa.Float, nullable=True),
        sa.Column("extra_json", sa.Text, nullable=True),
    )
    op.create_index("ix_pipeline_steps_run_id", "pipeline_steps", ["pipeline_run_id"])
    op.create_index("ix_pipeline_steps_run_step", "pipeline_steps", ["pipeline_run_id", "step_name"])

    op.create_table(
        "match_feedback",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pipeline_run_id", sa.Integer, sa.ForeignKey("pipeline_runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="SET NULL"), nullable=True),
        sa.Column("client_line_id", sa.Integer, nullable=True),
        sa.Column("system_choice_id", sa.Integer, nullable=True),
        sa.Column("user_choice_id", sa.Integer, nullable=True),
        sa.Column("match_score", sa.Float, nullable=True),
        sa.Column("is_correct", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_match_feedback_run_id", "match_feedback", ["pipeline_run_id"])
    op.create_index("ix_match_feedback_order_id", "match_feedback", ["order_id"])

    op.create_table(
        "system_settings_versions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("version_code", sa.String(50), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("settings_snapshot_json", sa.Text, nullable=True),
    )


def downgrade():
    op.drop_table("system_settings_versions")
    op.drop_table("match_feedback")
    op.drop_table("pipeline_steps")
    op.drop_table("pipeline_runs")

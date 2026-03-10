"""Add benchmark tables: datasets, cases, expected_results, runs, run_rows.

Revision ID: 032
Revises: 031
"""
from alembic import op
import sqlalchemy as sa

revision = "032"
down_revision = "031"


def upgrade():
    op.create_table(
        "benchmark_datasets",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(300), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="1"),
    )

    op.create_table(
        "benchmark_cases",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("dataset_id", sa.Integer, sa.ForeignKey("benchmark_datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(300), nullable=False),
        sa.Column("source_type", sa.String(30), nullable=False, server_default="client_text"),
        sa.Column("input_data", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_bench_cases_dataset", "benchmark_cases", ["dataset_id"])

    op.create_table(
        "benchmark_expected_results",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("benchmark_case_id", sa.Integer, sa.ForeignKey("benchmark_cases.id", ondelete="CASCADE"), nullable=False),
        sa.Column("row_index", sa.Integer, nullable=False, server_default="0"),
        sa.Column("expected_item_type", sa.String(100), nullable=True),
        sa.Column("expected_standard", sa.String(200), nullable=True),
        sa.Column("expected_size", sa.String(100), nullable=True),
        sa.Column("expected_strength", sa.String(50), nullable=True),
        sa.Column("expected_coating", sa.String(100), nullable=True),
        sa.Column("expected_catalog_item_id", sa.Integer, nullable=True),
        sa.Column("expected_supplier_item_name", sa.String(500), nullable=True),
        sa.Column("expected_price", sa.Float, nullable=True),
        sa.Column("expected_unit", sa.String(50), nullable=True),
    )
    op.create_index("ix_bench_expected_case", "benchmark_expected_results", ["benchmark_case_id"])
    op.create_index("ix_bench_expected_case_row", "benchmark_expected_results", ["benchmark_case_id", "row_index"])

    op.create_table(
        "benchmark_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("dataset_id", sa.Integer, sa.ForeignKey("benchmark_datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("settings_version", sa.String(50), nullable=True),
        sa.Column("started_at", sa.DateTime, nullable=False),
        sa.Column("finished_at", sa.DateTime, nullable=True),
        sa.Column("total_rows", sa.Integer, nullable=False, server_default="0"),
        sa.Column("parse_accuracy", sa.Float, nullable=True),
        sa.Column("catalog_match_accuracy", sa.Float, nullable=True),
        sa.Column("supplier_parse_accuracy", sa.Float, nullable=True),
        sa.Column("supplier_match_accuracy", sa.Float, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
    )
    op.create_index("ix_bench_runs_dataset", "benchmark_runs", ["dataset_id"])

    op.create_table(
        "benchmark_run_rows",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("benchmark_run_id", sa.Integer, sa.ForeignKey("benchmark_runs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("benchmark_case_id", sa.Integer, nullable=False),
        sa.Column("row_index", sa.Integer, nullable=False, server_default="0"),
        sa.Column("raw_text", sa.Text, nullable=True),
        sa.Column("system_item_type", sa.String(100), nullable=True),
        sa.Column("system_standard", sa.String(200), nullable=True),
        sa.Column("system_size", sa.String(100), nullable=True),
        sa.Column("system_strength", sa.String(50), nullable=True),
        sa.Column("system_coating", sa.String(100), nullable=True),
        sa.Column("system_catalog_item_id", sa.Integer, nullable=True),
        sa.Column("system_price", sa.Float, nullable=True),
        sa.Column("system_unit", sa.String(50), nullable=True),
        sa.Column("correct_item_type", sa.Boolean, nullable=True),
        sa.Column("correct_standard", sa.Boolean, nullable=True),
        sa.Column("correct_size", sa.Boolean, nullable=True),
        sa.Column("correct_strength", sa.Boolean, nullable=True),
        sa.Column("correct_coating", sa.Boolean, nullable=True),
        sa.Column("correct_catalog_match", sa.Boolean, nullable=True),
        sa.Column("correct_price", sa.Boolean, nullable=True),
        sa.Column("correct_unit", sa.Boolean, nullable=True),
    )
    op.create_index("ix_bench_run_rows_run", "benchmark_run_rows", ["benchmark_run_id"])


def downgrade():
    op.drop_table("benchmark_run_rows")
    op.drop_table("benchmark_runs")
    op.drop_table("benchmark_expected_results")
    op.drop_table("benchmark_cases")
    op.drop_table("benchmark_datasets")

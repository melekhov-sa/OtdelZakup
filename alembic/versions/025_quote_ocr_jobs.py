"""Add QuoteOcrJob and QuoteOcrTable tables.

Revision ID: 025
Revises: 024
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def _table_exists(name):
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"
    ), {"n": name})
    return result.scalar() is not None


def upgrade():
    if _table_exists("quote_ocr_jobs") and _table_exists("quote_ocr_tables"):
        return

    if not _table_exists("quote_ocr_jobs"):
        op.create_table(
            "quote_ocr_jobs",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
            sa.Column("filename", sa.String(300), nullable=True),
            sa.Column("content_type", sa.String(100), nullable=True),
            sa.Column("processor_type", sa.String(50), nullable=True),
            sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
            sa.Column("error", sa.Text, nullable=True),
            sa.Column("page_count", sa.Integer, nullable=True),
            sa.Column("tables_found", sa.Integer, nullable=True),
            sa.Column("confidence_avg", sa.Float, nullable=True),
        )

    if not _table_exists("quote_ocr_tables"):
        op.create_table(
            "quote_ocr_tables",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("job_id", sa.Integer,
                      sa.ForeignKey("quote_ocr_jobs.id", ondelete="CASCADE"), nullable=False),
            sa.Column("table_index", sa.Integer, nullable=False, server_default="0"),
            sa.Column("page_no", sa.Integer, nullable=True),
            sa.Column("n_rows", sa.Integer, nullable=False, server_default="0"),
            sa.Column("n_cols", sa.Integer, nullable=False, server_default="0"),
            sa.Column("confidence_avg", sa.Float, nullable=True),
            sa.Column("raw_json", sa.Text, nullable=False, server_default="[]"),
        )
        op.create_index("ix_quote_ocr_tables_job_id", "quote_ocr_tables", ["job_id"])


def downgrade():
    op.drop_table("quote_ocr_tables")
    op.drop_table("quote_ocr_jobs")

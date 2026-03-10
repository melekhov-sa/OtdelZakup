"""Add raw table storage for quotes + extra fields on quote_lines.

New tables: quote_tables, quote_table_rows
New columns on quote_lines: price_total, raw_cells_json, raw_qty_unit_text,
    raw_price_text, raw_sum_text

Revision ID: 024
Revises: 023
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def _table_exists(name):
    conn = op.get_bind()
    result = conn.execute(sa.text(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"
    ), {"n": name})
    return result.scalar() is not None


def _column_exists(table, column):
    conn = op.get_bind()
    result = conn.execute(sa.text(f"PRAGMA table_info('{table}')"))
    return any(row[1] == column for row in result)


def upgrade():
    if not _table_exists("quote_tables"):
        op.create_table(
            "quote_tables",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("quote_id", sa.Integer, sa.ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False),
            sa.Column("page_no", sa.Integer, nullable=False, server_default="1"),
            sa.Column("table_no", sa.Integer, nullable=False, server_default="1"),
            sa.Column("n_rows", sa.Integer, nullable=False, server_default="0"),
            sa.Column("n_cols", sa.Integer, nullable=False, server_default="0"),
            sa.Column("headers_json", sa.Text, nullable=True),
            sa.Column("source", sa.String(50), nullable=True),
            sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
        op.create_index("ix_quote_tables_quote_id", "quote_tables", ["quote_id"])

    if not _table_exists("quote_table_rows"):
        op.create_table(
            "quote_table_rows",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("quote_table_id", sa.Integer,
                      sa.ForeignKey("quote_tables.id", ondelete="CASCADE"), nullable=False),
            sa.Column("row_index", sa.Integer, nullable=False),
            sa.Column("cells_json", sa.Text, nullable=False, server_default="[]"),
        )
        op.create_index("ix_quote_table_rows_table_id", "quote_table_rows", ["quote_table_id"])

    with op.batch_alter_table("quote_lines") as batch_op:
        if not _column_exists("quote_lines", "price_total"):
            batch_op.add_column(sa.Column("price_total", sa.Float, nullable=True))
        if not _column_exists("quote_lines", "raw_cells_json"):
            batch_op.add_column(sa.Column("raw_cells_json", sa.Text, nullable=True))
        if not _column_exists("quote_lines", "raw_qty_unit_text"):
            batch_op.add_column(sa.Column("raw_qty_unit_text", sa.String(200), nullable=True))
        if not _column_exists("quote_lines", "raw_price_text"):
            batch_op.add_column(sa.Column("raw_price_text", sa.String(200), nullable=True))
        if not _column_exists("quote_lines", "raw_sum_text"):
            batch_op.add_column(sa.Column("raw_sum_text", sa.String(200), nullable=True))


def downgrade():
    with op.batch_alter_table("quote_lines") as batch_op:
        batch_op.drop_column("raw_sum_text")
        batch_op.drop_column("raw_price_text")
        batch_op.drop_column("raw_qty_unit_text")
        batch_op.drop_column("raw_cells_json")
        batch_op.drop_column("price_total")

    op.drop_table("quote_table_rows")
    op.drop_table("quote_tables")

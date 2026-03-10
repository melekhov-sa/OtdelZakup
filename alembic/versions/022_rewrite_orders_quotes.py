"""Rewrite orders & quotes schema.

Drop old 5 tables, create 7 new tables with full workflow support.

Revision ID: 022
Revises: 021
Create Date: 2026-03-04
"""
from alembic import op
import sqlalchemy as sa

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade():
    # Drop old tables in FK-safe order
    op.drop_table("match_links")
    op.drop_table("quote_lines")
    op.drop_table("quotes")
    op.drop_table("order_lines")
    op.drop_table("orders")

    # Create new tables
    op.create_table(
        "suppliers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(300), nullable=False, unique=True),
    )

    op.create_table(
        "orders",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(300), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )

    op.create_table(
        "client_lines",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("row_no", sa.Integer, nullable=False),
        sa.Column("raw_text", sa.Text, nullable=False),
        sa.Column("qty", sa.Float, nullable=True),
        sa.Column("unit", sa.String(50), nullable=True),
        sa.Column("parsed_json", sa.Text, nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="ok"),
        sa.Column("chosen_catalog_item_id", sa.Integer,
                   sa.ForeignKey("internal_item.id", ondelete="SET NULL"), nullable=True),
        sa.Column("chosen_by", sa.String(20), nullable=True),
        sa.Column("chosen_at", sa.DateTime, nullable=True),
    )
    op.create_index("ix_client_lines_order_id", "client_lines", ["order_id"])
    op.create_index("ix_client_lines_chosen_item", "client_lines", ["chosen_catalog_item_id"])

    op.create_table(
        "order_items",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("catalog_item_id", sa.Integer,
                   sa.ForeignKey("internal_item.id", ondelete="SET NULL"), nullable=True),
        sa.Column("display_name_snapshot", sa.Text, nullable=False),
        sa.Column("type_norm", sa.String(50), nullable=True),
        sa.Column("size_norm", sa.String(100), nullable=True),
        sa.Column("std_norm", sa.String(120), nullable=True),
        sa.Column("tokens_norm", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_order_items_order_id", "order_items", ["order_id"])
    op.create_index("ix_order_items_catalog_item_id", "order_items", ["catalog_item_id"])

    op.create_table(
        "quotes",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("supplier_id", sa.Integer, sa.ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_filename", sa.String(300), nullable=True),
        sa.Column("source_kind", sa.String(20), nullable=False, server_default="excel"),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_quotes_order_id", "quotes", ["order_id"])
    op.create_index("ix_quotes_supplier_id", "quotes", ["supplier_id"])

    op.create_table(
        "quote_lines",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("quote_id", sa.Integer, sa.ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("row_no", sa.Integer, nullable=False),
        sa.Column("raw_text", sa.Text, nullable=False),
        sa.Column("price", sa.Float, nullable=True),
        sa.Column("currency", sa.String(10), nullable=False, server_default="RUB"),
        sa.Column("unit", sa.String(50), nullable=True),
        sa.Column("parsed_json", sa.Text, nullable=True),
        sa.Column("type_norm", sa.String(50), nullable=True),
        sa.Column("size_norm", sa.String(100), nullable=True),
        sa.Column("std_norm", sa.String(120), nullable=True),
        sa.Column("tokens_norm", sa.Text, nullable=True),
    )
    op.create_index("ix_quote_lines_quote_id", "quote_lines", ["quote_id"])

    op.create_table(
        "quote_matches",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_item_id", sa.Integer,
                   sa.ForeignKey("order_items.id", ondelete="CASCADE"), nullable=False),
        sa.Column("quote_line_id", sa.Integer,
                   sa.ForeignKey("quote_lines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("jaccard", sa.Float, nullable=True),
        sa.Column("match_mode", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_quote_matches_order_item_id", "quote_matches", ["order_item_id"])
    op.create_index("uq_quote_match_ql", "quote_matches", ["quote_line_id"], unique=True)


def downgrade():
    op.drop_table("quote_matches")
    op.drop_table("quote_lines")
    op.drop_table("quotes")
    op.drop_table("order_items")
    op.drop_table("client_lines")
    op.drop_table("orders")
    op.drop_table("suppliers")

    # Recreate old tables (from migration 021)
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.String(300), nullable=False),
        sa.Column("customer_name", sa.String(300), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_table(
        "order_lines",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("product_id", sa.Integer, sa.ForeignKey("internal_item.id", ondelete="SET NULL"), nullable=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("type_norm", sa.String(50), nullable=True),
        sa.Column("size_norm", sa.String(100), nullable=True),
        sa.Column("std_norm", sa.String(120), nullable=True),
        sa.Column("tokens_norm", sa.Text, nullable=True),
    )
    op.create_index("ix_order_lines_order_id", "order_lines", ["order_id"])
    op.create_index("ix_order_lines_product_id", "order_lines", ["product_id"])
    op.create_table(
        "quotes",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer, sa.ForeignKey("orders.id", ondelete="CASCADE"), nullable=False),
        sa.Column("supplier_name", sa.String(300), nullable=False),
        sa.Column("filename", sa.String(300), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index("ix_quotes_order_id", "quotes", ["order_id"])
    op.create_table(
        "quote_lines",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("quote_id", sa.Integer, sa.ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("row_no", sa.Integer, nullable=False),
        sa.Column("raw_name", sa.Text, nullable=False),
        sa.Column("qty", sa.Float, nullable=True),
        sa.Column("unit", sa.String(30), nullable=True),
        sa.Column("price", sa.Float, nullable=True),
        sa.Column("type_norm", sa.String(50), nullable=True),
        sa.Column("size_norm", sa.String(100), nullable=True),
        sa.Column("std_norm", sa.String(120), nullable=True),
        sa.Column("tokens_norm", sa.Text, nullable=True),
    )
    op.create_index("ix_quote_lines_quote_id", "quote_lines", ["quote_id"])
    op.create_table(
        "match_links",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("order_line_id", sa.Integer, sa.ForeignKey("order_lines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("quote_line_id", sa.Integer, sa.ForeignKey("quote_lines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("jaccard", sa.Float, nullable=True),
        sa.Column("match_mode", sa.String(20), nullable=False),
    )
    op.create_index("ix_match_links_order_line_id", "match_links", ["order_line_id"])
    op.create_index("uq_match_link_quote_line", "match_links", ["quote_line_id"], unique=True)

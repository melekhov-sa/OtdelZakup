"""ORM models for the Orders & Quote Comparison module."""

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Column, DateTime, Float, Integer, String, Text,
    UniqueConstraint, ForeignKey,
)

from app.database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(300), nullable=False)
    status = Column(String(30), nullable=False, default="draft")
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, nullable=False,
                        default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class ClientLine(Base):
    __tablename__ = "client_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    row_no = Column(Integer, nullable=False)
    raw_text = Column(Text, nullable=False)
    qty = Column(Float, nullable=True)
    unit = Column(String(50), nullable=True)
    parsed_json = Column(Text, nullable=True)
    status = Column(String(20), nullable=False, default="ok")
    chosen_catalog_item_id = Column(Integer, ForeignKey("internal_item.id", ondelete="SET NULL"),
                                    nullable=True, index=True)
    chosen_by = Column(String(20), nullable=True)
    chosen_at = Column(DateTime, nullable=True)

    @property
    def parsed(self) -> dict:
        try:
            return json.loads(self.parsed_json or "{}")
        except (ValueError, TypeError):
            return {}


class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    catalog_item_id = Column(Integer, ForeignKey("internal_item.id", ondelete="SET NULL"),
                             nullable=True, index=True)
    display_name_snapshot = Column(Text, nullable=False)
    type_norm = Column(String(50), nullable=True)
    size_norm = Column(String(100), nullable=True)
    std_norm = Column(String(120), nullable=True)
    tokens_norm = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(300), nullable=False, unique=True)


class Quote(Base):
    __tablename__ = "quotes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    supplier_id = Column(Integer, ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False, index=True)
    source_filename = Column(String(300), nullable=True)
    source_kind = Column(String(20), nullable=False, default="excel")
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class QuoteLine(Base):
    __tablename__ = "quote_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    quote_id = Column(Integer, ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False, index=True)
    row_no = Column(Integer, nullable=False)
    raw_text = Column(Text, nullable=False)
    price = Column(Float, nullable=True)         # price per unit
    price_total = Column(Float, nullable=True)   # total price for the line
    currency = Column(String(10), nullable=False, default="RUB")
    qty = Column(Float, nullable=True)
    unit = Column(String(50), nullable=True)
    parsed_json = Column(Text, nullable=True)
    type_norm = Column(String(50), nullable=True)
    size_norm = Column(String(100), nullable=True)
    std_norm = Column(String(120), nullable=True)
    tokens_norm = Column(Text, nullable=True)
    line_class = Column(String(20), nullable=True)      # item/header/total/requisites/garbage
    filter_reason = Column(String(200), nullable=True)   # human-readable classification reason
    raw_cells_json = Column(Text, nullable=True)         # original row cells from table
    raw_qty_unit_text = Column(String(200), nullable=True)  # original qty/unit cell text
    raw_price_text = Column(String(200), nullable=True)     # original price cell text
    raw_sum_text = Column(String(200), nullable=True)       # original sum cell text

    @property
    def parsed(self) -> dict:
        try:
            return json.loads(self.parsed_json or "{}")
        except (ValueError, TypeError):
            return {}


class QuoteTable(Base):
    """Raw OCR/Excel table snapshot — preserves all original columns."""
    __tablename__ = "quote_tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    quote_id = Column(Integer, ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False, index=True)
    page_no = Column(Integer, nullable=False, default=1)
    table_no = Column(Integer, nullable=False, default=1)
    n_rows = Column(Integer, nullable=False, default=0)
    n_cols = Column(Integer, nullable=False, default=0)
    headers_json = Column(Text, nullable=True)   # ["Col1","Col2",...] or null
    source = Column(String(50), nullable=True)    # gcp_form_parser, excel, csv, ...
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    @property
    def headers(self) -> list[str]:
        try:
            return json.loads(self.headers_json or "[]")
        except (ValueError, TypeError):
            return []


class QuoteTableRow(Base):
    """Single row in a raw QuoteTable."""
    __tablename__ = "quote_table_rows"

    id = Column(Integer, primary_key=True, autoincrement=True)
    quote_table_id = Column(Integer, ForeignKey("quote_tables.id", ondelete="CASCADE"),
                            nullable=False, index=True)
    row_index = Column(Integer, nullable=False)
    cells_json = Column(Text, nullable=False, default="[]")

    @property
    def cells(self) -> list[str]:
        try:
            return json.loads(self.cells_json or "[]")
        except (ValueError, TypeError):
            return []


class QuoteOcrJob(Base):
    """A single OCR processing job for a quote/invoice file."""
    __tablename__ = "quote_ocr_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    filename = Column(String(300), nullable=True)
    content_type = Column(String(100), nullable=True)
    processor_type = Column(String(50), nullable=True)
    status = Column(String(20), nullable=False, default="pending")  # pending/done/error
    error = Column(Text, nullable=True)
    page_count = Column(Integer, nullable=True)
    tables_found = Column(Integer, nullable=True)
    confidence_avg = Column(Float, nullable=True)


class QuoteOcrTable(Base):
    """A single table extracted from OCR — stores raw cell grid."""
    __tablename__ = "quote_ocr_tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("quote_ocr_jobs.id", ondelete="CASCADE"),
                    nullable=False, index=True)
    table_index = Column(Integer, nullable=False, default=0)
    page_no = Column(Integer, nullable=True)
    n_rows = Column(Integer, nullable=False, default=0)
    n_cols = Column(Integer, nullable=False, default=0)
    confidence_avg = Column(Float, nullable=True)
    raw_json = Column(Text, nullable=False, default="[]")  # [[cell,...],...]

    @property
    def rows(self) -> list[list[str]]:
        try:
            return json.loads(self.raw_json or "[]")
        except (ValueError, TypeError):
            return []


class QuoteMatch(Base):
    __tablename__ = "quote_matches"
    __table_args__ = (UniqueConstraint("quote_line_id", name="uq_quote_match_ql"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_item_id = Column(Integer, ForeignKey("order_items.id", ondelete="CASCADE"),
                           nullable=False, index=True)
    quote_line_id = Column(Integer, ForeignKey("quote_lines.id", ondelete="CASCADE"), nullable=False)
    jaccard = Column(Float, nullable=True)
    match_mode = Column(String(20), nullable=False)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

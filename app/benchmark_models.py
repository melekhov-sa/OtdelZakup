"""ORM models for the Benchmark Engine."""

import json
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text,
    ForeignKey, Index,
)

from app.database import Base


class BenchmarkDataset(Base):
    __tablename__ = "benchmark_datasets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(300), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, nullable=False, default=True)


class BenchmarkCase(Base):
    __tablename__ = "benchmark_cases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey("benchmark_datasets.id", ondelete="CASCADE"),
                        nullable=False, index=True)
    name = Column(String(300), nullable=False)
    source_type = Column(String(30), nullable=False, default="client_text")
    # source_type: client_text, client_excel, client_pdf, client_image,
    #              supplier_text, supplier_excel, supplier_pdf
    input_data = Column(Text, nullable=False)  # raw text or JSON
    created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class BenchmarkExpectedResult(Base):
    __tablename__ = "benchmark_expected_results"
    __table_args__ = (
        Index("ix_bench_expected_case_row", "benchmark_case_id", "row_index"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    benchmark_case_id = Column(Integer, ForeignKey("benchmark_cases.id", ondelete="CASCADE"),
                               nullable=False, index=True)
    row_index = Column(Integer, nullable=False, default=0)

    # Parsing expected results
    expected_item_type = Column(String(100), nullable=True)
    expected_standard = Column(String(200), nullable=True)
    expected_size = Column(String(100), nullable=True)
    expected_strength = Column(String(50), nullable=True)
    expected_coating = Column(String(100), nullable=True)

    # Catalog match expected
    expected_catalog_item_id = Column(Integer, nullable=True)

    # Supplier line expected (for quote cases)
    expected_supplier_item_name = Column(String(500), nullable=True)
    expected_price = Column(Float, nullable=True)
    expected_unit = Column(String(50), nullable=True)


class BenchmarkRun(Base):
    __tablename__ = "benchmark_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey("benchmark_datasets.id", ondelete="CASCADE"),
                        nullable=False, index=True)
    settings_version = Column(String(50), nullable=True)

    started_at = Column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    finished_at = Column(DateTime, nullable=True)

    total_rows = Column(Integer, nullable=False, default=0)
    parse_accuracy = Column(Float, nullable=True)
    catalog_match_accuracy = Column(Float, nullable=True)
    supplier_parse_accuracy = Column(Float, nullable=True)
    supplier_match_accuracy = Column(Float, nullable=True)

    notes = Column(Text, nullable=True)


class BenchmarkRunRow(Base):
    __tablename__ = "benchmark_run_rows"
    __table_args__ = (
        Index("ix_bench_run_rows_run", "benchmark_run_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    benchmark_run_id = Column(Integer, ForeignKey("benchmark_runs.id", ondelete="CASCADE"),
                              nullable=False)
    benchmark_case_id = Column(Integer, nullable=False)
    row_index = Column(Integer, nullable=False, default=0)
    raw_text = Column(Text, nullable=True)

    # System results
    system_item_type = Column(String(100), nullable=True)
    system_standard = Column(String(200), nullable=True)
    system_size = Column(String(100), nullable=True)
    system_strength = Column(String(50), nullable=True)
    system_coating = Column(String(100), nullable=True)
    system_catalog_item_id = Column(Integer, nullable=True)
    system_price = Column(Float, nullable=True)
    system_unit = Column(String(50), nullable=True)

    # Per-field correctness
    correct_item_type = Column(Boolean, nullable=True)
    correct_standard = Column(Boolean, nullable=True)
    correct_size = Column(Boolean, nullable=True)
    correct_strength = Column(Boolean, nullable=True)
    correct_coating = Column(Boolean, nullable=True)
    correct_catalog_match = Column(Boolean, nullable=True)
    correct_price = Column(Boolean, nullable=True)
    correct_unit = Column(Boolean, nullable=True)

    @property
    def errors(self) -> list[str]:
        """Return list of field names where system != expected."""
        errs = []
        for f in ("item_type", "standard", "size", "strength", "coating",
                   "catalog_match", "price", "unit"):
            val = getattr(self, f"correct_{f}", None)
            if val is False:
                errs.append(f)
        return errs

"""Benchmark Engine — runs benchmark datasets against current algorithms.

Processes each BenchmarkCase:
1. Parses input_data using parse_raw_line (same as main pipeline)
2. Runs decide_match for catalog matching
3. Compares system output with BenchmarkExpectedResult
4. Computes per-field correctness and aggregate accuracy
"""
from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import dataclass

from app.database import get_db_session
from app.benchmark_models import (
    BenchmarkCase, BenchmarkDataset, BenchmarkExpectedResult,
    BenchmarkRun, BenchmarkRunRow,
)


def _norm(val: str | None) -> str:
    """Normalize a value for comparison: lowercase, strip."""
    if not val:
        return ""
    return val.strip().lower()


def _compare_field(system_val: str | None, expected_val: str | None) -> bool | None:
    """Compare two field values. Returns None if expected is empty (not evaluated)."""
    if not expected_val:
        return None  # no expectation set
    return _norm(system_val) == _norm(expected_val)


def _compare_id(system_id: int | None, expected_id: int | None) -> bool | None:
    if expected_id is None:
        return None
    return system_id == expected_id


def _compare_price(system_price: float | None, expected_price: float | None) -> bool | None:
    if expected_price is None:
        return None
    if system_price is None:
        return False
    return abs(system_price - expected_price) < 0.01


def run_benchmark(dataset_id: int, session=None) -> BenchmarkRun:
    """Run a full benchmark against the given dataset.

    Returns the BenchmarkRun with computed accuracy metrics.
    """
    close = session is None
    if session is None:
        session = get_db_session()

    try:
        dataset = session.get(BenchmarkDataset, dataset_id)
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} not found")

        # Get settings version
        settings_version = None
        try:
            from app.quality_models import SettingsVersion
            sv = session.query(SettingsVersion).order_by(SettingsVersion.id.desc()).first()
            if sv:
                settings_version = sv.version_code
        except Exception:
            pass

        run = BenchmarkRun(
            dataset_id=dataset_id,
            settings_version=settings_version,
            started_at=datetime.now(timezone.utc),
        )
        session.add(run)
        session.flush()

        # Load cases and expected results
        cases = session.query(BenchmarkCase).filter_by(dataset_id=dataset_id).all()

        # Preload catalog items for matching
        from app.models import InternalItem
        from app.match_settings import load_match_settings
        from app.matcher import decide_match

        all_items = session.query(InternalItem).filter_by(is_active=True).all()
        item_by_id = {item.id: item for item in all_items}
        settings = load_match_settings()

        # Counters for accuracy
        parse_checks = 0
        parse_correct = 0
        catalog_checks = 0
        catalog_correct = 0
        supplier_parse_checks = 0
        supplier_parse_correct = 0
        supplier_match_checks = 0
        supplier_match_correct = 0
        total_rows = 0

        for case in cases:
            expected_rows = (
                session.query(BenchmarkExpectedResult)
                .filter_by(benchmark_case_id=case.id)
                .order_by(BenchmarkExpectedResult.row_index)
                .all()
            )
            if not expected_rows:
                continue

            is_supplier = case.source_type.startswith("supplier_")

            # Parse input lines
            input_lines = _parse_input_lines(case)

            for exp in expected_rows:
                total_rows += 1
                idx = exp.row_index

                raw_text = input_lines[idx] if idx < len(input_lines) else ""

                # Run parse
                from app.services.line_parser import parse_raw_line
                parsed = parse_raw_line(raw_text) if raw_text else {}

                # Build standard from parsed
                system_standard = ""
                for k in ("gost", "din", "iso"):
                    v = parsed.get(k, "")
                    if v:
                        system_standard = v
                        break

                # Compare parse fields
                c_type = _compare_field(parsed.get("item_type"), exp.expected_item_type)
                c_std = _compare_field(system_standard, exp.expected_standard)
                c_size = _compare_field(parsed.get("size_norm") or parsed.get("size"), exp.expected_size)
                c_strength = _compare_field(parsed.get("strength"), exp.expected_strength)
                c_coating = _compare_field(parsed.get("coating"), exp.expected_coating)

                # Count parse accuracy
                for c in (c_type, c_std, c_size, c_strength, c_coating):
                    if c is not None:
                        if is_supplier:
                            supplier_parse_checks += 1
                            if c:
                                supplier_parse_correct += 1
                        else:
                            parse_checks += 1
                            if c:
                                parse_correct += 1

                # Catalog match
                c_catalog = None
                system_catalog_id = None
                if exp.expected_catalog_item_id is not None and not is_supplier:
                    row_dict = {
                        "name": raw_text,
                        "name_raw": raw_text,
                        "item_type": parsed.get("item_type", ""),
                        "size": parsed.get("size", ""),
                        "diameter": parsed.get("diameter", ""),
                        "length": parsed.get("length", ""),
                        "gost": parsed.get("gost", ""),
                        "iso": parsed.get("iso", ""),
                        "din": parsed.get("din", ""),
                        "strength": parsed.get("strength", ""),
                        "coating": parsed.get("coating", ""),
                    }
                    match_result = decide_match(
                        row_dict, settings, session,
                        all_items=all_items, item_by_id=item_by_id,
                    )
                    system_catalog_id = match_result.get("internal_item_id")
                    c_catalog = _compare_id(system_catalog_id, exp.expected_catalog_item_id)
                    catalog_checks += 1
                    if c_catalog:
                        catalog_correct += 1

                # Supplier match (catalog match for supplier lines)
                if exp.expected_catalog_item_id is not None and is_supplier:
                    row_dict = {
                        "name": raw_text,
                        "name_raw": raw_text,
                        "item_type": parsed.get("item_type", ""),
                        "size": parsed.get("size", ""),
                        "diameter": parsed.get("diameter", ""),
                        "length": parsed.get("length", ""),
                        "gost": parsed.get("gost", ""),
                        "iso": parsed.get("iso", ""),
                        "din": parsed.get("din", ""),
                        "strength": parsed.get("strength", ""),
                        "coating": parsed.get("coating", ""),
                    }
                    match_result = decide_match(
                        row_dict, settings, session,
                        all_items=all_items, item_by_id=item_by_id,
                    )
                    system_catalog_id = match_result.get("internal_item_id")
                    c_catalog = _compare_id(system_catalog_id, exp.expected_catalog_item_id)
                    supplier_match_checks += 1
                    if c_catalog:
                        supplier_match_correct += 1

                # Price / unit for supplier lines
                c_price = None
                c_unit = None
                # (price extraction not yet in parse_raw_line — placeholder for future)

                run_row = BenchmarkRunRow(
                    benchmark_run_id=run.id,
                    benchmark_case_id=case.id,
                    row_index=idx,
                    raw_text=raw_text,
                    system_item_type=parsed.get("item_type"),
                    system_standard=system_standard or None,
                    system_size=parsed.get("size_norm") or parsed.get("size") or None,
                    system_strength=parsed.get("strength") or None,
                    system_coating=parsed.get("coating") or None,
                    system_catalog_item_id=system_catalog_id,
                    correct_item_type=c_type,
                    correct_standard=c_std,
                    correct_size=c_size,
                    correct_strength=c_strength,
                    correct_coating=c_coating,
                    correct_catalog_match=c_catalog,
                    correct_price=c_price,
                    correct_unit=c_unit,
                )
                session.add(run_row)

        # Compute aggregate metrics
        run.total_rows = total_rows
        run.parse_accuracy = round(parse_correct / parse_checks, 4) if parse_checks else None
        run.catalog_match_accuracy = round(catalog_correct / catalog_checks, 4) if catalog_checks else None
        run.supplier_parse_accuracy = round(supplier_parse_correct / supplier_parse_checks, 4) if supplier_parse_checks else None
        run.supplier_match_accuracy = round(supplier_match_correct / supplier_match_checks, 4) if supplier_match_checks else None
        run.finished_at = datetime.now(timezone.utc)

        session.commit()
        return run

    finally:
        if close:
            session.close()


def _parse_input_lines(case: BenchmarkCase) -> list[str]:
    """Extract text lines from a benchmark case input_data.

    For text-based source types, splits by newlines.
    For JSON input, extracts from list.
    """
    import json

    data = case.input_data or ""

    # Try JSON array first
    try:
        parsed = json.loads(data)
        if isinstance(parsed, list):
            return [str(item) if not isinstance(item, dict) else item.get("name", str(item))
                    for item in parsed]
    except (json.JSONDecodeError, ValueError):
        pass

    # Plain text — split by newlines
    return [line.strip() for line in data.strip().splitlines() if line.strip()]


@dataclass
class BenchmarkSummary:
    run: BenchmarkRun
    error_rows: list[BenchmarkRunRow]
    total_errors: int
    field_stats: dict  # {field_name: {checked, correct, accuracy}}


def get_run_summary(run_id: int, session=None) -> BenchmarkSummary | None:
    """Load a benchmark run with error details."""
    close = session is None
    if session is None:
        session = get_db_session()
    try:
        run = session.get(BenchmarkRun, run_id)
        if not run:
            return None

        rows = (
            session.query(BenchmarkRunRow)
            .filter_by(benchmark_run_id=run_id)
            .all()
        )

        error_rows = [r for r in rows if r.errors]

        # Per-field stats
        fields = ("item_type", "standard", "size", "strength", "coating", "catalog_match")
        field_stats = {}
        for f in fields:
            checked = sum(1 for r in rows if getattr(r, f"correct_{f}") is not None)
            correct = sum(1 for r in rows if getattr(r, f"correct_{f}") is True)
            field_stats[f] = {
                "checked": checked,
                "correct": correct,
                "accuracy": round(correct / checked, 4) if checked else None,
            }

        session.expunge_all()
        return BenchmarkSummary(
            run=run,
            error_rows=error_rows,
            total_errors=len(error_rows),
            field_stats=field_stats,
        )
    finally:
        if close:
            session.close()

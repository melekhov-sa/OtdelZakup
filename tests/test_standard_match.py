"""Tests for ValidationRule condition_type=STANDARD_MATCH."""

import pytest
from unittest.mock import MagicMock


# ── Test isolation fixture ────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod

    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── Helper: build a mock STANDARD_MATCH rule ─────────────────────────────────

def _make_sm_rule(
    standard_source="ANY",
    expected_item_type_mode="FROM_DIRECTORY",
    expected_item_type=None,
    item_type_filter=None,
):
    rule = MagicMock()
    rule.item_type = item_type_filter  # None = all types
    rule.condition_type = "STANDARD_MATCH"
    rule.standard_source = standard_source
    rule.expected_item_type_mode = expected_item_type_mode
    rule.expected_item_type = expected_item_type
    rule.require_fields_list = []
    rule.forbid_fields_list = []
    return rule


# ── Test 1: conflict from directory ──────────────────────────────────────────

def test_validation_standard_match_from_directory_conflict():
    """Row has din=DIN 934 (→ гайка per directory) but item_type=болт → rule fires."""
    from app.readiness import _check_val_rule

    standards_cache = {("DIN", "934"): ("гайка", "Гайка шестигранная")}
    rule = _make_sm_rule(standard_source="DIN", expected_item_type_mode="FROM_DIRECTORY")

    row_dict = {"item_type": "болт", "din": "DIN 934", "iso": "", "gost": ""}
    fired, reasons = _check_val_rule(row_dict, rule, standards_cache)

    assert fired is True
    assert len(reasons) == 1
    assert "гайка" in reasons[0]
    assert "болт" in reasons[0]
    assert "DIN 934" in reasons[0]


# ── Test 2: no standard in row → no trigger ───────────────────────────────────

def test_validation_standard_match_no_standard_no_trigger():
    """Row has no standard at all → rule must not fire (no data to compare)."""
    from app.readiness import _check_val_rule

    standards_cache = {("DIN", "934"): ("гайка", "Гайка шестигранная")}
    rule = _make_sm_rule(standard_source="ANY")

    row_dict = {"item_type": "болт", "din": "", "iso": "", "gost": ""}
    fired, reasons = _check_val_rule(row_dict, rule, standards_cache)

    assert fired is False
    assert reasons == []


# ── Test 3: standard present but not in directory → no trigger ───────────────

def test_validation_standard_match_no_directory_entry_no_trigger():
    """Standard is in the row but not in the directory → no reference → no trigger."""
    from app.readiness import _check_val_rule

    standards_cache = {}  # empty directory
    rule = _make_sm_rule(standard_source="DIN", expected_item_type_mode="FROM_DIRECTORY")

    row_dict = {"item_type": "болт", "din": "DIN 934", "iso": "", "gost": ""}
    fired, reasons = _check_val_rule(row_dict, rule, standards_cache)

    assert fired is False
    assert reasons == []


# ── Test 4: FIXED mode — expected type set manually ──────────────────────────

def test_validation_standard_match_fixed_mode():
    """FIXED mode: expected_item_type is set manually; directory not needed."""
    from app.readiness import _check_val_rule

    standards_cache = {}  # not needed for FIXED
    rule = _make_sm_rule(
        standard_source="ANY",
        expected_item_type_mode="FIXED",
        expected_item_type="гайка",
    )

    # Conflict case: item_type=болт, but expected=гайка
    row_dict = {"item_type": "болт", "din": "DIN 934", "iso": "", "gost": ""}
    fired, reasons = _check_val_rule(row_dict, rule, standards_cache)

    assert fired is True
    assert "гайка" in reasons[0]
    assert "болт" in reasons[0]

    # Non-conflict case: item_type=гайка matches expected
    row_ok = {"item_type": "гайка", "din": "DIN 934", "iso": "", "gost": ""}
    fired_ok, _ = _check_val_rule(row_ok, rule, standards_cache)

    assert fired_ok is False

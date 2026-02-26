"""Tests for Sandbox Mode.

- test_sandbox_does_not_modify_prod_rules
- test_sandbox_processing_uses_snapshot
- test_apply_sandbox_promotes_rules_to_prod
- test_compare_sandbox_vs_prod
"""

import json
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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_prod_rules():
    """Seed one readiness rule and one standard into the prod DB."""
    from app.database import get_db_session
    from app.models import ReadinessRule, StandardRef

    session = get_db_session()
    try:
        session.add(ReadinessRule(
            name="Болт: обязательные",
            item_type="болт",
            require_fields='["size"]',
            priority=0,
            is_active=True,
        ))
        session.add(StandardRef(
            standard_kind="DIN",
            standard_code="933",
            item_type="болт",
            title="Болт DIN 933",
            is_active=True,
        ))
        session.commit()
    finally:
        session.close()


# ── Test 1: Sandbox does not modify prod rules ────────────────────────────────

def test_sandbox_does_not_modify_prod_rules():
    """Editing a rule inside a sandbox must not change the prod DB."""
    from app.database import get_db_session
    from app.models import ReadinessRule
    from app.sandbox import (
        create_sandbox_session,
        snapshot_update_rule,
        update_sandbox_snapshot,
        get_sandbox,
    )

    _seed_prod_rules()

    # Record original prod rule name
    session = get_db_session()
    try:
        prod_rule = session.query(ReadinessRule).first()
        original_name = prod_rule.name
        original_id = prod_rule.id
    finally:
        session.close()

    # Create sandbox and modify a rule inside it
    sid = create_sandbox_session()
    sb = get_sandbox(sid)
    new_snap = snapshot_update_rule(
        sb.rule_snapshot_json, "readiness_rules", original_id,
        {"name": "ИЗМЕНЁННОЕ ИМЯ В SANDBOX"}
    )
    update_sandbox_snapshot(sid, new_snap)

    # Prod DB rule must be unchanged
    session = get_db_session()
    try:
        prod_rule_after = session.get(ReadinessRule, original_id)
        assert prod_rule_after.name == original_name, (
            f"Prod rule was modified by sandbox! Expected '{original_name}', got '{prod_rule_after.name}'"
        )
    finally:
        session.close()

    # Sandbox snapshot must reflect the change
    sb_updated = get_sandbox(sid)
    snap_data = json.loads(sb_updated.rule_snapshot_json)
    sb_rule = next(r for r in snap_data["readiness_rules"] if r["id"] == original_id)
    assert sb_rule["name"] == "ИЗМЕНЁННОЕ ИМЯ В SANDBOX"


# ── Test 2: Sandbox processing uses snapshot ──────────────────────────────────

def test_sandbox_processing_uses_snapshot():
    """File processing in sandbox must use snapshot rules, not prod rules."""
    import pandas as pd
    from app.sandbox import (
        create_sandbox_session,
        get_sandbox,
        load_snapshot_rules,
        snapshot_update_rule,
        update_sandbox_snapshot,
    )
    from app.readiness import apply_readiness

    _seed_prod_rules()

    # Disable all readiness rules in sandbox (make snapshot have no active rules)
    sid = create_sandbox_session()
    sb = get_sandbox(sid)

    snap_data = json.loads(sb.rule_snapshot_json)
    # Disable every readiness rule in the snapshot
    for r in snap_data["readiness_rules"]:
        r["is_active"] = False
    new_snap = json.dumps(snap_data, ensure_ascii=False)
    update_sandbox_snapshot(sid, new_snap)

    sb = get_sandbox(sid)
    rule_ctx = load_snapshot_rules(sb.rule_snapshot_json)

    # Create a minimal DataFrame that would fail prod readiness (болт missing size, no inferrable pattern)
    df_orig = pd.DataFrame([{"name": "Болт нестандартный специальный", "qty": "10", "code": ""}])
    df_orig.index = [0]
    df_trans = df_orig.copy()
    df_trans["status"] = ""
    df_trans["reason"] = ""

    result = apply_readiness(
        df_orig, df_trans,
        rules=rule_ctx["readiness_rules"],
        standards_cache=rule_ctx["standards_cache"],
        inference_rules=rule_ctx["inference_rules"],
        validation_rules=rule_ctx["validation_rules"],
    )

    # With no active readiness rules (sandbox), status should be "ok" (readiness disabled path)
    # (prod would require size=filled for болт → but sandbox has no active rules)
    assert result.at[0, "status"] == "ok", (
        f"Expected 'ok' with disabled sandbox rules, got '{result.at[0, 'status']}'"
    )


# ── Test 3: Apply sandbox promotes rules to prod ─────────────────────────────

def test_apply_sandbox_promotes_rules_to_prod():
    """Applying a sandbox replaces prod rules with the snapshot contents."""
    from app.database import get_db_session
    from app.models import ReadinessRule, RuleVersion
    from app.sandbox import (
        create_sandbox_session,
        get_sandbox,
        snapshot_update_rule,
        update_sandbox_snapshot,
        apply_snapshot_to_prod,
    )

    _seed_prod_rules()

    sid = create_sandbox_session()
    sb = get_sandbox(sid)

    # Rename the rule in sandbox
    snap_data = json.loads(sb.rule_snapshot_json)
    rid = snap_data["readiness_rules"][0]["id"]
    new_snap = snapshot_update_rule(sb.rule_snapshot_json, "readiness_rules", rid, {"name": "ПРОМОУТ ИЗ SANDBOX"})
    update_sandbox_snapshot(sid, new_snap)

    sb = get_sandbox(sid)
    rv_id = apply_snapshot_to_prod(sb.rule_snapshot_json, "тест промоута")

    # Check prod rule was replaced
    session = get_db_session()
    try:
        prod_rules = session.query(ReadinessRule).all()
        assert len(prod_rules) == 1
        assert prod_rules[0].name == "ПРОМОУТ ИЗ SANDBOX"

        # Check RuleVersion was created
        rv = session.get(RuleVersion, rv_id)
        assert rv is not None
        assert rv.description == "тест промоута"
    finally:
        session.close()


# ── Test 4: Compare sandbox vs prod ──────────────────────────────────────────

def test_compare_sandbox_vs_prod():
    """Sandbox and prod should produce different statuses when rules differ."""
    import pandas as pd
    from app.sandbox import (
        create_sandbox_session,
        get_sandbox,
        load_snapshot_rules,
        snapshot_update_rule,
        update_sandbox_snapshot,
    )
    from app.readiness import apply_readiness, load_active_rules, load_active_standards, load_active_validation_rules
    from app.inference_engine import load_active_inference_rules

    _seed_prod_rules()

    # Sandbox: disable all readiness rules → everything becomes "ok"
    sid = create_sandbox_session()
    sb = get_sandbox(sid)
    snap_data = json.loads(sb.rule_snapshot_json)
    for r in snap_data["readiness_rules"]:
        r["is_active"] = False
    new_snap = json.dumps(snap_data, ensure_ascii=False)
    update_sandbox_snapshot(sid, new_snap)
    sb = get_sandbox(sid)

    rule_ctx = load_snapshot_rules(sb.rule_snapshot_json)

    # Row with no extractable size so prod readiness fires (size required for болт)
    df_orig = pd.DataFrame([{"name": "Болт нестандартный", "qty": "10", "code": ""}])
    df_orig.index = [0]

    # Sandbox run (no active readiness rules → ok)
    df_sb = df_orig.copy()
    df_sb["status"] = ""
    df_sb["reason"] = ""
    sb_result = apply_readiness(
        df_orig, df_sb,
        rules=rule_ctx["readiness_rules"],
        standards_cache=rule_ctx["standards_cache"],
        inference_rules=rule_ctx["inference_rules"],
        validation_rules=rule_ctx["validation_rules"],
    )

    # Prod run (has readiness rules, болт needs size → manual or review)
    df_prod = df_orig.copy()
    df_prod["status"] = ""
    df_prod["reason"] = ""
    prod_result = apply_readiness(
        df_orig, df_prod,
        rules=load_active_rules(),
        standards_cache=load_active_standards(),
        inference_rules=load_active_inference_rules(),
        validation_rules=load_active_validation_rules(),
    )

    sb_status = sb_result.at[0, "status"]
    prod_status = prod_result.at[0, "status"]

    # They should differ (sandbox ok, prod non-ok due to missing size)
    assert sb_status != prod_status, (
        f"Expected sandbox and prod to differ: sandbox={sb_status}, prod={prod_status}"
    )
    assert sb_status == "ok"
    assert prod_status in ("review", "manual")

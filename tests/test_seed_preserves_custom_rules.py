"""Seeding must not undo what a person configured by hand.

The service re-runs its seed functions on every startup.  Eight of them bail
out as soon as their table has rows; ``seed_initial_validation_rules`` used to
go further and rewrite existing rules whose required fields differed from the
built-in defaults.  That silently reverted deliberate configuration on every
restart — for stainless fasteners it replaced "steel_grade" with "coating",
which is wrong for a material that has a grade and no coating at all.
"""

import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(tmp_path / "uploads"))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(tmp_path / "cache"))
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))

    import app.database as db_mod
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


def _session():
    import app.database as db_mod
    return db_mod.SessionLocal()


def test_reseeding_keeps_hand_edited_required_fields():
    """A rule edited by the user survives the next startup untouched."""
    from app.models import BaseValidationRule
    from app.seed import seed_initial_validation_rules

    seed_initial_validation_rules()

    session = _session()
    rule = session.query(BaseValidationRule).order_by(BaseValidationRule.id).first()
    assert rule is not None, "seeding produced no rules"

    rule_id = rule.id
    custom = json.dumps(["steel_grade", "diameter", "length"])
    rule.required_fields = custom
    session.commit()
    session.close()

    # Service restarts — seeding runs again
    seed_initial_validation_rules()

    session = _session()
    after = session.get(BaseValidationRule, rule_id)
    stored = after.required_fields
    session.close()

    assert stored == custom, (
        f"seeding overwrote a hand-edited rule: expected {custom}, got {stored}"
    )


def test_reseeding_still_adds_rules_that_are_missing():
    """Removing the overwrite must not stop new categories from appearing."""
    from app.models import BaseValidationRule
    from app.seed import seed_initial_validation_rules

    seed_initial_validation_rules()

    session = _session()
    total = session.query(BaseValidationRule).count()
    victim = session.query(BaseValidationRule).order_by(BaseValidationRule.id).first()
    session.delete(victim)
    session.commit()
    session.close()

    seed_initial_validation_rules()

    session = _session()
    restored = session.query(BaseValidationRule).count()
    session.close()

    assert restored == total, "a missing rule was not re-created"

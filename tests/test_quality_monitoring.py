"""Tests for the Quality Monitoring Pipeline."""

import json
import pytest
from datetime import datetime, timezone


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


# ── Model tests ─────────────────────────────────────────────────────────────


def test_pipeline_run_creation():
    from app.database import get_db_session
    from app.quality_models import PipelineRun

    session = get_db_session()
    try:
        run = PipelineRun(
            created_at=datetime.now(timezone.utc),
            total_client_lines=10,
            parsed_client_lines=8,
            auto_matches=6,
            manual_matches=2,
        )
        session.add(run)
        session.commit()
        assert run.id is not None
        assert run.total_client_lines == 10
    finally:
        session.close()


def test_pipeline_step_extra_json():
    from app.database import get_db_session
    from app.quality_models import PipelineRun, PipelineStep

    session = get_db_session()
    try:
        run = PipelineRun(created_at=datetime.now(timezone.utc))
        session.add(run)
        session.flush()

        step = PipelineStep(
            pipeline_run_id=run.id,
            step_name="parse_client_request",
            input_rows=5,
            output_rows=4,
            success_rate=0.8,
        )
        step.extra = {"field_recognition_rate": 0.65, "size_recognition_rate": 0.5}
        session.add(step)
        session.commit()

        assert step.extra["field_recognition_rate"] == 0.65
        assert step.extra["size_recognition_rate"] == 0.5
    finally:
        session.close()


def test_match_feedback_is_correct():
    from app.database import get_db_session
    from app.quality_models import MatchFeedback

    session = get_db_session()
    try:
        # Correct match
        fb1 = MatchFeedback(
            system_choice_id=42,
            user_choice_id=42,
            is_correct=True,
            created_at=datetime.now(timezone.utc),
        )
        session.add(fb1)

        # Incorrect match
        fb2 = MatchFeedback(
            system_choice_id=42,
            user_choice_id=99,
            is_correct=False,
            created_at=datetime.now(timezone.utc),
        )
        session.add(fb2)
        session.commit()

        assert fb1.is_correct is True
        assert fb2.is_correct is False
    finally:
        session.close()


def test_settings_version():
    from app.database import get_db_session
    from app.quality_models import SettingsVersion

    session = get_db_session()
    try:
        sv = SettingsVersion(
            version_code="v1.0",
            created_at=datetime.now(timezone.utc),
            description="Initial",
            settings_snapshot_json=json.dumps({"auto_apply": True}),
        )
        session.add(sv)
        session.commit()
        assert sv.snapshot == {"auto_apply": True}
    finally:
        session.close()


# ── Service tests ───────────────────────────────────────────────────────────


def test_create_pipeline_run():
    from app.database import get_db_session
    from app.services.quality_service import create_pipeline_run

    session = get_db_session()
    try:
        run = create_pipeline_run(order_id=None, session=session)
        session.commit()
        assert run.id is not None
        assert run.total_client_lines == 0
    finally:
        session.close()


def test_track_step():
    from app.database import get_db_session
    from app.services.quality_service import create_pipeline_run, track_step
    from app.quality_models import PipelineStep

    session = get_db_session()
    try:
        run = create_pipeline_run(session=session)
        with track_step(run, "catalog_match", session, input_rows=10) as step:
            step.output_rows = 7
            step.success_rate = 0.7
        session.commit()

        loaded = session.query(PipelineStep).filter_by(pipeline_run_id=run.id).first()
        assert loaded is not None
        assert loaded.step_name == "catalog_match"
        assert loaded.input_rows == 10
        assert loaded.output_rows == 7
        assert loaded.duration_ms is not None
        assert loaded.duration_ms >= 0
    finally:
        session.close()


def test_record_feedback():
    from app.database import get_db_session
    from app.services.quality_service import record_feedback
    from app.quality_models import MatchFeedback

    session = get_db_session()
    try:
        fb = record_feedback(
            order_id=1,
            client_line_id=5,
            system_choice_id=10,
            user_choice_id=20,
            session=session,
        )
        session.commit()
        assert fb.is_correct is False

        fb2 = record_feedback(
            order_id=1,
            client_line_id=6,
            system_choice_id=10,
            user_choice_id=10,
            session=session,
        )
        session.commit()
        assert fb2.is_correct is True
    finally:
        session.close()


def test_compute_field_recognition():
    from app.services.quality_service import compute_field_recognition

    parsed_lines = [
        {"item_type": "болт", "size": "M12x50", "diameter": "M12", "length": "50",
         "strength": "8.8", "coating": "цинк", "gost": "7798", "din": "", "iso": ""},
        {"item_type": "гайка", "size": "", "diameter": "M10", "length": "",
         "strength": "", "coating": "", "gost": "", "din": "934", "iso": ""},
    ]
    result = compute_field_recognition(parsed_lines)
    assert result["field_recognition_rate"] > 0
    assert result["size_recognition_rate"] == 0.5  # 1 out of 2
    assert result["standard_recognition_rate"] == 1.0  # both have at least one std


def test_compute_quality_metrics_empty():
    from app.services.quality_service import compute_quality_metrics

    m = compute_quality_metrics()
    assert m.total_runs == 0
    assert m.parse_success_rate == 0.0


def test_compute_quality_metrics_with_data():
    from app.database import get_db_session
    from app.quality_models import PipelineRun
    from app.services.quality_service import compute_quality_metrics

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        run1 = PipelineRun(
            created_at=now,
            total_client_lines=10,
            parsed_client_lines=8,
            auto_matches=6,
            manual_matches=2,
            system_match_correct=5,
            system_match_incorrect=1,
        )
        run2 = PipelineRun(
            created_at=now,
            total_client_lines=20,
            parsed_client_lines=18,
            auto_matches=15,
            manual_matches=3,
            system_match_correct=14,
            system_match_incorrect=1,
        )
        session.add_all([run1, run2])
        session.commit()
    finally:
        session.close()

    m = compute_quality_metrics()
    assert m.total_runs == 2
    assert m.total_client_lines == 30
    assert m.parse_success_rate == round(26 / 30, 4)
    assert m.auto_match_rate == round(21 / 30, 4)
    assert m.manual_match_rate == round(5 / 30, 4)
    assert m.match_accuracy == round(19 / 21, 4)
    assert len(m.history) == 2


def test_save_settings_version_idempotent():
    from app.services.quality_service import save_settings_version
    from app.database import get_db_session
    from app.quality_models import SettingsVersion

    sv1 = save_settings_version("v1.0", description="test")
    sv2 = save_settings_version("v1.0", description="test again")
    assert sv1.id == sv2.id

    session = get_db_session()
    try:
        count = session.query(SettingsVersion).filter_by(version_code="v1.0").count()
        assert count == 1
    finally:
        session.close()


# ── Route tests ─────────────────────────────────────────────────────────────


def test_quality_dashboard_empty():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    resp = client.get("/system-quality")
    assert resp.status_code == 200
    assert "Мониторинг качества" in resp.text


def test_quality_api_empty():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    resp = client.get("/api/system/quality")
    assert resp.status_code == 200
    data = resp.json()
    assert "parse_success_rate" in data
    assert "correction_rate" in data
    assert data["total_runs"] == 0


def test_quality_dashboard_with_data():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.quality_models import PipelineRun

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        session.add(PipelineRun(
            created_at=now,
            total_client_lines=10,
            parsed_client_lines=9,
            auto_matches=7,
            manual_matches=2,
        ))
        session.commit()
    finally:
        session.close()

    client = TestClient(app)
    resp = client.get("/system-quality")
    assert resp.status_code == 200
    assert "90.0%" in resp.text  # parse_success_rate = 9/10


def test_quality_api_with_data():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.quality_models import PipelineRun

    session = get_db_session()
    try:
        now = datetime.now(timezone.utc)
        session.add(PipelineRun(
            created_at=now,
            total_client_lines=20,
            parsed_client_lines=16,
            auto_matches=12,
            manual_matches=4,
        ))
        session.commit()
    finally:
        session.close()

    client = TestClient(app)
    resp = client.get("/api/system/quality")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_runs"] == 1
    assert data["parse_success_rate"] == 0.8
    assert data["auto_match_rate"] == 0.6


# ── Cascade delete test ─────────────────────────────────────────────────────


def test_cascade_delete_steps():
    from app.database import get_db_session
    from app.quality_models import PipelineRun, PipelineStep
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        run = PipelineRun(created_at=datetime.now(timezone.utc))
        session.add(run)
        session.flush()
        step = PipelineStep(
            pipeline_run_id=run.id,
            step_name="test",
            input_rows=1,
        )
        session.add(step)
        session.commit()
        run_id = run.id

        session.delete(run)
        session.commit()

        orphan = session.query(PipelineStep).filter_by(pipeline_run_id=run_id).first()
        assert orphan is None
    finally:
        session.close()

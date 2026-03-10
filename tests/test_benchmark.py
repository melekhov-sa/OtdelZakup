"""Tests for the Benchmark Engine."""

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


def _seed_dataset(session):
    """Create a benchmark dataset with one case and expected results."""
    from app.benchmark_models import BenchmarkDataset, BenchmarkCase, BenchmarkExpectedResult

    ds = BenchmarkDataset(
        name="Тестовый датасет",
        description="Для тестов",
        created_at=datetime.now(timezone.utc),
    )
    session.add(ds)
    session.flush()

    case = BenchmarkCase(
        dataset_id=ds.id,
        name="Болты и гайки",
        source_type="client_text",
        input_data="Болт М12х50 8.8 ГОСТ 7798-70 цинк\nГайка М12 DIN 934 цинк",
        created_at=datetime.now(timezone.utc),
    )
    session.add(case)
    session.flush()

    exp1 = BenchmarkExpectedResult(
        benchmark_case_id=case.id,
        row_index=0,
        expected_item_type="болт",
        expected_size="M12X50",
        expected_strength="8.8",
        expected_coating="цинк",
    )
    exp2 = BenchmarkExpectedResult(
        benchmark_case_id=case.id,
        row_index=1,
        expected_item_type="гайка",
        expected_coating="цинк",
    )
    session.add_all([exp1, exp2])
    session.commit()
    return ds


# ── Model tests ─────────────────────────────────────────────────────────────


def test_dataset_creation():
    from app.database import get_db_session
    from app.benchmark_models import BenchmarkDataset

    session = get_db_session()
    try:
        ds = BenchmarkDataset(
            name="Test",
            created_at=datetime.now(timezone.utc),
        )
        session.add(ds)
        session.commit()
        assert ds.id is not None
    finally:
        session.close()


def test_case_with_expected():
    from app.database import get_db_session

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        from app.benchmark_models import BenchmarkCase, BenchmarkExpectedResult
        cases = session.query(BenchmarkCase).filter_by(dataset_id=ds.id).all()
        assert len(cases) == 1
        exps = session.query(BenchmarkExpectedResult).filter_by(benchmark_case_id=cases[0].id).all()
        assert len(exps) == 2
    finally:
        session.close()


def test_run_row_errors_property():
    from app.benchmark_models import BenchmarkRunRow

    row = BenchmarkRunRow(
        benchmark_run_id=1,
        benchmark_case_id=1,
        row_index=0,
        correct_item_type=True,
        correct_size=False,
        correct_strength=True,
        correct_coating=False,
    )
    assert "size" in row.errors
    assert "coating" in row.errors
    assert "item_type" not in row.errors


# ── Engine tests ────────────────────────────────────────────────────────────


def test_parse_input_lines_text():
    from app.services.benchmark_engine import _parse_input_lines
    from app.benchmark_models import BenchmarkCase

    case = BenchmarkCase(
        dataset_id=1, name="test", source_type="client_text",
        input_data="Болт М12х50\nГайка М12\nШайба 12",
        created_at=datetime.now(timezone.utc),
    )
    lines = _parse_input_lines(case)
    assert len(lines) == 3
    assert lines[0] == "Болт М12х50"


def test_parse_input_lines_json():
    from app.services.benchmark_engine import _parse_input_lines
    from app.benchmark_models import BenchmarkCase

    case = BenchmarkCase(
        dataset_id=1, name="test", source_type="client_text",
        input_data=json.dumps(["Болт М12х50", "Гайка М12"]),
        created_at=datetime.now(timezone.utc),
    )
    lines = _parse_input_lines(case)
    assert len(lines) == 2


def test_compare_field():
    from app.services.benchmark_engine import _compare_field

    assert _compare_field("болт", "болт") is True
    assert _compare_field("Болт", "болт") is True
    assert _compare_field("гайка", "болт") is False
    assert _compare_field("болт", None) is None  # no expectation
    assert _compare_field("болт", "") is None


def test_run_benchmark_parse_accuracy():
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        run = run_benchmark(ds.id, session=session)

        assert run.id is not None
        assert run.total_rows == 2
        assert run.parse_accuracy is not None
        assert run.parse_accuracy > 0  # should recognize at least some fields
        assert run.finished_at is not None
    finally:
        session.close()


def test_run_benchmark_run_rows_created():
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark
    from app.benchmark_models import BenchmarkRunRow

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        run = run_benchmark(ds.id, session=session)

        rows = session.query(BenchmarkRunRow).filter_by(benchmark_run_id=run.id).all()
        assert len(rows) == 2
        # First row should have system_item_type
        r0 = [r for r in rows if r.row_index == 0][0]
        assert r0.system_item_type is not None
        assert r0.raw_text is not None
    finally:
        session.close()


def test_get_run_summary():
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark, get_run_summary

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        run = run_benchmark(ds.id, session=session)
        summary = get_run_summary(run.id, session=session)

        assert summary is not None
        assert summary.run.id == run.id
        assert "item_type" in summary.field_stats
        assert summary.field_stats["item_type"]["checked"] > 0
    finally:
        session.close()


def test_run_benchmark_empty_dataset():
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark
    from app.benchmark_models import BenchmarkDataset

    session = get_db_session()
    try:
        ds = BenchmarkDataset(name="Empty", created_at=datetime.now(timezone.utc))
        session.add(ds)
        session.commit()

        run = run_benchmark(ds.id, session=session)
        assert run.total_rows == 0
        assert run.parse_accuracy is None
    finally:
        session.close()


# ── Route tests ─────────────────────────────────────────────────────────────


def test_benchmark_list_page():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    resp = client.get("/benchmark")
    assert resp.status_code == 200
    assert "Benchmark" in resp.text


def test_benchmark_new_form():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    resp = client.get("/benchmark/new")
    assert resp.status_code == 200
    assert "Новый датасет" in resp.text


def test_benchmark_crud_flow():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    # Create dataset
    resp = client.post("/benchmark/new", data={
        "name": "Test Dataset",
        "description": "For testing",
    })
    assert resp.status_code == 200  # follows redirect

    # Find dataset ID from page
    resp2 = client.get("/benchmark")
    assert "Test Dataset" in resp2.text


def test_benchmark_case_creation():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.benchmark_models import BenchmarkDataset

    session = get_db_session()
    try:
        ds = BenchmarkDataset(name="CaseTest", created_at=datetime.now(timezone.utc))
        session.add(ds)
        session.commit()
        ds_id = ds.id
    finally:
        session.close()

    client = TestClient(app)

    # Add case
    expected = json.dumps([
        {"row_index": 0, "item_type": "болт", "size": "M12X50"},
    ])
    resp = client.post(f"/benchmark/{ds_id}/cases/new", data={
        "name": "Test case",
        "source_type": "client_text",
        "input_data": "Болт М12х50 ГОСТ 7798",
        "expected_json": expected,
    })
    assert resp.status_code == 200

    # Verify case in detail page
    resp2 = client.get(f"/benchmark/{ds_id}")
    assert "Test case" in resp2.text


def test_benchmark_api_run():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        ds_id = ds.id
    finally:
        session.close()

    client = TestClient(app)
    resp = client.post(f"/api/benchmark/run/{ds_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert "run_id" in data
    assert data["total_rows"] == 2
    assert data["parse_accuracy"] is not None


def test_benchmark_api_results():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        run_benchmark(ds.id, session=session)
    finally:
        session.close()

    client = TestClient(app)
    resp = client.get("/api/benchmark/results")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    assert "parse_accuracy" in data[0]


def test_benchmark_run_detail_page():
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.services.benchmark_engine import run_benchmark

    session = get_db_session()
    try:
        ds = _seed_dataset(session)
        run = run_benchmark(ds.id, session=session)
        ds_id = ds.id
        run_id = run.id
    finally:
        session.close()

    client = TestClient(app)
    resp = client.get(f"/benchmark/{ds_id}/runs/{run_id}")
    assert resp.status_code == 200
    assert "Точность" in resp.text
    assert "Точность по полям" in resp.text


def test_benchmark_api_run_404():
    from fastapi.testclient import TestClient
    from app.main import app
    client = TestClient(app)

    resp = client.post("/api/benchmark/run/99999")
    assert resp.status_code == 404


# ── Cascade delete ──────────────────────────────────────────────────────────


def test_cascade_delete_dataset():
    from app.database import get_db_session
    from app.benchmark_models import BenchmarkDataset, BenchmarkCase, BenchmarkExpectedResult
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        ds = _seed_dataset(session)
        ds_id = ds.id

        session.delete(ds)
        session.commit()

        assert session.query(BenchmarkCase).filter_by(dataset_id=ds_id).count() == 0
        assert session.query(BenchmarkExpectedResult).count() == 0
    finally:
        session.close()

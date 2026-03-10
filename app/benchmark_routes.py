"""Routes for /benchmark — Benchmark Engine UI and API."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_db_session

benchmark_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── API ─────────────────────────────────────────────────────────────────────


@benchmark_router.post("/api/benchmark/run/{dataset_id}")
def api_run_benchmark(dataset_id: int):
    """Run benchmark for a dataset. Returns JSON with accuracy metrics."""
    from app.services.benchmark_engine import run_benchmark

    session = get_db_session()
    try:
        run = run_benchmark(dataset_id, session=session)
        return JSONResponse({
            "run_id": run.id,
            "dataset_id": run.dataset_id,
            "total_rows": run.total_rows,
            "parse_accuracy": run.parse_accuracy,
            "catalog_match_accuracy": run.catalog_match_accuracy,
            "supplier_parse_accuracy": run.supplier_parse_accuracy,
            "supplier_match_accuracy": run.supplier_match_accuracy,
            "settings_version": run.settings_version,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        })
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=404)
    finally:
        session.close()


@benchmark_router.get("/api/benchmark/results")
def api_benchmark_results(dataset_id: int | None = None, limit: int = 20):
    """List recent benchmark runs."""
    from app.benchmark_models import BenchmarkRun

    session = get_db_session()
    try:
        q = session.query(BenchmarkRun).order_by(BenchmarkRun.id.desc())
        if dataset_id:
            q = q.filter_by(dataset_id=dataset_id)
        runs = q.limit(limit).all()
        return JSONResponse([
            {
                "id": r.id,
                "dataset_id": r.dataset_id,
                "total_rows": r.total_rows,
                "parse_accuracy": r.parse_accuracy,
                "catalog_match_accuracy": r.catalog_match_accuracy,
                "supplier_parse_accuracy": r.supplier_parse_accuracy,
                "supplier_match_accuracy": r.supplier_match_accuracy,
                "settings_version": r.settings_version,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            }
            for r in runs
        ])
    finally:
        session.close()


# ── UI: Dataset list ────────────────────────────────────────────────────────


@benchmark_router.get("/benchmark", response_class=HTMLResponse)
def benchmark_list(request: Request):
    from app.benchmark_models import BenchmarkDataset, BenchmarkCase, BenchmarkRun

    session = get_db_session()
    try:
        datasets = session.query(BenchmarkDataset).order_by(BenchmarkDataset.id.desc()).all()
        ds_data = []
        for ds in datasets:
            case_count = session.query(BenchmarkCase).filter_by(dataset_id=ds.id).count()
            last_run = (
                session.query(BenchmarkRun)
                .filter_by(dataset_id=ds.id)
                .order_by(BenchmarkRun.id.desc())
                .first()
            )
            ds_data.append({"ds": ds, "case_count": case_count, "last_run": last_run})
    finally:
        session.close()

    return templates.TemplateResponse("benchmark_list.html", {
        "request": request, "datasets": ds_data,
    })


# ── UI: Create dataset ──────────────────────────────────────────────────────


@benchmark_router.get("/benchmark/new", response_class=HTMLResponse)
def benchmark_new_form(request: Request):
    return templates.TemplateResponse("benchmark_dataset_form.html", {
        "request": request, "is_edit": False, "ds": None,
    })


@benchmark_router.post("/benchmark/new")
def benchmark_new_save(
    name: str = Form(...),
    description: str = Form(""),
):
    from app.benchmark_models import BenchmarkDataset

    session = get_db_session()
    try:
        ds = BenchmarkDataset(
            name=name,
            description=description or None,
            created_at=datetime.now(timezone.utc),
        )
        session.add(ds)
        session.commit()
        ds_id = ds.id
    finally:
        session.close()
    return RedirectResponse(f"/benchmark/{ds_id}", status_code=303)


# ── UI: Dataset detail ──────────────────────────────────────────────────────


@benchmark_router.get("/benchmark/{ds_id}", response_class=HTMLResponse)
def benchmark_detail(request: Request, ds_id: int):
    from app.benchmark_models import (
        BenchmarkDataset, BenchmarkCase, BenchmarkExpectedResult, BenchmarkRun,
    )

    session = get_db_session()
    try:
        ds = session.get(BenchmarkDataset, ds_id)
        if not ds:
            return HTMLResponse("Dataset not found", status_code=404)

        cases = session.query(BenchmarkCase).filter_by(dataset_id=ds_id).all()
        case_data = []
        for c in cases:
            exp_count = session.query(BenchmarkExpectedResult).filter_by(benchmark_case_id=c.id).count()
            case_data.append({"case": c, "expected_count": exp_count})

        runs = (
            session.query(BenchmarkRun)
            .filter_by(dataset_id=ds_id)
            .order_by(BenchmarkRun.id.desc())
            .limit(20)
            .all()
        )
    finally:
        session.close()

    return templates.TemplateResponse("benchmark_detail.html", {
        "request": request, "ds": ds, "cases": case_data, "runs": runs,
    })


# ── UI: Add case ────────────────────────────────────────────────────────────


SOURCE_TYPE_OPTIONS = [
    ("client_text", "Текст заявки клиента"),
    ("client_excel", "Excel заявки"),
    ("client_pdf", "PDF заявки"),
    ("client_image", "Фото заявки"),
    ("supplier_text", "Текст КП поставщика"),
    ("supplier_excel", "Excel КП"),
    ("supplier_pdf", "PDF КП"),
]


@benchmark_router.get("/benchmark/{ds_id}/cases/new", response_class=HTMLResponse)
def benchmark_case_form(request: Request, ds_id: int):
    return templates.TemplateResponse("benchmark_case_form.html", {
        "request": request, "ds_id": ds_id,
        "source_type_options": SOURCE_TYPE_OPTIONS,
    })


@benchmark_router.post("/benchmark/{ds_id}/cases/new")
def benchmark_case_save(
    ds_id: int,
    name: str = Form(...),
    source_type: str = Form("client_text"),
    input_data: str = Form(...),
    expected_json: str = Form("[]"),
):
    from app.benchmark_models import BenchmarkCase, BenchmarkExpectedResult

    session = get_db_session()
    try:
        case = BenchmarkCase(
            dataset_id=ds_id,
            name=name,
            source_type=source_type,
            input_data=input_data,
            created_at=datetime.now(timezone.utc),
        )
        session.add(case)
        session.flush()

        # Parse expected results JSON
        try:
            expected_list = json.loads(expected_json)
        except (json.JSONDecodeError, ValueError):
            expected_list = []

        for i, exp in enumerate(expected_list):
            if isinstance(exp, dict):
                er = BenchmarkExpectedResult(
                    benchmark_case_id=case.id,
                    row_index=exp.get("row_index", i),
                    expected_item_type=exp.get("item_type"),
                    expected_standard=exp.get("standard"),
                    expected_size=exp.get("size"),
                    expected_strength=exp.get("strength"),
                    expected_coating=exp.get("coating"),
                    expected_catalog_item_id=exp.get("catalog_item_id"),
                    expected_supplier_item_name=exp.get("supplier_item_name"),
                    expected_price=exp.get("price"),
                    expected_unit=exp.get("unit"),
                )
                session.add(er)

        session.commit()
    finally:
        session.close()

    return RedirectResponse(f"/benchmark/{ds_id}", status_code=303)


# ── UI: Delete case ─────────────────────────────────────────────────────────


@benchmark_router.post("/benchmark/{ds_id}/cases/{case_id}/delete")
def benchmark_case_delete(ds_id: int, case_id: int):
    from app.benchmark_models import BenchmarkCase
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        c = session.get(BenchmarkCase, case_id)
        if c and c.dataset_id == ds_id:
            session.delete(c)
            session.commit()
    finally:
        session.close()
    return RedirectResponse(f"/benchmark/{ds_id}", status_code=303)


# ── UI: Run benchmark ───────────────────────────────────────────────────────


@benchmark_router.post("/benchmark/{ds_id}/run")
def benchmark_run(ds_id: int):
    from app.services.benchmark_engine import run_benchmark

    session = get_db_session()
    try:
        run = run_benchmark(ds_id, session=session)
        run_id = run.id
    finally:
        session.close()
    return RedirectResponse(f"/benchmark/{ds_id}/runs/{run_id}", status_code=303)


# ── UI: Run detail ──────────────────────────────────────────────────────────


@benchmark_router.get("/benchmark/{ds_id}/runs/{run_id}", response_class=HTMLResponse)
def benchmark_run_detail(request: Request, ds_id: int, run_id: int):
    from app.services.benchmark_engine import get_run_summary
    from app.benchmark_models import BenchmarkDataset, BenchmarkExpectedResult, BenchmarkRun

    session = get_db_session()
    try:
        ds = session.get(BenchmarkDataset, ds_id)
        if not ds:
            return HTMLResponse("Dataset not found", status_code=404)

        summary = get_run_summary(run_id, session=session)
        if not summary:
            return HTMLResponse("Run not found", status_code=404)

        # Load expected results for error rows
        error_details = []
        for er_row in summary.error_rows:
            exp = (
                session.query(BenchmarkExpectedResult)
                .filter_by(benchmark_case_id=er_row.benchmark_case_id, row_index=er_row.row_index)
                .first()
            )
            error_details.append({"row": er_row, "expected": exp})

        # Previous run for comparison
        prev_run = (
            session.query(BenchmarkRun)
            .filter(
                BenchmarkRun.dataset_id == ds_id,
                BenchmarkRun.id < run_id,
            )
            .order_by(BenchmarkRun.id.desc())
            .first()
        )
    finally:
        session.close()

    return templates.TemplateResponse("benchmark_run_detail.html", {
        "request": request, "ds": ds, "summary": summary,
        "error_details": error_details, "prev_run": prev_run,
    })


# ── UI: Delete dataset ──────────────────────────────────────────────────────


@benchmark_router.post("/benchmark/{ds_id}/delete")
def benchmark_delete(ds_id: int):
    from app.benchmark_models import BenchmarkDataset
    from sqlalchemy import text

    session = get_db_session()
    try:
        session.execute(text("PRAGMA foreign_keys = ON"))
        ds = session.get(BenchmarkDataset, ds_id)
        if ds:
            session.delete(ds)
            session.commit()
    finally:
        session.close()
    return RedirectResponse("/benchmark", status_code=303)

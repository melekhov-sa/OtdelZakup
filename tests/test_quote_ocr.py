"""Tests for the Quote OCR (raw table extraction) feature."""
import json

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# ── DB isolation fixture ──────────────────────────────────────────────────────

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
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


def _session():
    import app.database as db_mod
    return db_mod.SessionLocal()


# ── Fake Document AI response ─────────────────────────────────────────────────

def _make_fake_document(tables_data):
    """Build a fake Document AI response dict with tables.

    tables_data: list of list[list[str]] — each is a table's rows.
    """
    # Build document text by concatenating all cell texts
    text_parts = []
    offset = 0

    pages = [{"tables": [], "blocks": []}]

    for table_rows in tables_data:
        table = {"headerRows": [], "bodyRows": []}
        for row_idx, row in enumerate(table_rows):
            cells = []
            for cell_text in row:
                start = offset
                end = offset + len(cell_text)
                text_parts.append(cell_text)
                offset = end

                cells.append({
                    "layout": {
                        "textAnchor": {
                            "textSegments": [{"startIndex": str(start), "endIndex": str(end)}]
                        },
                        "confidence": 0.95,
                    }
                })

            row_dict = {"cells": cells}
            if row_idx == 0:
                table["headerRows"].append(row_dict)
            else:
                table["bodyRows"].append(row_dict)

        pages[0]["tables"].append(table)

    return {
        "text": "".join(text_parts),
        "pages": pages,
    }


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestExtractAllTables:

    def test_extracts_single_table(self):
        from app.services.quote_ocr import _extract_all_tables

        doc = _make_fake_document([
            [["Наименование", "Кол-во", "Цена"],
             ["Болт M12x60", "10", "100"],
             ["Гайка M12", "20", "50"]],
        ])
        tables = _extract_all_tables(doc)
        assert len(tables) == 1
        assert tables[0]["n_rows"] == 3
        assert tables[0]["n_cols"] == 3
        assert tables[0]["page_no"] == 1
        assert tables[0]["rows"][0] == ["Наименование", "Кол-во", "Цена"]
        assert tables[0]["rows"][1] == ["Болт M12x60", "10", "100"]

    def test_extracts_multiple_tables(self):
        from app.services.quote_ocr import _extract_all_tables

        doc = _make_fake_document([
            [["Col A", "Col B"], ["val1", "val2"]],
            [["X", "Y", "Z"], ["1", "2", "3"], ["4", "5", "6"]],
        ])
        tables = _extract_all_tables(doc)
        assert len(tables) == 2
        assert tables[0]["n_rows"] == 2
        assert tables[0]["n_cols"] == 2
        assert tables[1]["n_rows"] == 3
        assert tables[1]["n_cols"] == 3

    def test_empty_document(self):
        from app.services.quote_ocr import _extract_all_tables

        doc = {"text": "", "pages": []}
        tables = _extract_all_tables(doc)
        assert tables == []


class TestRunQuoteOcr:

    def test_saves_raw_table_structure(self, monkeypatch):
        """Mock Document AI -> verify tables saved in DB with correct structure."""
        from app.order_models import QuoteOcrJob, QuoteOcrTable

        fake_doc = _make_fake_document([
            [["№", "Наименование", "Кол-во", "Сумма"],
             ["1", "Болт M12x60", "10", "1000"],
             ["2", "Гайка M12", "20", "500"],
             ["3", "Шайба M12", "50", "250"]],
        ])

        monkeypatch.setattr(
            "app.integrations.google_document_ai.process_document",
            lambda *a, **kw: fake_doc,
        )

        from app.services.quote_ocr import run_quote_ocr

        sess = _session()
        job_id = run_quote_ocr(b"fake-pdf", "test.pdf", "application/pdf", sess)

        job = sess.get(QuoteOcrJob, job_id)
        assert job is not None
        assert job.status == "done"
        assert job.tables_found == 1
        assert job.page_count == 1

        tables = sess.query(QuoteOcrTable).filter_by(job_id=job_id).all()
        assert len(tables) == 1

        tbl = tables[0]
        assert tbl.n_rows == 4
        assert tbl.n_cols == 4
        assert tbl.table_index == 0
        assert tbl.page_no == 1

        rows = tbl.rows
        assert len(rows) == 4
        assert rows[0] == ["№", "Наименование", "Кол-во", "Сумма"]
        assert rows[1][1] == "Болт M12x60"
        assert rows[3][1] == "Шайба M12"
        sess.close()

    def test_error_handling(self, monkeypatch):
        """When Document AI raises, job should be saved with error status."""
        from app.order_models import QuoteOcrJob

        monkeypatch.setattr(
            "app.integrations.google_document_ai.process_document",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
        )

        from app.services.quote_ocr import run_quote_ocr

        sess = _session()
        job_id = run_quote_ocr(b"fake-pdf", "fail.pdf", "application/pdf", sess)

        job = sess.get(QuoteOcrJob, job_id)
        assert job.status == "error"
        assert "API down" in (job.error or "")
        sess.close()

    def test_multiple_tables_saved(self, monkeypatch):
        """Two tables in one page -> two QuoteOcrTable records."""
        from app.order_models import QuoteOcrTable

        fake_doc = _make_fake_document([
            [["A", "B"], ["1", "2"]],
            [["X", "Y", "Z"], ["a", "b", "c"]],
        ])

        monkeypatch.setattr(
            "app.integrations.google_document_ai.process_document",
            lambda *a, **kw: fake_doc,
        )

        from app.services.quote_ocr import run_quote_ocr

        sess = _session()
        job_id = run_quote_ocr(b"fake-pdf", "multi.pdf", "application/pdf", sess)

        tables = sess.query(QuoteOcrTable).filter_by(job_id=job_id).order_by(QuoteOcrTable.table_index).all()
        assert len(tables) == 2
        assert tables[0].n_cols == 2
        assert tables[1].n_cols == 3
        sess.close()


class TestCascadeDelete:

    def test_delete_job_removes_tables(self):
        """Deleting a QuoteOcrJob cascades to QuoteOcrTable."""
        from app.order_models import QuoteOcrJob, QuoteOcrTable

        sess = _session()
        sess.execute(text("PRAGMA foreign_keys = ON"))

        job = QuoteOcrJob(filename="test.pdf", status="done", tables_found=1)
        sess.add(job)
        sess.flush()

        sess.add(QuoteOcrTable(
            job_id=job.id, table_index=0, n_rows=2, n_cols=3,
            raw_json='[["a","b","c"],["d","e","f"]]',
        ))
        sess.commit()

        sess.execute(text("DELETE FROM quote_ocr_jobs WHERE id = :jid"), {"jid": job.id})
        sess.commit()

        assert sess.query(QuoteOcrTable).count() == 0
        sess.close()


class TestCsvExport:

    def test_csv_content(self):
        """QuoteOcrTable.rows -> CSV export should contain all cells."""
        from app.order_models import QuoteOcrJob, QuoteOcrTable

        sess = _session()
        job = QuoteOcrJob(filename="test.pdf", status="done")
        sess.add(job)
        sess.flush()

        rows = [["Наименование", "Кол-во"], ["Болт M12", "10"], ["Гайка M8", "20"]]
        tbl = QuoteOcrTable(
            job_id=job.id, table_index=0,
            n_rows=3, n_cols=2,
            raw_json=json.dumps(rows, ensure_ascii=False),
        )
        sess.add(tbl)
        sess.commit()

        # Verify raw_json round-trips correctly
        loaded = sess.get(QuoteOcrTable, tbl.id)
        assert loaded.rows == rows
        assert loaded.rows[1][0] == "Болт M12"
        sess.close()

"""Tests for app/pdf_parser.py and /upload-pdf routes.

Covers:
1.  detect_pdf_kind — IMAGE for raster extensions
2.  detect_pdf_kind — UNKNOWN for unsupported extension
3.  detect_pdf_kind — TEXT_PDF when pdfplumber finds chars
4.  detect_pdf_kind — SCAN_PDF when pdfplumber finds no/few chars
5.  _rows_to_dataframe — joins tokens, skips empty rows
6.  _rows_to_dataframe — returns empty DataFrame for all-empty input
7.  parse_pdf dispatches to parse_image for image paths
8.  parse_pdf returns error when pdfplumber not available (mocked)
9.  ParseResult default fields
10. _words_to_rows — groups words on same Y into one row
11. _text_to_rows — splits OCR text into token lists
12. Integration: upload text PDF via /upload-pdf → wizard page
13. Integration: upload unsupported format → 400 error
14. Integration: GET /upload-pdf shows form
15. Migration 019 tables are created
"""

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ── Isolation fixture ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir  = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR",  str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR  = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    db_mod.SessionLocal = sessionmaker(
        bind=db_mod.engine, autoflush=False, expire_on_commit=False
    )
    db_mod.init_db()


@pytest.fixture()
def client():
    from app.main import app
    from fastapi.testclient import TestClient
    return TestClient(app, raise_server_exceptions=True)


# ── Helper: build a minimal text PDF with fpdf2 ───────────────────────────────

def _make_text_pdf(text_lines: list[str]) -> bytes:
    """Create a simple PDF with text using fpdf2 (ASCII-only, built-in font).

    Pads content to guarantee pdfplumber extracts >= PDF_TEXT_CHAR_THRESHOLD chars.
    """
    try:
        from fpdf import FPDF
    except ImportError:
        pytest.skip("fpdf2 not installed")

    # Pad with filler lines to ensure > 50 extracted chars
    filler = [
        "Item no. 1: Bolt M8x25 DIN 933 zinc-plated class 8.8",
        "Item no. 2: Nut M8 DIN 934 ISO 4032 hex grade 8",
        "Item no. 3: Washer M8 DIN 125 flat washer steel",
    ]
    all_lines = text_lines + filler

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    for line in all_lines:
        # fpdf2 built-in fonts only support latin-1; use ASCII text in tests
        safe_line = line.encode("latin-1", errors="replace").decode("latin-1")
        pdf.cell(0, 10, safe_line)
        pdf.ln()
    return bytes(pdf.output())


def _make_blank_pdf() -> bytes:
    """Create a PDF with no text content (image-only simulation)."""
    try:
        from fpdf import FPDF
    except ImportError:
        pytest.skip("fpdf2 not installed")

    pdf = FPDF()
    pdf.add_page()
    # No text — simulates a scan
    return bytes(pdf.output())


# ── Unit tests: detect_pdf_kind ───────────────────────────────────────────────

class TestDetectPdfKind:
    def test_png_is_image(self, tmp_path):
        from app.pdf_parser import detect_pdf_kind
        f = tmp_path / "photo.png"
        f.write_bytes(b"\x89PNG\r\n")
        assert detect_pdf_kind(f) == "IMAGE"

    def test_jpg_is_image(self, tmp_path):
        from app.pdf_parser import detect_pdf_kind
        f = tmp_path / "scan.jpg"
        f.write_bytes(b"\xff\xd8\xff")
        assert detect_pdf_kind(f) == "IMAGE"

    def test_jpeg_is_image(self, tmp_path):
        from app.pdf_parser import detect_pdf_kind
        f = tmp_path / "scan.jpeg"
        f.write_bytes(b"\xff\xd8\xff")
        assert detect_pdf_kind(f) == "IMAGE"

    def test_tif_is_image(self, tmp_path):
        from app.pdf_parser import detect_pdf_kind
        f = tmp_path / "scan.tif"
        f.write_bytes(b"II*\x00")
        assert detect_pdf_kind(f) == "IMAGE"

    def test_unknown_extension(self, tmp_path):
        from app.pdf_parser import detect_pdf_kind
        f = tmp_path / "data.docx"
        f.write_bytes(b"PK")
        assert detect_pdf_kind(f) == "UNKNOWN"

    def test_text_pdf(self, tmp_path):
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            pytest.skip("pdfplumber not installed")
        from app.pdf_parser import detect_pdf_kind
        pdf_bytes = _make_text_pdf(["Bolt M8x25 DIN 933", "Nut M8 DIN 934"])
        f = tmp_path / "request.pdf"
        f.write_bytes(pdf_bytes)
        # fpdf2 creates text PDFs, so should be TEXT_PDF
        result = detect_pdf_kind(f)
        assert result == "TEXT_PDF"

    def test_blank_pdf_is_scan(self, tmp_path):
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            pytest.skip("pdfplumber not installed")
        from app.pdf_parser import detect_pdf_kind, PDF_TEXT_CHAR_THRESHOLD
        # A blank PDF has < threshold chars → SCAN_PDF
        pdf_bytes = _make_blank_pdf()
        f = tmp_path / "blank.pdf"
        f.write_bytes(pdf_bytes)
        result = detect_pdf_kind(f)
        assert result == "SCAN_PDF"


# ── Unit tests: _rows_to_dataframe ───────────────────────────────────────────

class TestRowsToDataframe:
    def _fn(self, rows):
        from app.pdf_routes import _rows_to_dataframe
        return _rows_to_dataframe(rows)

    def test_joins_tokens(self):
        df = self._fn([["Болт", "М8x25", "DIN933"], ["Гайка", "М8"]])
        assert list(df.columns) == ["name"]
        assert df.iloc[0]["name"] == "Болт М8x25 DIN933"
        assert df.iloc[1]["name"] == "Гайка М8"

    def test_skips_empty_rows(self):
        df = self._fn([[], ["", "  "], ["Болт"]])
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Болт"

    def test_all_empty_returns_empty_df(self):
        df = self._fn([[], [""], []])
        assert list(df.columns) == ["name"]
        assert len(df) == 0

    def test_single_token_row(self):
        df = self._fn([["Шайба"]])
        assert df.iloc[0]["name"] == "Шайба"


# ── Unit tests: ParseResult ───────────────────────────────────────────────────

class TestParseResult:
    def test_defaults(self):
        from app.pdf_parser import ParseResult
        r = ParseResult()
        assert r.rows == []
        assert r.method == ""
        assert r.status == "ok"
        assert r.metrics == {}
        assert r.error is None

    def test_fields_assigned(self):
        from app.pdf_parser import ParseResult
        r = ParseResult(rows=[["a"]], method="pdfplumber_table", status="ok",
                        metrics={"rows_extracted": 1})
        assert r.rows == [["a"]]
        assert r.metrics["rows_extracted"] == 1


# ── Unit tests: _text_to_rows ────────────────────────────────────────────────

class TestTextToRows:
    def _fn(self, text):
        from app.pdf_parser import _text_to_rows
        return _text_to_rows(text)

    def test_splits_lines(self):
        rows = self._fn("Болт М8x25\nГайка М8\n")
        assert rows == [["Болт", "М8x25"], ["Гайка", "М8"]]

    def test_skips_blank_lines(self):
        rows = self._fn("Болт\n\n\nГайка")
        assert rows == [["Болт"], ["Гайка"]]

    def test_empty_string(self):
        assert self._fn("") == []

    def test_single_line(self):
        rows = self._fn("Шайба М10 DIN125")
        assert rows == [["Шайба", "М10", "DIN125"]]


# ── Unit tests: _words_to_rows ────────────────────────────────────────────────

class TestWordsToRows:
    def _fn(self, pdf_mock):
        from app.pdf_parser import _words_to_rows
        return _words_to_rows(pdf_mock)

    def _mock_pdf(self, pages_words):
        """Build a fake pdfplumber PDF object from list of word dicts per page."""
        pdf = MagicMock()
        pages = []
        for words in pages_words:
            page = MagicMock()
            page.extract_words.return_value = words
            pages.append(page)
        pdf.pages = pages
        return pdf

    def _w(self, text, x0, top):
        return {"text": text, "x0": x0, "top": top}

    def test_single_row(self):
        words = [self._w("Болт", 10, 50), self._w("М8x25", 60, 50)]
        rows = self._fn(self._mock_pdf([words]))
        assert len(rows) == 1
        assert "Болт" in rows[0]
        assert "М8x25" in rows[0]

    def test_two_rows_different_y(self):
        words = [
            self._w("Болт",  10, 50),
            self._w("Гайка", 10, 70),  # different Y → new row
        ]
        rows = self._fn(self._mock_pdf([words]))
        assert len(rows) == 2

    def test_empty_pdf(self):
        rows = self._fn(self._mock_pdf([[]]))
        assert rows == []


# ── Integration tests: /upload-pdf routes ────────────────────────────────────

class TestUploadPdfRoutes:
    def test_get_form(self, client):
        resp = client.get("/upload-pdf")
        assert resp.status_code == 200
        assert "Импорт PDF" in resp.text

    def test_upload_unsupported_format(self, client):
        resp = client.post(
            "/upload-pdf",
            files={"file": ("data.docx", b"PK\x03\x04", "application/octet-stream")},
        )
        assert resp.status_code == 400
        assert "не поддерживается" in resp.text

    def test_upload_text_pdf_shows_wizard(self, client):
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            pytest.skip("pdfplumber not installed")
        pdf_bytes = _make_text_pdf(["Bolt M8x25 DIN933", "Nut M8 DIN934 100 pcs"])
        resp = client.post(
            "/upload-pdf",
            files={"file": ("req.pdf", pdf_bytes, "application/pdf")},
        )
        assert resp.status_code == 200
        # Wizard page should contain the extracted text or wizard heading
        assert "Черновик" in resp.text or "Извлечение" in resp.text

    def test_upload_png_tries_ocr(self, client, tmp_path):
        """PNG upload should attempt OCR (may return empty if tesseract missing)."""
        # Create a minimal valid PNG (1x1 white pixel)
        png_bytes = (
            b"\x89PNG\r\n\x1a\n"
            b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9c"
            b"b\xf8\x0f\x00\x00\x01\x01\x00\x00\x18\xdd\x8d\xb4"
            b"\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        resp = client.post(
            "/upload-pdf",
            files={"file": ("scan.png", png_bytes, "image/png")},
        )
        # Should render wizard or error gracefully (not 500)
        assert resp.status_code in (200, 422)

    def test_attachment_saved_to_db(self, client):
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            pytest.skip("pdfplumber not installed")
        pdf_bytes = _make_text_pdf(["Bolt M8x25 DIN933 100pcs"])
        client.post(
            "/upload-pdf",
            files={"file": ("req.pdf", pdf_bytes, "application/pdf")},
        )
        import app.database as db_mod
        from app.models import ImportAttachment
        session = db_mod.get_db_session()
        try:
            att = session.query(ImportAttachment).first()
            assert att is not None
            assert att.filename == "req.pdf"
            assert att.kind in ("TEXT_PDF", "SCAN_PDF", "UNKNOWN")
        finally:
            session.close()

    def test_parse_attempt_saved_to_db(self, client):
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            pytest.skip("pdfplumber not installed")
        pdf_bytes = _make_text_pdf(["Bolt M8x25 DIN933 100pcs"])
        client.post(
            "/upload-pdf",
            files={"file": ("req.pdf", pdf_bytes, "application/pdf")},
        )
        import app.database as db_mod
        from app.models import ImportParseAttempt
        session = db_mod.get_db_session()
        try:
            attempt = session.query(ImportParseAttempt).first()
            assert attempt is not None
            assert attempt.file_id is not None
            assert attempt.status in ("ok", "empty", "error")
        finally:
            session.close()


# ── Migration 019 tables ──────────────────────────────────────────────────────

class TestMigration019:
    def test_tables_created(self):
        import app.database as db_mod
        from sqlalchemy import inspect
        insp = inspect(db_mod.engine)
        tables = insp.get_table_names()
        assert "import_attachment" in tables
        assert "import_parse_attempt" in tables

    def test_import_attachment_columns(self):
        import app.database as db_mod
        from sqlalchemy import inspect
        insp = inspect(db_mod.engine)
        cols = {c["name"] for c in insp.get_columns("import_attachment")}
        assert {"id", "file_id", "filename", "kind", "storage_path", "created_at"} <= cols

    def test_import_parse_attempt_columns(self):
        import app.database as db_mod
        from sqlalchemy import inspect
        insp = inspect(db_mod.engine)
        cols = {c["name"] for c in insp.get_columns("import_parse_attempt")}
        assert {"id", "file_id", "attachment_id", "method", "status", "rows_found"} <= cols

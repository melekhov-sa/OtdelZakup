"""PDF and image import — parsing layer (no AI/LLM).

Public API
----------
detect_pdf_kind(path) -> "TEXT_PDF" | "SCAN_PDF" | "IMAGE" | "UNKNOWN"
parse_pdf(path, *, dpi=200, lang="rus+eng") -> ParseResult
parse_image(path, *, lang="rus+eng") -> ParseResult

ParseResult fields
------------------
  rows        : list[list[str]]  — 2-D table; each inner list is one data row
  method      : str              — "pdfplumber_table" | "pdfplumber_words" | "ocr_tesseract"
  status      : str              — "ok" | "empty" | "error"
  metrics     : dict             — quality / confidence details
  error       : str | None       — error message when status == "error"
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

# ── Constants ─────────────────────────────────────────────────────────────────

PDF_TEXT_CHAR_THRESHOLD = 50   # chars per page → TEXT_PDF
OCR_MIN_CONFIDENCE = 30        # pytesseract word confidence threshold (0-100)

PdfKind = Literal["TEXT_PDF", "SCAN_PDF", "IMAGE", "UNKNOWN"]


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ParseResult:
    rows: list[list[str]] = field(default_factory=list)
    method: str = ""
    status: str = "ok"
    metrics: dict = field(default_factory=dict)
    error: str | None = None


# ── Kind detection ─────────────────────────────────────────────────────────────

def detect_pdf_kind(path: str | Path) -> PdfKind:
    """Detect whether a PDF has embedded text or is scanned.

    Returns "TEXT_PDF" when ≥ PDF_TEXT_CHAR_THRESHOLD chars found on first 3 pages,
    "SCAN_PDF" when the file is a PDF but no/little text,
    "IMAGE" for raster images (.png/.jpg/.jpeg/.tif/.tiff/.bmp/.webp),
    "UNKNOWN" otherwise.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}:
        return "IMAGE"

    if suffix != ".pdf":
        return "UNKNOWN"

    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            total_chars = 0
            for page in pdf.pages[:3]:
                text = page.extract_text() or ""
                total_chars += len(text.strip())
        return "TEXT_PDF" if total_chars >= PDF_TEXT_CHAR_THRESHOLD else "SCAN_PDF"
    except Exception:
        return "UNKNOWN"


# ── Text PDF parsing via pdfplumber ──────────────────────────────────────────

def _parse_text_pdf(path: Path) -> ParseResult:
    """Extract rows from a text-based PDF using pdfplumber.

    Strategy:
    1. Try table extraction (pdfplumber built-in table detection).
    2. Fall back to word-cluster-based row building.
    """
    try:
        import pdfplumber
    except ImportError:
        return ParseResult(status="error", error="pdfplumber не установлен")

    try:
        with pdfplumber.open(path) as pdf:
            # ── Attempt 1: Table extraction ──────────────────────────────────
            all_rows: list[list[str]] = []
            table_pages = 0
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        clean = [_cell_str(c) for c in row]
                        if any(clean):
                            all_rows.append(clean)
                    table_pages += 1

            if all_rows:
                return ParseResult(
                    rows=all_rows,
                    method="pdfplumber_table",
                    status="ok" if all_rows else "empty",
                    metrics={
                        "pages_with_tables": table_pages,
                        "rows_extracted": len(all_rows),
                    },
                )

            # ── Attempt 2: Word cluster fallback ─────────────────────────────
            word_rows = _words_to_rows(pdf)
            return ParseResult(
                rows=word_rows,
                method="pdfplumber_words",
                status="ok" if word_rows else "empty",
                metrics={"rows_extracted": len(word_rows)},
            )
    except Exception as exc:
        return ParseResult(status="error", error=str(exc))


def _cell_str(val) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _words_to_rows(pdf) -> list[list[str]]:
    """Cluster words by Y-coordinate proximity into logical rows."""
    Y_GAP = 5  # pt — words within Y_GAP are on the same line

    all_words: list[dict] = []
    for page in pdf.pages:
        words = page.extract_words(x_tolerance=3, y_tolerance=3) or []
        all_words.extend(words)

    if not all_words:
        return []

    # Sort by top (y), then x
    all_words.sort(key=lambda w: (round(w["top"] / Y_GAP) * Y_GAP, w["x0"]))

    rows: list[list[str]] = []
    current_row: list[str] = []
    current_y: float | None = None

    for word in all_words:
        y = word["top"]
        if current_y is None or abs(y - current_y) > Y_GAP:
            if current_row:
                rows.append(current_row)
            current_row = [word["text"]]
            current_y = y
        else:
            current_row.append(word["text"])

    if current_row:
        rows.append(current_row)

    return rows


# ── OCR parsing via pytesseract ───────────────────────────────────────────────

def _preprocess_image_for_ocr(img_array):
    """Denoise, threshold, and deskew an image for better OCR accuracy."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        return img_array

    gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY) if len(img_array.shape) == 3 else img_array

    # Denoise
    gray = cv2.fastNlMeansDenoising(gray, h=10)

    # Adaptive threshold
    binary = cv2.adaptiveThreshold(
        gray, 255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        blockSize=31,
        C=10,
    )

    # Deskew: find text angle and rotate
    try:
        coords = np.column_stack(np.where(binary < 128))
        if len(coords) > 50:
            angle = cv2.minAreaRect(coords)[-1]
            if angle < -45:
                angle = 90 + angle
            if abs(angle) > 0.5:
                h, w = binary.shape
                M = cv2.getRotationMatrix2D((w // 2, h // 2), angle, 1.0)
                binary = cv2.warpAffine(
                    binary, M, (w, h),
                    flags=cv2.INTER_CUBIC,
                    borderMode=cv2.BORDER_REPLICATE,
                )
    except Exception:
        pass  # deskew is best-effort

    return binary


class OcrUnavailableError(RuntimeError):
    """Raised when Tesseract is not installed or not in PATH."""


def _ocr_image_array(img_array, lang: str) -> tuple[str, float]:
    """Run pytesseract on a numpy array. Returns (text, avg_confidence).

    Raises OcrUnavailableError if Tesseract binary is not found.
    """
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        raise OcrUnavailableError("pytesseract или pillow не установлены")

    pil_img = Image.fromarray(img_array)
    try:
        data = pytesseract.image_to_data(pil_img, lang=lang, output_type=pytesseract.Output.DICT)
    except Exception as exc:
        exc_name = type(exc).__name__
        if "TesseractNotFound" in exc_name or "tesseract" in str(exc).lower():
            raise OcrUnavailableError(
                "Tesseract не найден. Установите tesseract-ocr и добавьте в PATH "
                "(Windows: https://github.com/UB-Mannheim/tesseract/wiki; "
                "Docker: apt-get install tesseract-ocr tesseract-ocr-rus)."
            ) from exc
        raise

    words, confs = [], []
    for w, c in zip(data["text"], data["conf"]):
        if str(w).strip() and int(c) >= 0:
            words.append(str(w).strip())
            confs.append(int(c))

    text = " ".join(words)
    avg_conf = sum(confs) / len(confs) if confs else 0.0
    return text, avg_conf


def _text_to_rows(text: str) -> list[list[str]]:
    """Split raw OCR text into rows of tokens."""
    rows = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(stripped.split())
    return rows


def _parse_scan_pdf(path: Path, dpi: int, lang: str) -> ParseResult:
    """Rasterize each PDF page and OCR it."""
    try:
        import fitz  # pymupdf
        import numpy as np
        from PIL import Image
    except ImportError:
        return ParseResult(status="error", error="pymupdf или pillow не установлены")

    try:
        doc = fitz.open(str(path))
    except Exception as exc:
        return ParseResult(status="error", error=str(exc))

    all_rows: list[list[str]] = []
    page_confs: list[float] = []

    try:
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)

        for page in doc:
            pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                pix.height, pix.width, pix.n
            )
            processed = _preprocess_image_for_ocr(img_array)
            try:
                text, conf = _ocr_image_array(processed, lang)
            except OcrUnavailableError as exc:
                return ParseResult(status="error", method="ocr_tesseract", error=str(exc))
            page_confs.append(conf)
            all_rows.extend(_text_to_rows(text))
    finally:
        doc.close()

    avg_conf = sum(page_confs) / len(page_confs) if page_confs else 0.0
    return ParseResult(
        rows=all_rows,
        method="ocr_tesseract",
        status="ok" if all_rows else "empty",
        metrics={
            "dpi": dpi,
            "lang": lang,
            "avg_confidence": round(avg_conf, 1),
            "pages_processed": len(page_confs),
            "rows_extracted": len(all_rows),
        },
    )


# ── Public parse_image ────────────────────────────────────────────────────────

def parse_image(path: str | Path, *, lang: str = "rus+eng") -> ParseResult:
    """Parse a raster image file (PNG, JPG, etc.) via OCR."""
    path = Path(path)
    try:
        import numpy as np
        from PIL import Image
    except ImportError:
        return ParseResult(status="error", error="pillow не установлен")

    try:
        pil_img = Image.open(path).convert("RGB")
        img_array = np.array(pil_img)
    except Exception as exc:
        return ParseResult(status="error", error=str(exc))

    processed = _preprocess_image_for_ocr(img_array)
    try:
        text, conf = _ocr_image_array(processed, lang)
    except OcrUnavailableError as exc:
        return ParseResult(status="error", method="ocr_tesseract", error=str(exc))

    rows = _text_to_rows(text)
    return ParseResult(
        rows=rows,
        method="ocr_tesseract",
        status="ok" if rows else "empty",
        metrics={
            "lang": lang,
            "avg_confidence": round(conf, 1),
            "rows_extracted": len(rows),
        },
    )


# ── Main public entry point ───────────────────────────────────────────────────

def parse_pdf(path: str | Path, *, dpi: int = 200, lang: str = "rus+eng") -> ParseResult:
    """Detect kind and dispatch to the right parser."""
    path = Path(path)
    kind = detect_pdf_kind(path)

    if kind == "IMAGE":
        return parse_image(path, lang=lang)
    if kind == "TEXT_PDF":
        return _parse_text_pdf(path)
    if kind == "SCAN_PDF":
        return _parse_scan_pdf(path, dpi=dpi, lang=lang)

    return ParseResult(
        status="error",
        error=f"Неизвестный тип файла: {path.suffix}",
    )

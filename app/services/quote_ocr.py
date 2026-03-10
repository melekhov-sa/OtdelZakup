"""Quote OCR service — extract raw tables from PDF/image via Google Document AI.

No smart parsing, no column detection. Just raw table extraction and storage.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _extract_all_tables(document: dict) -> list[dict]:
    """Extract ALL tables from Document AI response as raw cell grids.

    Returns list of {page_no, rows: [[cell_text, ...], ...], confidence_avg}.
    No filtering, no merging — every table from every page.
    """
    document_text: str = document.get("text") or ""
    pages: list = document.get("pages") or []
    tables: list[dict] = []

    for page_idx, page in enumerate(pages):
        page_no = page_idx + 1
        for table in (page.get("tables") or []):
            header_rows = table.get("headerRows") or []
            body_rows = table.get("bodyRows") or []

            rows: list[list[str]] = []
            confidences: list[float] = []

            for row in header_rows + body_rows:
                cells: list[str] = []
                for cell in (row.get("cells") or []):
                    layout = cell.get("layout") or {}
                    anchor = layout.get("textAnchor") or {}
                    segments = anchor.get("textSegments") or []
                    text_parts = []
                    for seg in segments:
                        start = int(seg.get("startIndex", 0))
                        end = int(seg.get("endIndex", 0))
                        text_parts.append(document_text[start:end])
                    cell_text = "".join(text_parts).strip()
                    cells.append(cell_text)

                    conf = layout.get("confidence")
                    if conf is not None:
                        confidences.append(float(conf))
                rows.append(cells)

            n_rows = len(rows)
            n_cols = max((len(r) for r in rows), default=0)
            avg_conf = round(sum(confidences) / len(confidences) * 100, 1) if confidences else None

            tables.append({
                "page_no": page_no,
                "rows": rows,
                "n_rows": n_rows,
                "n_cols": n_cols,
                "confidence_avg": avg_conf,
            })

    return tables


def run_quote_ocr(
    file_bytes: bytes,
    filename: str,
    content_type: str,
    session: Session,
) -> int:
    """Process a file through Google Document AI, extract tables, save to DB.

    Returns the QuoteOcrJob.id.
    """
    from app.order_models import QuoteOcrJob, QuoteOcrTable

    job = QuoteOcrJob(
        filename=filename,
        content_type=content_type,
        processor_type="document_ai",
        status="pending",
    )
    session.add(job)
    session.flush()
    job_id = job.id

    try:
        from app.integrations.google_document_ai import process_document

        document = process_document(file_bytes, content_type)

        pages = document.get("pages") or []
        job.page_count = len(pages)

        # Avg confidence across all blocks
        all_confs: list[float] = []
        for page in pages:
            for block in (page.get("blocks") or []):
                conf = (block.get("layout") or {}).get("confidence")
                if conf is not None:
                    all_confs.append(float(conf))
        job.confidence_avg = round(sum(all_confs) / len(all_confs) * 100, 1) if all_confs else None

        tables = _extract_all_tables(document)
        job.tables_found = len(tables)

        for idx, tbl in enumerate(tables):
            ocr_table = QuoteOcrTable(
                job_id=job_id,
                table_index=idx,
                page_no=tbl["page_no"],
                n_rows=tbl["n_rows"],
                n_cols=tbl["n_cols"],
                confidence_avg=tbl["confidence_avg"],
                raw_json=json.dumps(tbl["rows"], ensure_ascii=False),
            )
            session.add(ocr_table)

        job.status = "done"
        logger.info(
            "OCR job #%d: %d pages, %d tables extracted from '%s'",
            job_id, job.page_count, len(tables), filename,
        )

    except Exception as exc:
        job.status = "error"
        job.error = str(exc)[:1000]
        logger.error("OCR job #%d failed: %s", job_id, exc)

    session.commit()
    return job_id

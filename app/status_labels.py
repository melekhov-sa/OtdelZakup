"""Unified status labels for the application."""

# Internal status values used in data-status attributes and DataFrame
STATUS_OK = "ok"
STATUS_REVIEW = "review"
STATUS_MANUAL = "manual"

# Internal -> Russian display label
STATUS_LABELS: dict[str, str] = {
    STATUS_OK: "Не требует проверки",
    STATUS_REVIEW: "Требуется просмотреть",
    STATUS_MANUAL: "Требуется вручную разобрать",
}


def status_label(status: str) -> str:
    """Return Russian display label for a status value."""
    return STATUS_LABELS.get(status, status)

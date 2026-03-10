"""Google Document AI client wrapper.

Authorization strategy (priority order):
  1. JSON сервисного аккаунта — если задан в настройках (/settings/google-ocr)
     или в переменной окружения GOOGLE_CREDENTIALS_JSON.
  2. Application Default Credentials (ADC) — в остальных случаях.
     Локально: выполнить ``gcloud auth application-default login``.
     Docker:   пробросить том ``-v ~/.config/gcloud:/root/.config/gcloud``.

JSON ключ НЕ является обязательным.  ADC — предпочтительный вариант
для локальной разработки, когда политика запрещает создание ключей
сервисных аккаунтов (iam.disableServiceAccountKeyCreation).

Configuration (DB via SystemSetting, fallback to env vars):
    google_ocr_project_id   / GOOGLE_PROJECT_ID
    google_ocr_location     / GOOGLE_LOCATION
    google_ocr_processor_id / GOOGLE_PROCESSOR_ID
    google_ocr_credentials_json / GOOGLE_CREDENTIALS_JSON   (optional)
"""
from __future__ import annotations

import os
from typing import Any


# ── Custom exceptions ──────────────────────────────────────────────────────────

class GoogleDocAIError(Exception):
    """Базовая ошибка Google Document AI."""


class GoogleCredentialsError(GoogleDocAIError):
    """Ошибка аутентификации Google."""


class GoogleQuotaError(GoogleDocAIError):
    """Превышен лимит запросов Google Document AI."""


class GoogleNotFoundError(GoogleDocAIError):
    """Ресурс не найден в Google Document AI (неверный processor_id или location)."""


class GoogleFileSizeError(GoogleDocAIError):
    """Файл слишком большой для Google Document AI."""


# ── Config helpers ─────────────────────────────────────────────────────────────

def _db_setting(key: str) -> str:
    """Read one SystemSetting value from the DB; return '' on any error."""
    try:
        from app.database import get_db_session  # noqa: PLC0415
        from app.models import SystemSetting  # noqa: PLC0415

        session = get_db_session()
        try:
            row = session.query(SystemSetting).filter_by(key=key).first()
            return (row.value or "").strip() if row else ""
        finally:
            session.close()
    except Exception:
        return ""


def _get(db_key: str, env_key: str) -> str:
    """DB value (if non-empty), else env var, else ''."""
    return _db_setting(db_key) or os.environ.get(env_key, "").strip()


def is_configured() -> bool:
    """Return True when all three required connection params are available."""
    return bool(
        _get("google_ocr_project_id",   "GOOGLE_PROJECT_ID")
        and _get("google_ocr_location",     "GOOGLE_LOCATION")
        and _get("google_ocr_processor_id", "GOOGLE_PROCESSOR_ID")
    )


def _load_credentials():
    """Build Google credentials object, or return None to use ADC."""
    json_str = _get("google_ocr_credentials_json", "GOOGLE_CREDENTIALS_JSON")
    if not json_str:
        return None  # Application Default Credentials (env GOOGLE_APPLICATION_CREDENTIALS)

    import json  # noqa: PLC0415

    try:
        info = json.loads(json_str)
    except Exception as exc:
        raise GoogleCredentialsError(
            "Не удалось разобрать JSON сервисного аккаунта — "
            "проверьте настройки Google OCR."
        ) from exc

    try:
        from google.oauth2 import service_account  # noqa: PLC0415
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    except Exception as exc:
        raise GoogleCredentialsError(
            f"Не удалось создать учётные данные из JSON сервисного аккаунта: {exc}"
        ) from exc


# ── Public API ─────────────────────────────────────────────────────────────────

def process_document(file_bytes: bytes, mime_type: str) -> dict[str, Any]:
    """Send *file_bytes* to Google Document AI; return the response as a plain dict.

    Retries once on transient 5xx / deadline errors.

    Raises
    ------
    GoogleCredentialsError  — authentication / permission problem
    GoogleQuotaError        — quota exceeded (429 / ResourceExhausted)
    GoogleNotFoundError     — bad processor_id or location (404)
    GoogleFileSizeError     — file too large (>20 MB or rejected by the API)
    GoogleDocAIError        — any other error
    """
    project_id   = _get("google_ocr_project_id",   "GOOGLE_PROJECT_ID")
    location     = _get("google_ocr_location",     "GOOGLE_LOCATION")
    processor_id = _get("google_ocr_processor_id", "GOOGLE_PROCESSOR_ID")

    if not project_id or not location or not processor_id:
        raise GoogleCredentialsError(
            "Google Document AI не настроен. "
            "Укажите Project ID, Location и Processor ID в настройках Google OCR."
        )

    if len(file_bytes) > 20 * 1024 * 1024:
        raise GoogleFileSizeError(
            "Файл слишком большой: Google Document AI принимает не более 20 МБ."
        )

    try:
        import google.auth.exceptions as gauthexc  # noqa: PLC0415
        from google.api_core import exceptions as gexc  # noqa: PLC0415
        from google.cloud import documentai  # noqa: PLC0415
        from google.protobuf.json_format import MessageToDict  # noqa: PLC0415
    except ImportError as exc:
        raise GoogleDocAIError(
            "Пакет google-cloud-documentai не установлен. "
            "Выполните: pip install google-cloud-documentai"
        ) from exc

    credentials = _load_credentials()
    client_options = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    try:
        client = documentai.DocumentProcessorServiceClient(
            credentials=credentials,
            client_options=client_options,
        )
    except gauthexc.DefaultCredentialsError as exc:
        raise GoogleCredentialsError(
            "Не удалось авторизоваться в Google. "
            "Выполните: gcloud auth application-default login  "
            "— или задайте JSON сервисного аккаунта в настройках: /settings/google-ocr"
        ) from exc

    name = client.processor_path(project_id, location, processor_id)
    raw_doc = documentai.RawDocument(content=file_bytes, mime_type=mime_type)
    request_obj = documentai.ProcessRequest(name=name, raw_document=raw_doc)

    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            response = client.process_document(request=request_obj)
            return MessageToDict(response.document._pb)
        except gexc.Unauthenticated as exc:
            raise GoogleCredentialsError(
                f"Ошибка аутентификации Google: {exc}"
            ) from exc
        except gexc.PermissionDenied as exc:
            raise GoogleCredentialsError(
                f"Доступ запрещён (проверьте роли сервисного аккаунта): {exc}"
            ) from exc
        except gexc.ResourceExhausted as exc:
            raise GoogleQuotaError(
                f"Превышен лимит запросов Google Document AI: {exc}"
            ) from exc
        except gexc.NotFound as exc:
            raise GoogleNotFoundError(
                f"Ресурс не найден в Google Document AI "
                f"(проверьте Processor ID и Location): {exc}"
            ) from exc
        except gexc.InvalidArgument as exc:
            msg = str(exc).lower()
            if "too large" in msg or "size" in msg:
                raise GoogleFileSizeError(
                    f"Файл отклонён как слишком большой: {exc}"
                ) from exc
            raise GoogleDocAIError(
                f"Некорректный запрос к Google Document AI: {exc}"
            ) from exc
        except (
            gexc.ServiceUnavailable,
            gexc.InternalServerError,
            gexc.DeadlineExceeded,
        ) as exc:
            last_exc = exc
            if attempt == 0:
                continue  # retry once
            raise GoogleDocAIError(
                f"Временная ошибка Google Document AI после повторной попытки: {exc}"
            ) from exc
        except Exception as exc:
            raise GoogleDocAIError(f"Ошибка Google Document AI: {exc}") from exc

    raise GoogleDocAIError(f"Ошибка Google Document AI: {last_exc}")

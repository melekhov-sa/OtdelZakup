"""Google Document AI client wrapper.

Reads credentials from environment variables only — keys are never logged or printed.

Required env vars:
    GOOGLE_PROJECT_ID      — GCP project ID
    GOOGLE_LOCATION        — processor location (e.g. "eu" or "us")
    GOOGLE_PROCESSOR_ID    — Document AI processor ID

Optional env vars (choose one):
    GOOGLE_CREDENTIALS_JSON     — service account JSON as a string
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON file (ADC)
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


# ── Helpers ────────────────────────────────────────────────────────────────────

def is_configured() -> bool:
    """Return True if all required env vars are set."""
    required = ("GOOGLE_PROJECT_ID", "GOOGLE_LOCATION", "GOOGLE_PROCESSOR_ID")
    return all(os.environ.get(v, "").strip() for v in required)


def _load_credentials():
    """Load Google service account credentials from env vars, or None for ADC."""
    json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not json_str:
        return None  # use Application Default Credentials (ADC)

    import json  # noqa: PLC0415

    try:
        info = json.loads(json_str)
    except Exception as exc:
        raise GoogleCredentialsError(
            "Не удалось разобрать GOOGLE_CREDENTIALS_JSON как JSON."
        ) from exc

    try:
        from google.oauth2 import service_account  # noqa: PLC0415
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    except Exception as exc:
        raise GoogleCredentialsError(
            f"Не удалось создать учётные данные из GOOGLE_CREDENTIALS_JSON: {exc}"
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
    project_id   = os.environ.get("GOOGLE_PROJECT_ID", "").strip()
    location     = os.environ.get("GOOGLE_LOCATION", "").strip()
    processor_id = os.environ.get("GOOGLE_PROCESSOR_ID", "").strip()

    if not project_id or not location or not processor_id:
        raise GoogleCredentialsError(
            "Не заданы переменные окружения GOOGLE_PROJECT_ID, "
            "GOOGLE_LOCATION и/или GOOGLE_PROCESSOR_ID."
        )

    if len(file_bytes) > 20 * 1024 * 1024:
        raise GoogleFileSizeError(
            "Файл слишком большой: Google Document AI принимает не более 20 МБ."
        )

    try:
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
    client = documentai.DocumentProcessorServiceClient(
        credentials=credentials,
        client_options=client_options,
    )

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
                f"(проверьте GOOGLE_PROCESSOR_ID и GOOGLE_LOCATION): {exc}"
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

"""Tests for Google Document AI authorization logic.

All tests mock the Google SDK — no network calls, no real credentials required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_fake_json() -> str:
    """Minimal service account JSON string (invalid key, only for parsing tests)."""
    import json
    return json.dumps({
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "key1",
        "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\n-----END RSA PRIVATE KEY-----\n",
        "client_email": "test@test-project.iam.gserviceaccount.com",
        "token_uri": "https://oauth2.googleapis.com/token",
    })


# ── _load_credentials ──────────────────────────────────────────────────────────

class TestLoadCredentials:
    def test_no_json_returns_none(self):
        """When no JSON is set anywhere, _load_credentials returns None (→ use ADC)."""
        from app.integrations.google_document_ai import _load_credentials

        with patch("app.integrations.google_document_ai._get", return_value=""):
            result = _load_credentials()
        assert result is None

    def test_valid_json_returns_credentials(self):
        """When a valid service-account JSON is present, credentials object is returned."""
        from app.integrations.google_document_ai import _load_credentials

        fake_creds = MagicMock()
        with (
            patch("app.integrations.google_document_ai._get", return_value=_make_fake_json()),
            patch(
                "google.oauth2.service_account.Credentials.from_service_account_info",
                return_value=fake_creds,
            ),
        ):
            result = _load_credentials()
        assert result is fake_creds

    def test_invalid_json_raises_credentials_error(self):
        """Malformed JSON raises GoogleCredentialsError with a helpful message."""
        from app.integrations.google_document_ai import GoogleCredentialsError, _load_credentials

        with patch("app.integrations.google_document_ai._get", return_value="not-json{{{"):
            with pytest.raises(GoogleCredentialsError, match="JSON"):
                _load_credentials()


# ── process_document — client creation ────────────────────────────────────────

class TestProcessDocumentAuth:
    """Tests that process_document passes credentials correctly to the client."""

    _ENV = {
        "google_ocr_project_id":   "proj",
        "google_ocr_location":     "eu",
        "google_ocr_processor_id": "proc123",
        "google_ocr_credentials_json": "",
    }

    def _mock_get(self, key: str, env_key: str) -> str:  # noqa: ARG002
        return self._ENV.get(key, "")

    def test_no_json_client_receives_none_credentials(self):
        """Without JSON, credentials=None is passed → client uses ADC."""
        from app.integrations.google_document_ai import process_document

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.document._pb = MagicMock()

        with (
            patch("app.integrations.google_document_ai._get", side_effect=self._mock_get),
            patch("app.integrations.google_document_ai._load_credentials", return_value=None),
            patch("google.auth.exceptions"),
            patch(
                "google.cloud.documentai.DocumentProcessorServiceClient",
                return_value=mock_client,
            ) as mock_cls,
            patch("google.protobuf.json_format.MessageToDict", return_value={"text": ""}),
        ):
            mock_client.process_document.return_value = mock_response
            mock_client.processor_path.return_value = "projects/proj/locations/eu/processors/proc123"

            process_document(b"fake", "application/pdf")

            # credentials=None was passed to client constructor
            _, kwargs = mock_cls.call_args
            assert kwargs.get("credentials") is None

    def test_json_set_client_receives_explicit_credentials(self):
        """When JSON is set, explicit credentials object is passed to client."""
        from app.integrations.google_document_ai import process_document

        fake_creds = MagicMock()
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.document._pb = MagicMock()

        env = dict(self._ENV, **{"google_ocr_credentials_json": _make_fake_json()})

        def _get(key, env_key=""):  # noqa: ARG001
            return env.get(key, "")

        with (
            patch("app.integrations.google_document_ai._get", side_effect=_get),
            patch(
                "app.integrations.google_document_ai._load_credentials",
                return_value=fake_creds,
            ),
            patch(
                "google.cloud.documentai.DocumentProcessorServiceClient",
                return_value=mock_client,
            ) as mock_cls,
            patch("google.protobuf.json_format.MessageToDict", return_value={"text": ""}),
        ):
            mock_client.process_document.return_value = mock_response
            mock_client.processor_path.return_value = "projects/proj/locations/eu/processors/proc123"

            process_document(b"fake", "application/pdf")

            _, kwargs = mock_cls.call_args
            assert kwargs.get("credentials") is fake_creds

    def test_missing_adc_raises_credentials_error_with_gcloud_hint(self):
        """DefaultCredentialsError → GoogleCredentialsError mentioning gcloud."""
        import google.auth.exceptions as gauthexc

        from app.integrations.google_document_ai import GoogleCredentialsError, process_document

        with (
            patch("app.integrations.google_document_ai._get", side_effect=self._mock_get),
            patch("app.integrations.google_document_ai._load_credentials", return_value=None),
            patch(
                "google.cloud.documentai.DocumentProcessorServiceClient",
                side_effect=gauthexc.DefaultCredentialsError("no creds"),
            ),
        ):
            with pytest.raises(GoogleCredentialsError) as exc_info:
                process_document(b"fake", "application/pdf")

        assert "gcloud" in str(exc_info.value).lower()

    def test_missing_project_id_raises_credentials_error(self):
        """Empty project_id must raise before even touching the Google client."""
        from app.integrations.google_document_ai import GoogleCredentialsError, process_document

        with patch("app.integrations.google_document_ai._get", return_value=""):
            with pytest.raises(GoogleCredentialsError, match="не настроен"):
                process_document(b"fake", "application/pdf")


# ── is_configured ──────────────────────────────────────────────────────────────

class TestIsConfigured:
    def test_all_three_params_set(self):
        from app.integrations.google_document_ai import is_configured

        def _get(key, env_key=""):  # noqa: ARG001
            return {"google_ocr_project_id": "p", "google_ocr_location": "eu",
                    "google_ocr_processor_id": "x"}.get(key, "")

        with patch("app.integrations.google_document_ai._get", side_effect=_get):
            assert is_configured() is True

    def test_missing_processor_id(self):
        from app.integrations.google_document_ai import is_configured

        def _get(key, env_key=""):  # noqa: ARG001
            return {"google_ocr_project_id": "p", "google_ocr_location": "eu"}.get(key, "")

        with patch("app.integrations.google_document_ai._get", side_effect=_get):
            assert is_configured() is False

    def test_no_json_does_not_affect_is_configured(self):
        """Absence of JSON must not make is_configured() return False."""
        from app.integrations.google_document_ai import is_configured

        def _get(key, env_key=""):  # noqa: ARG001
            return {
                "google_ocr_project_id": "p",
                "google_ocr_location": "eu",
                "google_ocr_processor_id": "x",
                # no credentials_json key → empty string by default
            }.get(key, "")

        with patch("app.integrations.google_document_ai._get", side_effect=_get):
            assert is_configured() is True

"""Tests for ingestion.sources.edgar_source."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
import responses

from ingestion.sources import edgar_source as edgar
from ingestion.sources.edgar_source import EdgarConfig


def _config(**overrides: object) -> EdgarConfig:
    base: dict[str, object] = {
        "companies": (("AAPL", "320193"),),
        "forms": ("10-K",),
        "per_company_limit": 5,
        "sleep_seconds": 0.5,
        "connection_string": "UseDevelopmentStorage=true",
    }
    base.update(overrides)
    return EdgarConfig(**base)  # type: ignore[arg-type]


def _submissions_url(cik: str) -> str:
    padded = edgar._padded_cik(cik)
    return f"{edgar.BASE_URL}/CIK{padded}.json"


def _sample_payload() -> dict[str, object]:
    return {
        "filings": {
            "recent": {
                "form": ["10-K", "4"],
                "filingDate": ["2024-01-02", "2024-01-03"],
                "accessionNumber": ["0000320193-24-000001", "0000320193-24-000002"],
                "primaryDocument": ["aapl-10k.htm", "xslF345X5/wf-form4_123.xml"],
            }
        }
    }


@responses.activate
def test_fetch_returns_matching_filings_on_happy_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, json=_sample_payload(), status=200)

    cfg = _config()
    out = edgar.fetch(cfg)

    assert list(out.columns) == list(edgar.EDGAR_COLUMNS)
    assert len(out) == 1
    assert out.iloc[0]["ticker"] == "AAPL"
    assert out.iloc[0]["form_type"] == "10-K"


@responses.activate
def test_fetch_returns_empty_when_no_forms_match_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    payload = {
        "filings": {
            "recent": {
                "form": ["4", "4"],
                "filingDate": ["2024-01-02", "2024-01-03"],
                "accessionNumber": ["0000320193-24-000001", "0000320193-24-000002"],
                "primaryDocument": ["a.xml", "b.xml"],
            }
        }
    }
    responses.add(responses.GET, url, json=payload, status=200)

    cfg = _config(forms=("10-K",))
    out = edgar.fetch(cfg)
    assert out.empty
    assert list(out.columns) == list(edgar.EDGAR_COLUMNS)


@responses.activate
def test_fetch_raises_when_payload_not_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, json=["not", "dict"], status=200)

    cfg = _config()
    with pytest.raises(RuntimeError, match="missing filings.recent"):
        edgar.fetch(cfg)


@responses.activate
def test_fetch_raises_when_recent_filings_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, json={"filings": {}}, status=200)

    cfg = _config()
    with pytest.raises(RuntimeError, match="missing filings.recent"):
        edgar.fetch(cfg)


@responses.activate
def test_fetch_raises_when_filings_arrays_not_lists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(
        responses.GET,
        url,
        json={
            "filings": {
                "recent": {
                    "form": "10-K",
                    "filingDate": ["2024-01-01"],
                    "accessionNumber": ["0000320193-24-000001"],
                    "primaryDocument": ["x.htm"],
                }
            }
        },
        status=200,
    )

    cfg = _config()
    with pytest.raises(RuntimeError, match="malformed filings arrays"):
        edgar.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_http_403(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, body="Forbidden", status=403)

    cfg = _config()
    with pytest.raises(RuntimeError, match="EDGAR request failed"):
        edgar.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_http_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, body="Unavailable", status=503)

    cfg = _config()
    with pytest.raises(RuntimeError, match="EDGAR request failed"):
        edgar.fetch(cfg)


def test_fetch_raises_on_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)

    def raise_timeout(*_a: object, **_k: object) -> None:
        raise requests.exceptions.Timeout("timed out")

    monkeypatch.setattr(edgar.requests, "get", raise_timeout)
    cfg = _config()
    with pytest.raises(RuntimeError, match="EDGAR request failed"):
        edgar.fetch(cfg)


@responses.activate
def test_fetch_raises_on_malformed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(edgar.time, "sleep", lambda _s: None)
    url = _submissions_url("320193")
    responses.add(responses.GET, url, body="{", status=200, content_type="application/json")

    cfg = _config()
    with pytest.raises(RuntimeError, match="EDGAR request failed"):
        edgar.fetch(cfg)


def test_padded_cik_raises_when_no_digits() -> None:
    with pytest.raises(RuntimeError, match="Invalid CIK"):
        edgar._padded_cik("abc")


@patch.object(edgar.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_on_upload_failure(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = OSError("upload failed")
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "cik": ["320193"],
            "form_type": ["10-K"],
            "filed_date": ["2024-01-01"],
            "accession_number": ["0000320193-24-000001"],
            "primary_document": ["x.htm"],
        }
    )
    with pytest.raises(RuntimeError, match="Failed writing blob"):
        edgar.write_to_blob(df, run_date=date(2024, 1, 5), config=cfg)


@patch.object(edgar.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_path_on_success(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(columns=list(edgar.EDGAR_COLUMNS))
    run = date(2024, 8, 1)
    path = edgar.write_to_blob(df, run_date=run, config=cfg)
    assert path == f"{edgar.SOURCE_NAME}/{run.isoformat()}/{edgar.SOURCE_NAME}_raw.parquet"


def test_load_config_raises_when_edgar_companies_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EDGAR_COMPANIES", raising=False)
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing EDGAR_COMPANIES"):
        edgar._load_config()


def test_load_config_raises_when_edgar_companies_malformed_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="EDGAR_COMPANIES must be like"):
        edgar._load_config()


def test_load_config_raises_when_edgar_forms_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.delenv("EDGAR_FORMS", raising=False)
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing EDGAR_FORMS"):
        edgar._load_config()


def test_load_config_raises_when_edgar_limit_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.delenv("EDGAR_LIMIT", raising=False)
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing EDGAR_LIMIT"):
        edgar._load_config()


def test_load_config_raises_when_edgar_forms_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", ", , ")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least one form"):
        edgar._load_config()


def test_load_config_raises_when_edgar_limit_not_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "x")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="EDGAR_LIMIT must be an integer"):
        edgar._load_config()


def test_load_config_raises_when_edgar_sleep_seconds_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.delenv("EDGAR_SLEEP_SECONDS", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing EDGAR_SLEEP_SECONDS"):
        edgar._load_config()


def test_load_config_raises_when_edgar_sleep_seconds_below_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.1")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least 0.5"):
        edgar._load_config()


def test_load_config_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDGAR_COMPANIES", "AAPL=320193")
    monkeypatch.setenv("EDGAR_FORMS", "10-K")
    monkeypatch.setenv("EDGAR_LIMIT", "10")
    monkeypatch.setenv("EDGAR_SLEEP_SECONDS", "0.5")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        edgar._load_config()

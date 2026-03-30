"""Tests for ingestion.sources.fred_source."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
import responses

from ingestion.sources import fred_source as fred
from ingestion.sources.fred_source import FredConfig


def _config(**overrides: object) -> FredConfig:
    base: dict[str, object] = {
        "api_key": "test-key",
        "series_ids": ("UNRATE",),
        "days": 7,
        "connection_string": "UseDevelopmentStorage=true",
    }
    base.update(overrides)
    return FredConfig(**base)  # type: ignore[arg-type]


@responses.activate
def test_fetch_returns_observations_on_happy_path() -> None:
    payload = {
        "observations": [
            {"date": "2024-01-01", "value": "3.5"},
            {"date": "2024-02-01", "value": "3.6"},
        ]
    }
    responses.add(
        responses.GET,
        fred.FRED_OBSERVATIONS_URL,
        json=payload,
        status=200,
    )

    cfg = _config(series_ids=("UNRATE",))
    out = fred.fetch(cfg)

    assert list(out.columns) == list(fred.FRED_COLUMNS)
    assert len(out) == 2
    assert (out["series_id"] == "UNRATE").all()


def test_fetch_returns_empty_dataframe_when_series_ids_empty() -> None:
    cfg = _config(series_ids=())
    out = fred.fetch(cfg)
    assert out.empty
    assert list(out.columns) == list(fred.FRED_COLUMNS)


@responses.activate
def test_fetch_skips_observations_with_missing_date() -> None:
    responses.add(
        responses.GET,
        fred.FRED_OBSERVATIONS_URL,
        json={"observations": [{"value": "1"}]},
        status=200,
    )
    cfg = _config(series_ids=("X",))
    out = fred.fetch(cfg)
    assert out.empty


@responses.activate
def test_fetch_raises_runtime_error_on_http_400() -> None:
    responses.add(
        responses.GET,
        fred.FRED_OBSERVATIONS_URL,
        json={"error": "bad"},
        status=400,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="FRED request failed"):
        fred.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_http_500() -> None:
    responses.add(
        responses.GET,
        fred.FRED_OBSERVATIONS_URL,
        body="Internal Error",
        status=500,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="FRED request failed"):
        fred.fetch(cfg)


def test_fetch_raises_runtime_error_on_request_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_timeout(*_a: object, **_k: object) -> None:
        raise requests.exceptions.Timeout("timed out")

    monkeypatch.setattr(fred.requests, "get", raise_timeout)
    cfg = _config()
    with pytest.raises(RuntimeError, match="FRED request failed"):
        fred.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_malformed_json_body() -> None:
    responses.add(
        responses.GET,
        fred.FRED_OBSERVATIONS_URL,
        body="not json {",
        status=200,
        content_type="application/json",
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="FRED request failed"):
        fred.fetch(cfg)


@patch.object(fred.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_on_upload_failure(
    mock_from_cs: MagicMock,
) -> None:
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = OSError("network")

    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client

    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(
        {
            "series_id": ["UNRATE"],
            "date": ["2024-01-01"],
            "value": ["3.0"],
        }
    )
    with pytest.raises(RuntimeError, match="Failed writing blob"):
        fred.write_to_blob(df, run_date=date(2024, 1, 10), config=cfg)


@patch.object(fred.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_path_on_success(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(columns=list(fred.FRED_COLUMNS))
    run = date(2024, 5, 1)
    path = fred.write_to_blob(df, run_date=run, config=cfg)

    expected = f"{fred.SOURCE_NAME}/{run.isoformat()}/{fred.SOURCE_NAME}_raw.parquet"
    assert path == expected
    uploaded = blob_client.upload_blob.call_args[0][0]
    assert isinstance(uploaded, io.BytesIO)


def test_load_config_raises_when_fred_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FRED_KEY", raising=False)
    monkeypatch.setenv("FRED_SERIES_IDS", "GDP")
    monkeypatch.setenv("FRED_DAYS", "30")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing FRED_KEY"):
        fred._load_config()


def test_load_config_raises_when_fred_series_ids_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.delenv("FRED_SERIES_IDS", raising=False)
    monkeypatch.setenv("FRED_DAYS", "30")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing FRED_SERIES_IDS"):
        fred._load_config()


def test_load_config_raises_when_fred_series_ids_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.setenv("FRED_SERIES_IDS", " , ")
    monkeypatch.setenv("FRED_DAYS", "30")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least one series"):
        fred._load_config()


def test_load_config_raises_when_fred_days_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.setenv("FRED_SERIES_IDS", "GDP")
    monkeypatch.delenv("FRED_DAYS", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="Missing FRED_DAYS"):
        fred._load_config()


def test_load_config_raises_when_fred_days_not_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.setenv("FRED_SERIES_IDS", "GDP")
    monkeypatch.setenv("FRED_DAYS", "abc")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="FRED_DAYS must be an integer"):
        fred._load_config()


def test_load_config_raises_when_fred_days_not_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.setenv("FRED_SERIES_IDS", "GDP")
    monkeypatch.setenv("FRED_DAYS", "0")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="positive integer"):
        fred._load_config()


def test_load_config_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FRED_KEY", "k")
    monkeypatch.setenv("FRED_SERIES_IDS", "GDP")
    monkeypatch.setenv("FRED_DAYS", "30")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        fred._load_config()

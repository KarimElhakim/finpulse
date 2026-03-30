"""Tests for ingestion.sources.alphavantage_source."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
import responses

from ingestion.sources import alphavantage_source as av
from ingestion.sources.alphavantage_source import AlphaVantageConfig


def _config(**overrides: object) -> AlphaVantageConfig:
    base: dict[str, object] = {
        "api_key": "key",
        "tickers": ("IBM",),
        "sleep_seconds": 12.0,
        "connection_string": "UseDevelopmentStorage=true",
    }
    base.update(overrides)
    return AlphaVantageConfig(**base)  # type: ignore[arg-type]


def _daily_series_payload() -> dict[str, object]:
    return {
        "Meta Data": {"1. Information": "Daily"},
        "Time Series (Daily)": {
            "2024-01-02": {
                "1. open": "10.0",
                "2. high": "11.0",
                "3. low": "9.0",
                "4. close": "10.5",
                "5. volume": "100",
            }
        },
    }


@responses.activate
def test_fetch_returns_ohlcv_on_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        json=_daily_series_payload(),
        status=200,
    )
    cfg = _config()
    out = av.fetch(cfg)
    assert list(out.columns) == list(av.AV_COLUMNS)
    assert len(out) == 1
    assert out.iloc[0]["ticker"] == "IBM"
    assert out.iloc[0]["close"] == "10.5"


def test_fetch_returns_empty_when_all_tickers_blank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    cfg = _config(tickers=("  ", ""))
    out = av.fetch(cfg)
    assert out.empty
    assert list(out.columns) == list(av.AV_COLUMNS)


@responses.activate
def test_fetch_raises_when_time_series_row_not_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        json={
            "Time Series (Daily)": {
                "2024-01-02": "not-a-dict",
            }
        },
        status=200,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="malformed record"):
        av.fetch(cfg)


@responses.activate
def test_fetch_raises_when_time_series_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        json={"Meta Data": {}},
        status=200,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="missing time series"):
        av.fetch(cfg)


@responses.activate
def test_fetch_raises_on_api_error_message_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        json={"Error Message": "Invalid API call."},
        status=200,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="Alpha Vantage API error"):
        av.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_http_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        body="Not Found",
        status=404,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="Alpha Vantage request failed"):
        av.fetch(cfg)


@responses.activate
def test_fetch_raises_runtime_error_on_http_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        body="Service Unavailable",
        status=503,
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="Alpha Vantage request failed"):
        av.fetch(cfg)


def test_fetch_raises_on_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)

    def raise_timeout(*_a: object, **_k: object) -> None:
        raise requests.exceptions.ConnectTimeout("timeout")

    monkeypatch.setattr(av.requests, "get", raise_timeout)
    cfg = _config()
    with pytest.raises(RuntimeError, match="Alpha Vantage request failed"):
        av.fetch(cfg)


@responses.activate
def test_fetch_raises_on_malformed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(av.time, "sleep", lambda _s: None)
    responses.add(
        responses.GET,
        av.BASE_URL,
        body="{",
        status=200,
        content_type="application/json",
    )
    cfg = _config()
    with pytest.raises(RuntimeError, match="Alpha Vantage request failed"):
        av.fetch(cfg)


@patch.object(av.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_on_upload_failure(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = PermissionError("denied")
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(
        {
            "ticker": ["IBM"],
            "date": ["2024-01-02"],
            "open": ["1"],
            "high": ["1"],
            "low": ["1"],
            "close": ["1"],
            "volume": ["1"],
        }
    )
    with pytest.raises(RuntimeError, match="Failed writing blob"):
        av.write_to_blob(df, run_date=date(2024, 2, 1), config=cfg)


@patch.object(av.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_path_on_success(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(columns=list(av.AV_COLUMNS))
    run = date(2024, 3, 10)
    path = av.write_to_blob(df, run_date=run, config=cfg)
    assert path == f"{av.SOURCE_NAME}/{run.isoformat()}/{av.SOURCE_NAME}_raw.parquet"


def test_load_config_raises_when_alpha_vantage_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ALPHA_VANTAGE_KEY", raising=False)
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", "IBM")
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "12")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="ALPHA_VANTAGE_KEY"):
        av._load_config()


def test_load_config_raises_when_tickers_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.delenv("ALPHAVANTAGE_TICKERS", raising=False)
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "12")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="ALPHAVANTAGE_TICKERS"):
        av._load_config()


def test_load_config_raises_when_tickers_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", ",,")
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "12")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least one ticker"):
        av._load_config()


def test_load_config_raises_when_sleep_seconds_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", "IBM")
    monkeypatch.delenv("ALPHAVANTAGE_SLEEP_SECONDS", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="ALPHAVANTAGE_SLEEP_SECONDS"):
        av._load_config()


def test_load_config_raises_when_sleep_seconds_not_numeric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", "IBM")
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "x")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="must be a number"):
        av._load_config()


def test_load_config_raises_when_sleep_seconds_below_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", "IBM")
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "11")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least 12"):
        av._load_config()


def test_load_config_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALPHA_VANTAGE_KEY", "k")
    monkeypatch.setenv("ALPHAVANTAGE_TICKERS", "IBM")
    monkeypatch.setenv("ALPHAVANTAGE_SLEEP_SECONDS", "12")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        av._load_config()

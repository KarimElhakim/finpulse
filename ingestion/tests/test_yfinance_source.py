"""Tests for ingestion.sources.yfinance_source."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.sources import yfinance_source as yf_src


@pytest.fixture
def sample_multiindex_ohlcv() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=2, freq="D")
    idx.name = "Date"
    cols = pd.MultiIndex.from_product(
        [["AAPL"], ["Open", "High", "Low", "Close", "Volume"]]
    )
    data = [
        [100.0, 101.0, 99.0, 100.5, 1_000_000],
        [100.5, 102.0, 100.0, 101.0, 1_100_000],
    ]
    return pd.DataFrame(data, index=idx, columns=cols)


def test_fetch_returns_expected_ohlcv_columns_on_happy_path(
    monkeypatch: pytest.MonkeyPatch, sample_multiindex_ohlcv: pd.DataFrame
) -> None:
    def fake_download(**_kwargs: object) -> pd.DataFrame:
        return sample_multiindex_ohlcv

    monkeypatch.setattr(yf_src.yf, "download", fake_download)

    out = yf_src.fetch(["AAPL"], days=30)

    assert list(out.columns) == list(yf_src.OHLCV_COLUMNS)
    assert len(out) == 2
    assert (out["ticker"] == "AAPL").all()


def test_fetch_happy_path_single_ticker_non_multiindex_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idx = pd.date_range("2024-01-02", periods=1, freq="D")
    idx.name = "Date"
    raw = pd.DataFrame(
        {
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.0],
            "Close": [10.5],
            "Volume": [500],
        },
        index=idx,
    )

    monkeypatch.setattr(yf_src.yf, "download", lambda **_k: raw)

    out = yf_src.fetch(["ZZZ"], days=7)

    assert len(out) == 1
    assert out.iloc[0]["ticker"] == "ZZZ"
    assert out.iloc[0]["close"] == 10.5


def test_fetch_returns_empty_dataframe_when_tickers_empty() -> None:
    out = yf_src.fetch([], days=30)
    assert out.empty
    assert list(out.columns) == list(yf_src.OHLCV_COLUMNS)


def test_fetch_returns_empty_dataframe_when_tickers_only_whitespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(**_kwargs: object) -> pd.DataFrame:
        raise AssertionError("yfinance should not be called for empty ticker list")

    monkeypatch.setattr(yf_src.yf, "download", fail_if_called)

    out = yf_src.fetch(["  ", "\t", ""], days=30)
    assert out.empty


def test_fetch_raises_runtime_error_when_download_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(**_kwargs: object) -> pd.DataFrame:
        raise ConnectionError("network down")

    monkeypatch.setattr(yf_src.yf, "download", boom)

    with pytest.raises(RuntimeError, match="yfinance download failed"):
        yf_src.fetch(["AAPL"], days=1)


def test_fetch_returns_empty_when_yfinance_returns_empty_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        yf_src.yf, "download", lambda **_k: pd.DataFrame()
    )

    out = yf_src.fetch(["AAPL"], days=30)
    assert out.empty


def test_fetch_returns_empty_when_yfinance_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(yf_src.yf, "download", lambda **_k: None)

    out = yf_src.fetch(["AAPL"], days=30)
    assert out.empty


def test_fetch_raises_when_ticker_entry_is_not_string_like() -> None:
    with pytest.raises((TypeError, AttributeError)):
        yf_src.fetch([object()], days=1)  # type: ignore[arg-type]


def test_default_days_uses_thirty_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YFINANCE_DAYS", raising=False)
    assert yf_src._default_days() == 30


def test_default_days_parses_valid_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YFINANCE_DAYS", "14")
    assert yf_src._default_days() == 14


def test_default_days_raises_when_not_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YFINANCE_DAYS", "not-a-number")
    with pytest.raises(RuntimeError, match="YFINANCE_DAYS must be an integer"):
        yf_src._default_days()


def test_default_days_raises_when_not_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YFINANCE_DAYS", "0")
    with pytest.raises(RuntimeError, match="positive integer"):
        yf_src._default_days()


def test_env_tickers_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("YFINANCE_TICKERS", raising=False)
    with pytest.raises(RuntimeError, match="Missing YFINANCE_TICKERS"):
        yf_src._env_tickers()


def test_env_tickers_raises_when_empty_after_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("YFINANCE_TICKERS", ", , ")
    with pytest.raises(RuntimeError, match="at least one ticker"):
        yf_src._env_tickers()


def test_bronze_container_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FINPULSE_BRONZE_CONTAINER", raising=False)
    with pytest.raises(RuntimeError, match="Missing FINPULSE_BRONZE_CONTAINER"):
        yf_src._bronze_container()


def test_write_to_blob_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    df = yf_src._empty_ohlcv()
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        yf_src.write_to_blob(df, run_date=date(2024, 6, 1))


def test_write_to_blob_raises_when_bronze_container_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")
    monkeypatch.delenv("FINPULSE_BRONZE_CONTAINER", raising=False)
    df = yf_src._empty_ohlcv()
    with pytest.raises(RuntimeError, match="Failed writing blob") as exc_info:
        yf_src.write_to_blob(df, run_date=date(2024, 6, 1))
    assert exc_info.value.__cause__ is not None
    assert "FINPULSE_BRONZE_CONTAINER" in str(exc_info.value.__cause__)


@patch.object(yf_src.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_runtime_error_on_upload_failure(
    mock_from_cs: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "conn")
    monkeypatch.setenv("FINPULSE_BRONZE_CONTAINER", "bronze")

    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = OSError("permission denied")

    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client

    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    df = pd.DataFrame(
        {
            "ticker": ["A"],
            "date": pd.to_datetime(["2024-01-01"]),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    )

    with pytest.raises(RuntimeError, match="Failed writing blob"):
        yf_src.write_to_blob(df, run_date=date(2024, 1, 15))


@patch.object(yf_src.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_blob_path_on_success(
    mock_from_cs: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "conn")
    monkeypatch.setenv("FINPULSE_BRONZE_CONTAINER", "bronze")

    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    df = yf_src._empty_ohlcv()
    run = date(2024, 3, 20)
    path = yf_src.write_to_blob(df, run_date=run)

    expected = f"{yf_src.SOURCE_NAME}/{run.isoformat()}/{yf_src.SOURCE_NAME}_raw.parquet"
    assert path == expected
    blob_client.upload_blob.assert_called_once()
    uploaded = blob_client.upload_blob.call_args[0][0]
    assert isinstance(uploaded, io.BytesIO)

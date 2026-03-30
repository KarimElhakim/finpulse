"""Tests for ingestion.sources.newsdata_source."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
import responses

from ingestion.sources import newsdata_source as nd
from ingestion.sources.newsdata_source import NewsDataConfig


def _config() -> NewsDataConfig:
    return NewsDataConfig(
        api_key="test-key",
        connection_string="UseDevelopmentStorage=true",
    )


def _success_payload(results: list[object]) -> dict[str, object]:
    return {"status": "success", "results": results}


@responses.activate
def test_fetch_returns_rows_on_happy_path() -> None:
    results = [
        {
            "article_id": "a1",
            "title": "T",
            "description": "D",
            "source_id": "src",
            "pubDate": "2024-01-01 12:00:00",
            "link": "https://example.com",
        }
    ]
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json=_success_payload(results),
        status=200,
    )
    out = nd.fetch(_config())
    assert list(out.columns) == list(nd.NEWS_COLUMNS)
    assert len(out) == 1
    assert out.iloc[0]["article_id"] == "a1"
    assert out.iloc[0]["url"] == "https://example.com"


@responses.activate
def test_fetch_returns_empty_dataframe_when_results_empty_list() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json=_success_payload([]),
        status=200,
    )
    out = nd.fetch(_config())
    assert out.empty
    assert list(out.columns) == list(nd.NEWS_COLUMNS)


@responses.activate
def test_fetch_raises_when_results_key_missing() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json={"status": "success"},
        status=200,
    )
    with pytest.raises(RuntimeError, match="missing results"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_when_results_not_list() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json={"status": "success", "results": {}},
        status=200,
    )
    with pytest.raises(RuntimeError, match="not a list"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_when_result_item_not_object() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json={"status": "success", "results": ["x"]},
        status=200,
    )
    with pytest.raises(RuntimeError, match="malformed result"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_when_api_status_not_success() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        json={"status": "error", "message": "quota"},
        status=200,
    )
    with pytest.raises(RuntimeError, match="NewsData API error"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_runtime_error_on_http_401() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        body="Unauthorized",
        status=401,
    )
    with pytest.raises(RuntimeError, match="NewsData request failed"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_runtime_error_on_http_502() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        body="Bad Gateway",
        status=502,
    )
    with pytest.raises(RuntimeError, match="NewsData request failed"):
        nd.fetch(_config())


def test_fetch_raises_on_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_timeout(*_a: object, **_k: object) -> None:
        raise requests.exceptions.ReadTimeout("slow")

    monkeypatch.setattr(nd.requests, "get", raise_timeout)
    with pytest.raises(RuntimeError, match="NewsData request failed"):
        nd.fetch(_config())


@responses.activate
def test_fetch_raises_on_malformed_json() -> None:
    responses.add(
        responses.GET,
        nd.BASE_URL,
        body="not-json",
        status=200,
        content_type="application/json",
    )
    with pytest.raises(RuntimeError, match="NewsData request failed"):
        nd.fetch(_config())


@patch.object(nd.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_on_upload_failure(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = ConnectionError("reset")
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    df = pd.DataFrame(
        {
            "article_id": ["1"],
            "title": ["t"],
            "description": ["d"],
            "source_id": ["s"],
            "pubdate": ["2024-01-01"],
            "url": ["https://x"],
        }
    )
    with pytest.raises(RuntimeError, match="Failed writing blob"):
        nd.write_to_blob(df, run_date=date(2024, 4, 1), config=_config())


@patch.object(nd.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_path_on_success(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    df = pd.DataFrame(columns=list(nd.NEWS_COLUMNS))
    run = date(2024, 6, 15)
    path = nd.write_to_blob(df, run_date=run, config=_config())
    assert path == f"{nd.SOURCE_NAME}/{run.isoformat()}/{nd.SOURCE_NAME}_raw.parquet"
    assert isinstance(blob_client.upload_blob.call_args[0][0], io.BytesIO)


def test_load_config_raises_when_newsdata_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NEWSDATA_KEY", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="NEWSDATA_KEY"):
        nd._load_config()


def test_load_config_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NEWSDATA_KEY", "k")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        nd._load_config()

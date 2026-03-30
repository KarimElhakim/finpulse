"""Tests for ingestion.sources.rss_source."""

from __future__ import annotations

import io
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import feedparser
import pandas as pd
import pytest

from ingestion.sources import rss_source as rss
from ingestion.sources.rss_source import RssConfig


def _atom_feed_xml() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Test Channel</title>
  <entry>
    <title>Headline</title>
    <summary>Summary text</summary>
    <published>2024-01-01T12:00:00Z</published>
    <link href="https://example.com/a"/>
  </entry>
</feed>
"""


def _config(**overrides: object) -> RssConfig:
    base: dict[str, object] = {
        "feed_urls": ("https://example.com/feed.xml",),
        "connection_string": "UseDevelopmentStorage=true",
    }
    base.update(overrides)
    return RssConfig(**base)  # type: ignore[arg-type]


def test_fetch_from_urls_returns_rows_on_happy_path_parsed_feed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = feedparser.parse(_atom_feed_xml())

    monkeypatch.setattr(rss.feedparser, "parse", lambda _url: parsed)

    out = rss._fetch_from_urls(["https://example.com/feed.xml"])
    assert list(out.columns) == list(rss.RSS_COLUMNS)
    assert len(out) == 1
    assert out.iloc[0]["title"] == "Headline"
    assert "Test Channel" in str(out.iloc[0]["source"])


def test_fetch_from_urls_returns_empty_dataframe_when_no_urls() -> None:
    out = rss._fetch_from_urls([])
    assert out.empty
    assert list(out.columns) == list(rss.RSS_COLUMNS)


def test_fetch_from_urls_skips_feed_when_parse_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(_url: str) -> object:
        raise ValueError("bad xml")

    monkeypatch.setattr(rss.feedparser, "parse", boom)
    out = rss._fetch_from_urls(["https://example.com/x"])
    assert out.empty


def test_fetch_from_urls_continues_when_one_feed_has_no_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    good = feedparser.parse(_atom_feed_xml())

    def fake_parse(url: str) -> object:
        if "bad" in url:
            return SimpleNamespace(
                bozo=0,
                feed=SimpleNamespace(title="Empty"),
                entries=[],
            )
        return good

    monkeypatch.setattr(rss.feedparser, "parse", fake_parse)
    out = rss._fetch_from_urls(
        ["https://example.com/bad.xml", "https://example.com/good.xml"]
    )
    assert len(out) == 1


def test_fetch_from_urls_still_emits_rows_when_feed_marked_invalid_but_entries_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = SimpleNamespace(
        title="T",
        summary="S",
        published="P",
        link="L",
    )
    parsed = SimpleNamespace(
        bozo=1,
        feed=SimpleNamespace(title="Bozo Feed"),
        entries=[entry],
    )
    monkeypatch.setattr(rss.feedparser, "parse", lambda _u: parsed)
    out = rss._fetch_from_urls(["http://local/test"])
    assert len(out) == 1


def test_fetch_from_urls_returns_empty_when_feed_invalid_and_no_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = SimpleNamespace(
        bozo=1,
        feed=SimpleNamespace(title="Bad Feed"),
        entries=[],
    )
    monkeypatch.setattr(rss.feedparser, "parse", lambda _u: parsed)
    out = rss._fetch_from_urls(["http://local/test"])
    assert out.empty


def test_fetch_from_urls_returns_empty_when_no_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parsed = SimpleNamespace(
        bozo=0,
        feed=SimpleNamespace(title="Empty"),
        entries=[],
    )
    monkeypatch.setattr(rss.feedparser, "parse", lambda _u: parsed)
    out = rss._fetch_from_urls(["http://local/test"])
    assert out.empty


def test_fetch_uses_configured_feed_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RSS_FEED_URLS", "http://a|http://b")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")

    calls: list[str] = []

    def fake_parse(url: str) -> object:
        calls.append(url)
        return SimpleNamespace(
            bozo=0,
            feed=SimpleNamespace(title="F"),
            entries=[
                SimpleNamespace(
                    title="t", summary="s", published="p", link="l"
                )
            ],
        )

    monkeypatch.setattr(rss.feedparser, "parse", fake_parse)
    out = rss.fetch()
    assert set(calls) == {"http://a", "http://b"}
    assert len(out) == 2


@patch.object(rss.BlobServiceClient, "from_connection_string")
def test_write_to_blob_raises_on_upload_failure(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    blob_client.upload_blob.side_effect = OSError("disk")
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    cfg = _config()
    df = pd.DataFrame(
        {
            "source": ["s"],
            "title": ["t"],
            "summary": [""],
            "published": [""],
            "link": [""],
        }
    )
    with pytest.raises(RuntimeError, match="Failed writing blob"):
        rss.write_to_blob(df, run_date=date(2024, 1, 20), config=cfg)


@patch.object(rss.BlobServiceClient, "from_connection_string")
def test_write_to_blob_returns_path_on_success(mock_from_cs: MagicMock) -> None:
    blob_client = MagicMock()
    container_client = MagicMock()
    container_client.get_blob_client.return_value = blob_client
    service = MagicMock()
    service.get_container_client.return_value = container_client
    mock_from_cs.return_value = service

    df = pd.DataFrame(columns=list(rss.RSS_COLUMNS))
    run = date(2024, 7, 1)
    path = rss.write_to_blob(df, run_date=run, config=_config())
    assert path == f"{rss.SOURCE_NAME}/{run.isoformat()}/{rss.SOURCE_NAME}_raw.parquet"


def test_load_config_raises_when_rss_feed_urls_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RSS_FEED_URLS", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="RSS_FEED_URLS"):
        rss._load_config()


def test_load_config_raises_when_rss_feed_urls_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RSS_FEED_URLS", " | ")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "x")
    with pytest.raises(RuntimeError, match="at least one URL"):
        rss._load_config()


def test_load_config_raises_when_azure_connection_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RSS_FEED_URLS", "https://example.com/f")
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(RuntimeError, match="AZURE_STORAGE_CONNECTION_STRING"):
        rss._load_config()

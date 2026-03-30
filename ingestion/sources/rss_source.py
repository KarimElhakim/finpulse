"""
FinPulse RSS ingestion source.

Parses configured RSS feeds using feedparser and writes the combined raw entries
as Parquet into the Bronze zone (Azure Blob Storage).
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Final, Sequence

import feedparser
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

SOURCE_NAME: Final[str] = "rss"
BRONZE_CONTAINER: Final[str] = "finpulse-bronze"
ENV_FEED_URLS: Final[str] = "RSS_FEED_URLS"
RSS_COLUMNS: Final[tuple[str, ...]] = (
    "source",
    "title",
    "summary",
    "published",
    "link",
)

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True, slots=True)
class RssConfig:
    feed_urls: Sequence[str]
    connection_string: str


def _load_config() -> RssConfig:
    raw_urls = os.getenv(ENV_FEED_URLS, "").strip()
    if not raw_urls:
        raise RuntimeError(f"Missing {ENV_FEED_URLS} in environment/.env")

    feed_urls = [u.strip() for u in raw_urls.split("|") if u.strip()]
    if not feed_urls:
        raise RuntimeError(f"{ENV_FEED_URLS} must contain at least one URL")

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    return RssConfig(feed_urls=feed_urls, connection_string=connection_string)


def _feed_label(parsed: object, fallback_url: str) -> str:
    feed = getattr(parsed, "feed", None)
    return (
        getattr(feed, "title", None)
        or getattr(feed, "link", None)
        or fallback_url
    )


def _fetch_from_urls(feed_urls: Sequence[str]) -> pd.DataFrame:
    """
    Parse RSS feeds and return raw entries.

    Returns a DataFrame with columns:
    source, title, summary, published, link
    """
    rows: list[dict[str, object]] = []

    for feed_url in feed_urls:
        try:
            parsed = feedparser.parse(feed_url)
        except Exception as exc:
            logger.warning(
                "RSS parse failed source=%s url=%s error=%s",
                SOURCE_NAME,
                feed_url,
                exc,
            )
            continue

        if getattr(parsed, "bozo", 0):
            logger.warning(
                "RSS bozo feed source=%s url=%s",
                SOURCE_NAME,
                feed_url,
            )

        entries = getattr(parsed, "entries", None)
        if not entries:
            logger.warning(
                "RSS no entries source=%s url=%s",
                SOURCE_NAME,
                feed_url,
            )
            continue

        feed_source = _feed_label(parsed, feed_url)

        for entry in entries:
            title = getattr(entry, "title", None)
            summary = getattr(entry, "summary", None)
            published = getattr(entry, "published", None)
            link = getattr(entry, "link", None)

            rows.append(
                {
                    "source": feed_source,
                    "title": title,
                    "summary": summary,
                    "published": published,
                    "link": link,
                }
            )

    if not rows:
        return pd.DataFrame(columns=list(RSS_COLUMNS))

    return pd.DataFrame(rows, columns=list(RSS_COLUMNS))


def fetch() -> pd.DataFrame:
    """
    Parse configured RSS feeds and return raw entries.

    Returns a DataFrame with columns:
    source, title, summary, published, link
    """
    config = _load_config()
    return _fetch_from_urls(config.feed_urls)


def write_to_blob(df: pd.DataFrame, run_date: date, config: RssConfig) -> str:
    """
    Write the DataFrame to Azure Blob Storage as Parquet.

    Returns the blob path written.
    """
    folder = run_date.isoformat()
    blob_path = f"{SOURCE_NAME}/{folder}/{SOURCE_NAME}_raw.parquet"

    try:
        service = BlobServiceClient.from_connection_string(
            config.connection_string
        )
        container = service.get_container_client(BRONZE_CONTAINER)
        blob = container.get_blob_client(blob_path)

        buf = io.BytesIO()
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        blob.upload_blob(buf, overwrite=True)
    except Exception as exc:
        msg = f"Failed writing blob source={SOURCE_NAME} path={blob_path}"
        logger.exception(msg)
        raise RuntimeError(msg) from exc

    return blob_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    config = _load_config()

    per_feed_counts: dict[str, int] = {}
    all_rows: list[dict[str, object]] = []

    for feed_url in config.feed_urls:
        df_feed = _fetch_from_urls([feed_url])
        per_feed_counts[feed_url] = len(df_feed)
        all_rows.extend(df_feed.to_dict(orient="records"))

    for feed_url, count in per_feed_counts.items():
        print(f"Rows fetched ({feed_url}): {count}")
    print(f"Rows fetched (total): {len(all_rows)}")

    df_all = (
        pd.DataFrame(all_rows, columns=list(RSS_COLUMNS))
        if all_rows
        else pd.DataFrame(columns=list(RSS_COLUMNS))
    )

    written_path = write_to_blob(df_all, run_date=date.today(), config=config)
    print(f"Blob written: {BRONZE_CONTAINER}/{written_path}")

"""
FinPulse NewsData.io ingestion source.

Fetches the latest business-category headlines from NewsData.io and writes the
result as Parquet into the Bronze zone (Azure Blob Storage).
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Final

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

SOURCE_NAME: Final[str] = "newsdata"
BRONZE_CONTAINER: Final[str] = "finpulse-bronze"
BASE_URL: Final[str] = "https://newsdata.io/api/1/news"
NEWS_COLUMNS: Final[tuple[str, ...]] = (
    "article_id",
    "title",
    "description",
    "source_id",
    "pubdate",
    "url",
)

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True, slots=True)
class NewsDataConfig:
    api_key: str
    connection_string: str


def _load_config() -> NewsDataConfig:
    api_key = os.getenv("NEWSDATA_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing NEWSDATA_KEY in environment/.env")

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    return NewsDataConfig(api_key=api_key, connection_string=connection_string)


def fetch(config: NewsDataConfig) -> pd.DataFrame:
    """
    Fetch latest business-category headlines (one page).

    Returns a DataFrame with columns:
    article_id, title, description, source_id, pubdate, url
    """
    params = {
        "apikey": config.api_key,
        "language": "en",
        "category": "business",
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        msg = f"NewsData request failed source={SOURCE_NAME} endpoint={BASE_URL}"
        logger.exception(msg)
        raise RuntimeError(msg) from exc

    status = payload.get("status")
    if status and str(status).lower() != "success":
        msg = (
            f"NewsData API error source={SOURCE_NAME} endpoint={BASE_URL} "
            f"status={status} message={payload.get('message')}"
        )
        logger.error(msg)
        raise RuntimeError(msg)

    results = payload.get("results")
    if results is None:
        msg = f"NewsData missing results source={SOURCE_NAME} endpoint={BASE_URL}"
        logger.error(msg)
        raise RuntimeError(msg)

    if not isinstance(results, list):
        msg = f"NewsData results not a list source={SOURCE_NAME} endpoint={BASE_URL}"
        logger.error(msg)
        raise RuntimeError(msg)

    rows: list[dict[str, object]] = []

    for item in results:
        if not isinstance(item, dict):
            msg = (
                f"NewsData malformed result source={SOURCE_NAME} "
                f"endpoint={BASE_URL}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        rows.append(
            {
                "article_id": item.get("article_id"),
                "title": item.get("title"),
                "description": item.get("description"),
                "source_id": item.get("source_id"),
                "pubdate": item.get("pubDate"),
                "url": item.get("link"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=list(NEWS_COLUMNS))

    return pd.DataFrame(rows, columns=list(NEWS_COLUMNS))


def write_to_blob(df: pd.DataFrame, run_date: date, config: NewsDataConfig) -> str:
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
    df_raw = fetch(config)
    print(f"Rows fetched: {len(df_raw)}")

    written_path = write_to_blob(df_raw, run_date=date.today(), config=config)
    print(f"Blob written: {BRONZE_CONTAINER}/{written_path}")

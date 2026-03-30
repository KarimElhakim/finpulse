"""
FinPulse FRED ingestion source.

Fetches recent observations for selected FRED series and writes the raw result as
Parquet into the Bronze zone (Azure Blob Storage).
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date
from dataclasses import dataclass
from typing import Final, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests import Response
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

SOURCE_NAME: Final[str] = "fred"
BRONZE_CONTAINER: Final[str] = "finpulse-bronze"
FRED_OBSERVATIONS_URL: Final[str] = (
    "https://api.stlouisfed.org/fred/series/observations"
)
FRED_COLUMNS: Final[tuple[str, ...]] = ("series_id", "date", "value")

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True, slots=True)
class FredConfig:
    api_key: str
    series_ids: Sequence[str]
    days: int
    connection_string: str


def _load_config() -> FredConfig:
    api_key = os.getenv("FRED_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing FRED_KEY in environment/.env")

    raw_series = os.getenv("FRED_SERIES_IDS", "").strip()
    if not raw_series:
        raise RuntimeError("Missing FRED_SERIES_IDS in environment/.env")
    series_ids = [s.strip() for s in raw_series.split(",") if s.strip()]
    if not series_ids:
        raise RuntimeError("FRED_SERIES_IDS must contain at least one series ID")

    raw_days = os.getenv("FRED_DAYS", "").strip()
    if not raw_days:
        raise RuntimeError("Missing FRED_DAYS in environment/.env")
    try:
        days = int(raw_days)
    except ValueError:
        raise RuntimeError("FRED_DAYS must be an integer") from None
    if days <= 0:
        raise RuntimeError("FRED_DAYS must be a positive integer")

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    return FredConfig(
        api_key=api_key,
        series_ids=series_ids,
        days=days,
        connection_string=connection_string,
    )


def fetch(config: FredConfig) -> pd.DataFrame:
    """
    Fetch last `days` of observations for the configured FRED series IDs.

    Returns a DataFrame with columns: series_id, date, value
    """
    series_list = [s.strip() for s in config.series_ids if s and s.strip()]
    if not series_list:
        return pd.DataFrame(columns=list(FRED_COLUMNS))

    end_date = date.today()
    start_date = end_date.fromordinal(
        end_date.toordinal() - max(config.days, 1)
    )

    rows: list[dict[str, object]] = []

    for series_id in series_list:
        params = {
            "series_id": series_id,
            "api_key": config.api_key,
            "file_type": "json",
            "observation_start": start_date.isoformat(),
            "observation_end": end_date.isoformat(),
            "sort_order": "asc",
        }

        try:
            response: Response = requests.get(
                FRED_OBSERVATIONS_URL,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            msg = (
                f"FRED request failed source={SOURCE_NAME} "
                f"series_id={series_id} endpoint={FRED_OBSERVATIONS_URL}"
            )
            logger.exception(msg)
            raise RuntimeError(msg) from exc

        observations = payload.get("observations", [])
        if not observations:
            logger.warning(
                "No observations returned for source=%s series_id=%s",
                SOURCE_NAME,
                series_id,
            )
            continue

        for obs in observations:
            obs_date = obs.get("date")
            raw_value = obs.get("value")
            if not obs_date:
                continue

            value = raw_value

            rows.append(
                {
                    "series_id": series_id,
                    "date": obs_date,
                    "value": value,
                }
            )

    if not rows:
        return pd.DataFrame(columns=list(FRED_COLUMNS))

    return pd.DataFrame(rows, columns=list(FRED_COLUMNS))


def write_to_blob(df: pd.DataFrame, run_date: date, config: FredConfig) -> str:
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

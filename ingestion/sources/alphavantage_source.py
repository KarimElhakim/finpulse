"""
FinPulse Alpha Vantage ingestion source.

Fetches daily OHLCV market data per ticker from Alpha Vantage
(TIME_SERIES_DAILY) and writes the result as Parquet into the Bronze zone
(Azure Blob Storage).
"""

from __future__ import annotations

import io
import logging
import os
import time
from dataclasses import dataclass
from datetime import date
from typing import Final, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

SOURCE_NAME: Final[str] = "alphavantage"
BRONZE_CONTAINER: Final[str] = "finpulse-bronze"
BASE_URL: Final[str] = "https://www.alphavantage.co/query"
FUNCTION: Final[str] = "TIME_SERIES_DAILY"
SLEEP_SECONDS_BETWEEN_REQUESTS: Final[float] = 12.0
AV_COLUMNS: Final[tuple[str, ...]] = (
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True, slots=True)
class AlphaVantageConfig:
    api_key: str
    tickers: Sequence[str]
    sleep_seconds: float
    connection_string: str


def _load_config() -> AlphaVantageConfig:
    api_key = os.getenv("ALPHA_VANTAGE_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing ALPHA_VANTAGE_KEY in environment/.env")

    raw_tickers = os.getenv("ALPHAVANTAGE_TICKERS", "").strip()
    if not raw_tickers:
        raise RuntimeError("Missing ALPHAVANTAGE_TICKERS in environment/.env")
    tickers = [t.strip() for t in raw_tickers.split(",") if t.strip()]
    if not tickers:
        raise RuntimeError("ALPHAVANTAGE_TICKERS must contain at least one ticker")

    raw_sleep = os.getenv("ALPHAVANTAGE_SLEEP_SECONDS", "").strip()
    if not raw_sleep:
        raise RuntimeError(
            "Missing ALPHAVANTAGE_SLEEP_SECONDS in environment/.env"
        )
    try:
        sleep_seconds = float(raw_sleep)
    except ValueError:
        raise RuntimeError("ALPHAVANTAGE_SLEEP_SECONDS must be a number") from None
    if sleep_seconds < SLEEP_SECONDS_BETWEEN_REQUESTS:
        raise RuntimeError(
            "ALPHAVANTAGE_SLEEP_SECONDS must be at least 12 to avoid rate limiting"
        )

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    return AlphaVantageConfig(
        api_key=api_key,
        tickers=tickers,
        sleep_seconds=sleep_seconds,
        connection_string=connection_string,
    )


def _payload_error(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return "Unexpected Alpha Vantage response (not a JSON object)"

    error_message = payload.get("Error Message")
    if error_message:
        return str(error_message)

    note = payload.get("Note")
    if note:
        return str(note)

    info = payload.get("Information")
    if info:
        return str(info)

    return None


def fetch(config: AlphaVantageConfig) -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Alpha Vantage (one ticker at a time).

    Returns a DataFrame with columns:
    ticker, date, open, high, low, close, volume
    """
    rows: list[dict[str, object]] = []

    for i, ticker in enumerate(config.tickers):
        symbol = ticker.strip()
        if not symbol:
            continue

        params = {
            "function": FUNCTION,
            "symbol": symbol,
            "apikey": config.api_key,
            "datatype": "json",
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            msg = (
                f"Alpha Vantage request failed source={SOURCE_NAME} "
                f"ticker={symbol} endpoint={BASE_URL}"
            )
            logger.exception(msg)
            raise RuntimeError(msg) from exc

        error = _payload_error(payload)
        if error:
            msg = (
                f"Alpha Vantage API error source={SOURCE_NAME} "
                f"ticker={symbol} endpoint={BASE_URL} error={error}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        series = payload.get("Time Series (Daily)")
        if not isinstance(series, dict) or not series:
            msg = (
                f"Alpha Vantage missing time series source={SOURCE_NAME} "
                f"ticker={symbol} endpoint={BASE_URL}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        for day, values in series.items():
            if not isinstance(values, dict):
                msg = (
                    f"Alpha Vantage malformed record source={SOURCE_NAME} "
                    f"ticker={symbol} endpoint={BASE_URL} date={day}"
                )
                logger.error(msg)
                raise RuntimeError(msg)

            rows.append(
                {
                    "ticker": symbol,
                    "date": day,
                    "open": values.get("1. open"),
                    "high": values.get("2. high"),
                    "low": values.get("3. low"),
                    "close": values.get("4. close"),
                    "volume": values.get("5. volume"),
                }
            )

        if i < len(config.tickers) - 1 and config.sleep_seconds > 0:
            time.sleep(config.sleep_seconds)

    if not rows:
        return pd.DataFrame(columns=list(AV_COLUMNS))

    return pd.DataFrame(rows, columns=list(AV_COLUMNS))


def write_to_blob(df: pd.DataFrame, run_date: date, config: AlphaVantageConfig) -> str:
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

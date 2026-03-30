"""
FinPulse yfinance ingestion source.

Fetches daily OHLCV market data for a set of tickers from Yahoo Finance via yfinance,
and writes the result as Parquet into the Bronze zone (Azure Blob Storage).
"""

from __future__ import annotations

import io
import logging
import os
from datetime import date
from typing import Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

SOURCE_NAME = "yfinance"

logger = logging.getLogger(__name__)

load_dotenv()


def fetch(tickers: Sequence[str], days: int) -> pd.DataFrame:
    """
    Fetch daily OHLCV data for tickers for the last `days` calendar days.

    Returns a DataFrame with columns:
    ticker, date, open, high, low, close, volume
    """
    ticker_list = [t.strip() for t in tickers if t and t.strip()]
    if not ticker_list:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )

    try:
        raw = yf.download(
            tickers=ticker_list,
            period=f"{days}d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
        )
    except Exception as exc:
        msg = f"yfinance download failed for tickers={ticker_list}"
        logger.exception(msg)
        raise RuntimeError(msg) from exc

    if raw is None or raw.empty:
        logger.warning("yfinance returned no data for tickers=%s", ticker_list)
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )

    frames: list[pd.DataFrame] = []

    if isinstance(raw.columns, pd.MultiIndex):
        for ticker in ticker_list:
            if ticker not in raw.columns.get_level_values(0):
                logger.warning("No data returned for ticker=%s", ticker)
                continue

            df_t = raw[ticker].copy()
            if df_t.empty:
                logger.warning("No data returned for ticker=%s", ticker)
                continue

            df_t = df_t.reset_index()
            frames.append(
                pd.DataFrame(
                    {
                        "ticker": ticker,
                        "date": pd.to_datetime(df_t["Date"]),
                        "open": df_t.get("Open"),
                        "high": df_t.get("High"),
                        "low": df_t.get("Low"),
                        "close": df_t.get("Close"),
                        "volume": df_t.get("Volume"),
                    }
                )
            )
    else:
        df_t = raw.copy().reset_index()
        frames.append(
            pd.DataFrame(
                {
                    "ticker": ticker_list[0],
                    "date": pd.to_datetime(df_t["Date"]),
                    "open": df_t.get("Open"),
                    "high": df_t.get("High"),
                    "low": df_t.get("Low"),
                    "close": df_t.get("Close"),
                    "volume": df_t.get("Volume"),
                }
            )
        )

    if not frames:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )

    out = pd.concat(frames, ignore_index=True)
    return out[["ticker", "date", "open", "high", "low", "close", "volume"]]


def _default_days() -> int:
    raw = os.getenv("YFINANCE_DAYS", "").strip()
    if not raw:
        return 30
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError("YFINANCE_DAYS must be an integer") from None
    if value <= 0:
        raise RuntimeError("YFINANCE_DAYS must be a positive integer")
    return value


def _env_tickers() -> list[str]:
    raw = os.getenv("YFINANCE_TICKERS", "").strip()
    if not raw:
        raise RuntimeError(
            "Missing YFINANCE_TICKERS (comma-separated) in environment/.env"
        )
    tickers = [t.strip() for t in raw.split(",") if t.strip()]
    if not tickers:
        raise RuntimeError("YFINANCE_TICKERS must contain at least one ticker")
    return tickers


def _bronze_container() -> str:
    name = os.getenv("FINPULSE_BRONZE_CONTAINER", "").strip()
    if not name:
        raise RuntimeError("Missing FINPULSE_BRONZE_CONTAINER in environment/.env")
    return name


def write_to_blob(df: pd.DataFrame, run_date: date) -> str:
    """
    Write the DataFrame to Azure Blob Storage as Parquet.

    Returns the blob path written.
    """
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    folder = run_date.isoformat()
    blob_path = f"{SOURCE_NAME}/{folder}/{SOURCE_NAME}_raw.parquet"

    try:
        service = BlobServiceClient.from_connection_string(connection_string)
        container = service.get_container_client(_bronze_container())
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

    df_raw = fetch(tickers=_env_tickers(), days=_default_days())
    print(f"Rows fetched: {len(df_raw)}")

    written_path = write_to_blob(df_raw, run_date=date.today())
    print(f"Blob written: {_bronze_container()}/{written_path}")

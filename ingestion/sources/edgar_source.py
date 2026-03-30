"""
FinPulse SEC EDGAR ingestion source.

Fetches recent SEC filings from the EDGAR submissions API for configured
companies (CIKs), filters for key form types, and writes the result as Parquet
into the Bronze zone (Azure Blob Storage).
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

SOURCE_NAME: Final[str] = "edgar"
BRONZE_CONTAINER: Final[str] = "finpulse-bronze"
BASE_URL: Final[str] = "https://data.sec.gov/submissions"
USER_AGENT: Final[str] = "FinPulse-Pipeline contact@finpulse.dev"
SLEEP_SECONDS_BETWEEN_REQUESTS: Final[float] = 0.5
EDGAR_COLUMNS: Final[tuple[str, ...]] = (
    "ticker",
    "cik",
    "form_type",
    "filed_date",
    "accession_number",
    "primary_document",
)

logger = logging.getLogger(__name__)

load_dotenv()


@dataclass(frozen=True, slots=True)
class EdgarConfig:
    companies: Sequence[tuple[str, str]]
    forms: Sequence[str]
    per_company_limit: int
    sleep_seconds: float
    connection_string: str


def _load_config() -> EdgarConfig:
    raw_companies = os.getenv("EDGAR_COMPANIES", "").strip()
    if not raw_companies:
        raise RuntimeError("Missing EDGAR_COMPANIES in environment/.env")

    companies: list[tuple[str, str]] = []
    for pair in raw_companies.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            raise RuntimeError("EDGAR_COMPANIES must be like AAPL=320193,...")
        ticker, cik = pair.split("=", 1)
        ticker = ticker.strip()
        cik = cik.strip()
        if not ticker or not cik:
            raise RuntimeError("EDGAR_COMPANIES entries must be non-empty")
        companies.append((ticker, cik))

    if not companies:
        raise RuntimeError("EDGAR_COMPANIES must contain at least one company")

    raw_forms = os.getenv("EDGAR_FORMS", "").strip()
    if not raw_forms:
        raise RuntimeError("Missing EDGAR_FORMS in environment/.env")
    forms = [f.strip() for f in raw_forms.split(",") if f.strip()]
    if not forms:
        raise RuntimeError("EDGAR_FORMS must contain at least one form type")

    raw_limit = os.getenv("EDGAR_LIMIT", "").strip()
    if not raw_limit:
        raise RuntimeError("Missing EDGAR_LIMIT in environment/.env")
    try:
        limit = int(raw_limit)
    except ValueError:
        raise RuntimeError("EDGAR_LIMIT must be an integer") from None
    if limit <= 0:
        raise RuntimeError("EDGAR_LIMIT must be a positive integer")

    raw_sleep = os.getenv("EDGAR_SLEEP_SECONDS", "").strip()
    if not raw_sleep:
        raise RuntimeError("Missing EDGAR_SLEEP_SECONDS in environment/.env")
    try:
        sleep_seconds = float(raw_sleep)
    except ValueError:
        raise RuntimeError("EDGAR_SLEEP_SECONDS must be a number") from None
    if sleep_seconds < SLEEP_SECONDS_BETWEEN_REQUESTS:
        raise RuntimeError(
            "EDGAR_SLEEP_SECONDS must be at least 0.5 to respect SEC rate limits"
        )

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    connection_string = connection_string.strip()
    if not connection_string:
        raise RuntimeError(
            "Missing AZURE_STORAGE_CONNECTION_STRING in environment/.env"
        )

    return EdgarConfig(
        companies=companies,
        forms=forms,
        per_company_limit=limit,
        sleep_seconds=sleep_seconds,
        connection_string=connection_string,
    )


def _padded_cik(cik: str) -> str:
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        raise RuntimeError(f"Invalid CIK: {cik}")
    return digits.zfill(10)


def fetch(config: EdgarConfig) -> pd.DataFrame:
    """
    Fetch recent EDGAR filings for configured companies.

    Returns a DataFrame with columns:
    ticker, cik, form_type, filed_date, accession_number, primary_document
    """
    rows: list[dict[str, object]] = []

    headers = {"User-Agent": USER_AGENT}

    for idx, (ticker, cik) in enumerate(config.companies):
        padded = _padded_cik(cik)
        url = f"{BASE_URL}/CIK{padded}.json"

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            msg = (
                f"EDGAR request failed source={SOURCE_NAME} "
                f"ticker={ticker} cik={cik} endpoint={url}"
            )
            logger.exception(msg)
            raise RuntimeError(msg) from exc

        recent = (
            payload.get("filings", {}).get("recent")
            if isinstance(payload, dict)
            else None
        )
        if not isinstance(recent, dict):
            msg = (
                f"EDGAR missing filings.recent source={SOURCE_NAME} "
                f"ticker={ticker} cik={cik} endpoint={url}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        if not (
            isinstance(forms, list)
            and isinstance(dates, list)
            and isinstance(accessions, list)
            and isinstance(primary_docs, list)
        ):
            msg = (
                f"EDGAR malformed filings arrays source={SOURCE_NAME} "
                f"ticker={ticker} cik={cik} endpoint={url}"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        added = 0
        for i in range(min(len(forms), len(dates), len(accessions), len(primary_docs))):
            form_type = forms[i]
            if form_type not in config.forms:
                continue

            rows.append(
                {
                    "ticker": ticker,
                    "cik": str(cik),
                    "form_type": form_type,
                    "filed_date": dates[i],
                    "accession_number": accessions[i],
                    "primary_document": primary_docs[i],
                }
            )
            added += 1
            if added >= config.per_company_limit:
                break

        if added == 0:
            logger.warning(
                "EDGAR no matching filings source=%s ticker=%s cik=%s",
                SOURCE_NAME,
                ticker,
                cik,
            )

        if idx < len(config.companies) - 1 and config.sleep_seconds > 0:
            time.sleep(config.sleep_seconds)

    if not rows:
        return pd.DataFrame(columns=list(EDGAR_COLUMNS))

    return pd.DataFrame(rows, columns=list(EDGAR_COLUMNS))


def write_to_blob(df: pd.DataFrame, run_date: date, config: EdgarConfig) -> str:
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

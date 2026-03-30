"""
FinPulse ingestion orchestration — runs all sources in sequence.

See docs/specs/ingestion_runner.md for behavior and CLI contract.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date

from ingestion.sources import alphavantage_source as av
from ingestion.sources import edgar_source as edgar
from ingestion.sources import fred_source as fred
from ingestion.sources import newsdata_source as nd
from ingestion.sources import rss_source as rss
from ingestion.sources import yfinance_source as yf

logger = logging.getLogger(__name__)

SOURCE_ORDER: tuple[str, ...] = (
    "yfinance",
    "fred",
    "alphavantage",
    "newsdata",
    "rss",
    "edgar",
)


@dataclass(frozen=True, slots=True)
class SourceRunResult:
    name: str
    ok: bool
    rows: int | None = None
    error: str | None = None


def _run_yfinance(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "yfinance"
    try:
        logger.info("%s: starting", name)
        df = yf.fetch(yf._env_tickers(), yf._default_days())
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            path = yf.write_to_blob(df, run_date)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def _run_fred(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "fred"
    try:
        logger.info("%s: starting", name)
        cfg = fred._load_config()
        df = fred.fetch(cfg)
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            path = fred.write_to_blob(df, run_date, cfg)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def _run_alphavantage(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "alphavantage"
    try:
        logger.info("%s: starting", name)
        cfg = av._load_config()
        df = av.fetch(cfg)
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            path = av.write_to_blob(df, run_date, cfg)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def _run_newsdata(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "newsdata"
    try:
        logger.info("%s: starting", name)
        cfg = nd._load_config()
        df = nd.fetch(cfg)
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            path = nd.write_to_blob(df, run_date, cfg)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def _run_rss(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "rss"
    try:
        logger.info("%s: starting", name)
        df = rss.fetch()
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            cfg = rss._load_config()
            path = rss.write_to_blob(df, run_date, cfg)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def _run_edgar(run_date: date, dry_run: bool) -> SourceRunResult:
    name = "edgar"
    try:
        logger.info("%s: starting", name)
        cfg = edgar._load_config()
        df = edgar.fetch(cfg)
        rows = len(df)
        if dry_run:
            logger.info("%s: fetch OK, rows=%s (dry-run, skipping blob write)", name, rows)
        else:
            path = edgar.write_to_blob(df, run_date, cfg)
            logger.info("%s: fetch OK, rows=%s, blob=%s", name, rows, path)
        return SourceRunResult(name=name, ok=True, rows=rows)
    except Exception as exc:
        logger.exception("%s: failed", name)
        return SourceRunResult(name=name, ok=False, error=str(exc))


def run_all(run_date: date | None = None, *, dry_run: bool = False) -> tuple[list[SourceRunResult], float]:
    """
    Execute all ingestion sources in order.

    Returns (results in order, elapsed_seconds).
    """
    if run_date is None:
        run_date = date.today()

    runners = (
        _run_yfinance,
        _run_fred,
        _run_alphavantage,
        _run_newsdata,
        _run_rss,
        _run_edgar,
    )

    results: list[SourceRunResult] = []
    start = time.perf_counter()
    for runner in runners:
        results.append(runner(run_date, dry_run))

    assert [r.name for r in results] == list(SOURCE_ORDER)

    elapsed = time.perf_counter() - start
    return results, elapsed


def print_summary_table(results: list[SourceRunResult]) -> None:
    """Print final summary to stdout."""
    print()
    print(f"{'source':<14} {'status':<8} detail")
    print("-" * 72)
    for r in results:
        if r.ok:
            detail = f"rows={r.rows}"
            status = "OK"
        else:
            status = "FAILED"
            detail = (r.error or "unknown error").replace("\n", " ")
            if len(detail) > 64:
                detail = detail[:61] + "..."
        print(f"{r.name:<14} {status:<8} {detail}")
    print()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all FinPulse ingestion sources in sequence.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data only; do not write to Azure Blob Storage.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """
    Run all sources; return 0 if every source succeeded, else 1.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    args = parse_args(argv)
    if args.dry_run:
        logger.info("Dry-run mode: blob writes disabled")

    results, elapsed = run_all(dry_run=args.dry_run)
    logger.info("Total elapsed: %.3fs", elapsed)
    print_summary_table(results)

    if all(r.ok for r in results):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

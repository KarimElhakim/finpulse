"""Tests for ingestion.runner."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion import runner


@pytest.fixture
def empty_df() -> pd.DataFrame:
    return pd.DataFrame()


@pytest.fixture
def patch_all_sources_success(
    monkeypatch: pytest.MonkeyPatch, empty_df: pd.DataFrame
) -> None:
    monkeypatch.setattr(runner.yf, "_env_tickers", lambda: ["AAPL"])
    monkeypatch.setattr(runner.yf, "_default_days", lambda: 1)
    monkeypatch.setattr(runner.yf, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.yf, "write_to_blob", lambda *_a, **_k: "yfinance/p")

    for mod, name in (
        (runner.fred, "fred"),
        (runner.av, "alphavantage"),
        (runner.nd, "newsdata"),
        (runner.edgar, "edgar"),
    ):
        monkeypatch.setattr(mod, "_load_config", MagicMock())
        monkeypatch.setattr(mod, "fetch", lambda *_a, **_k: empty_df)
        monkeypatch.setattr(mod, "write_to_blob", lambda *_a, **_k: f"{name}/p")

    monkeypatch.setattr(runner.rss, "fetch", lambda: empty_df)
    monkeypatch.setattr(runner.rss, "_load_config", MagicMock())
    monkeypatch.setattr(runner.rss, "write_to_blob", lambda *_a, **_k: "rss/p")


def test_run_all_invokes_sources_in_order(
    monkeypatch: pytest.MonkeyPatch, empty_df: pd.DataFrame
) -> None:
    order: list[str] = []

    def track(name: str):
        def _inner(*_a: object, **_k: object) -> pd.DataFrame:
            order.append(name)
            return empty_df

        return _inner

    monkeypatch.setattr(runner.yf, "_env_tickers", lambda: ["AAPL"])
    monkeypatch.setattr(runner.yf, "_default_days", lambda: 1)
    monkeypatch.setattr(runner.yf, "fetch", track("yfinance"))
    monkeypatch.setattr(runner.yf, "write_to_blob", lambda *_a, **_k: "p")

    for mod, label in (
        (runner.fred, "fred"),
        (runner.av, "alphavantage"),
        (runner.nd, "newsdata"),
        (runner.edgar, "edgar"),
    ):
        monkeypatch.setattr(mod, "_load_config", MagicMock())
        monkeypatch.setattr(mod, "fetch", track(label))
        monkeypatch.setattr(mod, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.rss, "fetch", track("rss"))
    monkeypatch.setattr(runner.rss, "_load_config", MagicMock())
    monkeypatch.setattr(runner.rss, "write_to_blob", lambda *_a, **_k: "p")

    results, _elapsed = runner.run_all(run_date=date(2024, 1, 1), dry_run=False)

    assert order == list(runner.SOURCE_ORDER)
    assert all(r.ok for r in results) is True


def test_dry_run_skips_all_write_to_blob_calls(
    monkeypatch: pytest.MonkeyPatch, empty_df: pd.DataFrame
) -> None:
    writes: list[str] = []

    def record_write(name: str):
        def _inner(*_a: object, **_k: object) -> str:
            writes.append(name)
            return f"{name}/path"

        return _inner

    monkeypatch.setattr(runner.yf, "_env_tickers", lambda: ["AAPL"])
    monkeypatch.setattr(runner.yf, "_default_days", lambda: 1)
    monkeypatch.setattr(runner.yf, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.yf, "write_to_blob", record_write("yfinance"))

    for mod, label in (
        (runner.fred, "fred"),
        (runner.av, "alphavantage"),
        (runner.nd, "newsdata"),
        (runner.edgar, "edgar"),
    ):
        monkeypatch.setattr(mod, "_load_config", MagicMock())
        monkeypatch.setattr(mod, "fetch", lambda *_a, **_k: empty_df)
        monkeypatch.setattr(mod, "write_to_blob", record_write(label))

    monkeypatch.setattr(runner.rss, "fetch", lambda: empty_df)
    monkeypatch.setattr(runner.rss, "_load_config", MagicMock())
    monkeypatch.setattr(runner.rss, "write_to_blob", record_write("rss"))

    runner.run_all(run_date=date(2024, 1, 1), dry_run=True)

    assert writes == []


def test_failure_in_one_source_does_not_stop_others(
    monkeypatch: pytest.MonkeyPatch, empty_df: pd.DataFrame
) -> None:
    monkeypatch.setattr(runner.yf, "_env_tickers", lambda: ["AAPL"])
    monkeypatch.setattr(runner.yf, "_default_days", lambda: 1)

    def boom(*_a: object, **_k: object) -> pd.DataFrame:
        raise RuntimeError("yfinance down")

    monkeypatch.setattr(runner.yf, "fetch", boom)

    monkeypatch.setattr(runner.fred, "_load_config", MagicMock())
    monkeypatch.setattr(runner.fred, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.fred, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.av, "_load_config", MagicMock())
    monkeypatch.setattr(runner.av, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.av, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.nd, "_load_config", MagicMock())
    monkeypatch.setattr(runner.nd, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.nd, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.rss, "fetch", lambda: empty_df)
    monkeypatch.setattr(runner.rss, "_load_config", MagicMock())
    monkeypatch.setattr(runner.rss, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.edgar, "_load_config", MagicMock())
    monkeypatch.setattr(runner.edgar, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.edgar, "write_to_blob", lambda *_a, **_k: "p")

    results, _elapsed = runner.run_all(run_date=date(2024, 1, 1), dry_run=False)

    assert results[0].ok is False
    assert results[0].error is not None
    assert all(r.ok for r in results[1:])


def test_main_returns_zero_when_all_success(
    monkeypatch: pytest.MonkeyPatch, patch_all_sources_success: None
) -> None:
    assert runner.main([]) == 0


def test_main_returns_nonzero_when_any_failure(
    monkeypatch: pytest.MonkeyPatch, empty_df: pd.DataFrame
) -> None:
    monkeypatch.setattr(runner.yf, "_env_tickers", lambda: ["AAPL"])
    monkeypatch.setattr(runner.yf, "_default_days", lambda: 1)
    monkeypatch.setattr(runner.yf, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.yf, "write_to_blob", lambda *_a, **_k: "p")

    def fred_fetch_fail(**_k: object) -> pd.DataFrame:
        raise RuntimeError("fred failed")

    monkeypatch.setattr(runner.fred, "_load_config", MagicMock())
    monkeypatch.setattr(runner.fred, "fetch", fred_fetch_fail)

    monkeypatch.setattr(runner.av, "_load_config", MagicMock())
    monkeypatch.setattr(runner.av, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.av, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.nd, "_load_config", MagicMock())
    monkeypatch.setattr(runner.nd, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.nd, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.rss, "fetch", lambda: empty_df)
    monkeypatch.setattr(runner.rss, "_load_config", MagicMock())
    monkeypatch.setattr(runner.rss, "write_to_blob", lambda *_a, **_k: "p")

    monkeypatch.setattr(runner.edgar, "_load_config", MagicMock())
    monkeypatch.setattr(runner.edgar, "fetch", lambda *_a, **_k: empty_df)
    monkeypatch.setattr(runner.edgar, "write_to_blob", lambda *_a, **_k: "p")

    assert runner.main([]) == 1


def test_print_summary_table_includes_rows_and_errors(capsys: pytest.CaptureFixture[str]) -> None:
    results = [
        runner.SourceRunResult("yfinance", True, rows=10),
        runner.SourceRunResult("fred", False, error="HTTP 500"),
    ]
    runner.print_summary_table(results)
    out = capsys.readouterr().out
    assert "yfinance" in out and "OK" in out and "rows=10" in out
    assert "fred" in out and "FAILED" in out and "HTTP 500" in out

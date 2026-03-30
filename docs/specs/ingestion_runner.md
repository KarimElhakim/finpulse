# Ingestion runner specification

## Purpose

`ingestion/runner.py` is the single orchestration entry point for FinPulse’s Python ingestion pipeline. It runs six data sources in a fixed order, each producing a DataFrame (or equivalent) and optionally writing Parquet to Azure Bronze. The runner coordinates sequencing, optional dry-run mode, per-source outcomes, structured logging, a terminal summary, process exit semantics, and total elapsed time. Configuration is supplied exclusively via environment variables loaded from `.env` (dotenv is assumed to be loaded by individual source modules before config is read).

---

## CLI

### Invocation

The runner must be executable in both of these equivalent ways:

| Form | Example |
|------|---------|
| Module | `python -m ingestion.runner` |
| Script | `python ingestion/runner.py` |

Both must resolve imports and behavior identically (same `__main__` behavior or thin wrapper).

### Arguments (argparse)

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--dry-run` | flag | off | If set: execute fetch for each applicable source only; do **not** call Azure/Blob write functions. |

No other positional arguments are required for the baseline spec; `run_date` and source-specific parameters come from `.env` and source helpers (e.g. `_env_tickers()`, `_default_days()` for yfinance).

---

## Execution order

Sources run **sequentially**, **once each**, in this **exact** order:

1. **yfinance** — `ingestion/sources/yfinance_source.py`  
   - `fetch(tickers, days)` where `tickers` and `days` come from `_env_tickers()` and `_default_days()` (environment-driven).  
   - `write_to_blob(df, run_date)` — **no** config object.

2. **fred** — `ingestion/sources/fred_source.py`  
   - `config = _load_config()` → `Config`.  
   - `fetch(config)`  
   - `write_to_blob(df, run_date, config)`

3. **alphavantage** — `ingestion/sources/alphavantage_source.py`  
   - Same pattern as fred: `_load_config()`, `fetch(config)`, `write_to_blob(df, run_date, config)`.

4. **newsdata** — `ingestion/sources/newsdata_source.py`  
   - Same pattern as fred: `_load_config()`, `fetch(config)`, `write_to_blob(df, run_date, config)`.

5. **rss** — `ingestion/sources/rss_source.py`  
   - `fetch()` — **no arguments** (config loaded inside `fetch`).  
   - `config = _load_config()` → `RssConfig` for writes.  
   - `write_to_blob(df, run_date, config)`

6. **edgar** — `ingestion/sources/edgar_source.py`  
   - Same pattern as fred: `_load_config()`, `fetch(config)`, `write_to_blob(df, run_date, config)`.

The runner must not reorder sources or run any source twice in a single invocation unless explicitly extended later.

---

## Dry-run behavior

When `--dry-run` is present:

- For each source, the runner still calls the **fetch** path appropriate to that source (including internal config loading where `fetch` requires it or loads it internally, e.g. RSS).
- The runner **must not** invoke `write_to_blob` for any source.
- Logging should indicate dry-run mode at start (INFO) and, per source, that writes were skipped (INFO).

When `--dry-run` is absent:

- After a successful fetch (or the source’s defined success path), the runner calls `write_to_blob` with the signatures above, passing `run_date` and `config` as required by each source.

---

## Error handling

- Execution is **sequential** but **fault-isolated**: if one source raises an exception or returns a failure condition, the runner **must not** abort the full run.
- On failure for a source: log the exception or error at **ERROR** level (including stack trace or message as appropriate for standard logging), record the failure in that source’s result, and **continue** to the next source.
- Sources that succeed must still run and report row counts (or equivalent) independently of prior failures.

---

## Result model

Each source produces one **per-source result** object (conceptually; implementation may use a dataclass, named tuple, or dict) with:

| Field | Description |
|-------|-------------|
| Source identifier | Stable name matching the order list: `yfinance`, `fred`, `alphavantage`, `newsdata`, `rss`, `edgar`. |
| Status | `success` or `failure` (or boolean + separate error field). |
| Row count | On success: non-negative integer (rows in the DataFrame or agreed unit). On failure: `None` or omitted. |
| Error message | On failure: short human-readable message (and optionally exception type). On success: empty or omitted. |

The runner aggregates six such results (one per source), in order, for logging and the final summary table.

---

## Logging and summary table format

### Logging

- Use Python’s **`logging`** module (not `print` for operational messages).
- **INFO**: runner start, dry-run flag, per-source start, per-source success with row count, dry-run skip of writes, and final elapsed time.
- **ERROR**: per-source failures (after catch), without stopping the pipeline.

### Summary table (stdout)

At the end of the run, print a **final summary table** to stdout (plain text is sufficient; markdown-style columns optional) with one row per source and columns:

| Column | Content |
|--------|---------|
| Source | Name (`yfinance`, `fred`, …). |
| Status | `OK` or `FAILED` (or consistent uppercase labels). |
| Detail | On OK: row count (e.g. `rows=1234`). On FAILED: error message (truncated if very long, with full detail in logs). |

Example shape (illustrative):

```text
source        status   detail
yfinance      OK       rows=500
fred          FAILED   HTTP 503 ...
...
```

Also log **total elapsed wall-clock time** at INFO level at the end (e.g. seconds with millisecond precision), covering the entire run from start to after the last source finishes.

---

## Exit codes

| Code | Condition |
|------|-----------|
| **0** | All six sources completed with **success** (fetch + write path as applicable; dry-run counts success if fetch succeeded and writes were intentionally skipped). |
| **Non-zero** | **Any** source reported **failure** (exception, or explicit failure result after fetch/write). |

Document clearly in code and this spec: exit code reflects **aggregate** outcome; partial success yields non-zero if any source failed.

---

## Dependencies

- **Python**: Same runtime as the rest of FinPulse ingestion.
- **Packages**: Existing ingestion stack (e.g. pandas, Azure SDK for Blob if used inside `write_to_blob`), argparse, logging, standard library `time` (or similar) for elapsed time.
- **Configuration**: `.env` via dotenv loaded in source modules; runner does not duplicate secret loading except if a minimal `run_date` or shared helper is read from env at runner level—if so, document in implementation; default remains “config from sources’ `_load_config()` / helpers.”
- **Imports**: `ingestion.sources.{name}_source` modules and their public functions as listed in Execution order.

---

## Future extensions

Non-normative ideas that **must not** change default behavior unless specified in a later revision:

- `--only <source>` or `--skip <source>` for partial runs.
- Parallel execution with a thread/async policy (currently explicitly sequential).
- JSON or structured log output for CI.
- Prometheus/metrics hooks.
- Explicit `--run-date YYYY-MM-DD` CLI override for `run_date`.
- Retry policy per source with backoff.
- Configuration validation pass before any fetch.

---

*Version: aligned with FinPulse ingestion sources as of this document; bump version in repo when runner behavior or source contracts change.*

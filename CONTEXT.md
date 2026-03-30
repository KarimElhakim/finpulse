# FinPulse — session context

## What this project is
End-to-end financial data pipeline with medallion architecture (Bronze/Silver/Gold).
Portfolio project targeting AI Engineer and Data Engineer roles.
Owner: Karim Elhakim — Senior Software Engineer and AI Engineer, MSc AI from UWE Bristol.

## Tech stack
- Ingestion: Python (yfinance, SEC EDGAR, FRED, Alpha Vantage, NewsData.io, RSS feedparser)
- Storage: Azure Blob Storage — Bronze zone (real Azure, free tier)
- Processing: Databricks Community Edition, PySpark, Delta Lake
- AI layer: FinBERT sentiment scoring on news headlines
- Orchestration: Apache Airflow (local Docker), architected to mirror ADF
- Serving API: ASP.NET Core Web API (C#)
- Dashboard: Streamlit (deployed free on Streamlit Cloud)

## Environment variables (all in .env, never hardcoded)
- ALPHA_VANTAGE_KEY
- NEWSDATA_KEY
- FRED_KEY
- AZURE_STORAGE_CONNECTION_STRING

## Azure setup
- Storage account: finpulsestorage
- Resource group: finpulse-rg
- Container name: finpulse-bronze
- Blob path pattern: {source_name}/{YYYY-MM-DD}/{source_name}_raw.parquet

## Current sprint — Week 2
Goal: build Databricks notebooks that read Bronze raw Parquet, enforce schema, and
publish cleaned Silver Delta tables.

## Ingestion sources
- yfinance: price, volume, dividends. No API key needed.
- SEC EDGAR: 10-K 10-Q 8-K filings. No API key needed.
- FRED: macro indicators. Key = FRED_KEY.
- Alpha Vantage: stock and forex data. Key = ALPHA_VANTAGE_KEY.
- NewsData.io: financial news headlines. Key = NEWSDATA_KEY.
- RSS feeds: Yahoo Finance, Reuters, CNBC. No key. Parsed with feedparser.

## Sprint 1 checklist
- [ ] Azure Storage account created — DONE
- [ ] finpulse-bronze container created — DONE
- [ ] AZURE_STORAGE_CONNECTION_STRING added to .env — DONE
- [ ] ALPHA_VANTAGE_KEY added to .env — DONE
- [ ] NEWSDATA_KEY added to .env — DONE
- [ ] FRED_KEY added to .env — DONE
- [ ] Python venv created and all dependencies installed — DONE
- [ ] ingestion/sources/yfinance_source.py — DONE
- [ ] ingestion/sources/edgar_source.py — DONE
- [ ] ingestion/sources/fred_source.py — DONE
- [ ] ingestion/sources/alphavantage_source.py — DONE
- [ ] ingestion/sources/newsdata_source.py — DONE
- [ ] ingestion/sources/rss_source.py — DONE

## Files created so far
- Project folder structure
- .env, .gitignore, requirements.txt, README.md
- .cursor/rules/core.mdc
- .cursor/rules/ingestion.mdc
- .cursor/rules/pipeline.mdc
- .cursor/rules/subagents.mdc
- .cursor/rules/context-refresh.mdc
- .cursor/skills/finpulse-ingestion/SKILL.md

## Decisions made
- No Reddit/PRAW (skipped)
- Airflow used locally for orchestration, documented as ADF-equivalent
- .NET Core API is the serving layer — differentiator from pure Python projects
- Subagent build+verify pattern enforced via subagents.mdc
- .venv/ is project-isolated, not global

## Next step
Week 2 — Databricks Bronze to Silver notebooks.

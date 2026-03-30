# FinPulse ingestion skill

## Purpose
Writing a new ingestion source for FinPulse that reads from an external API or feed and writes raw Parquet to Azure Blob Storage.

## Steps
1. Create ingestion/sources/{source_name}.py
2. Follow the structure in ingestion.mdc exactly
3. Load credentials from .env only
4. Return a typed pandas DataFrame from the fetch function
5. Write Parquet to finpulse-bronze container using azure-storage-blob
6. Test locally with if __name__ == "__main__" before declaring done
7. Spawn a reviewer subagent to verify against ingestion.mdc standards

## References
- @ingestion.mdc for structure standards
- @core.mdc for error handling and code style
- CONTEXT.md for Azure connection string env var name

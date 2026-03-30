# Tester agent skill

## Purpose
Write comprehensive pytest test suites for FinPulse source files.

## Test file location
tests/{module_name}/test_{filename}.py

## What to cover for every file
1. Happy path — normal valid inputs produce correct outputs
2. Empty inputs — empty lists, empty DataFrames, empty strings
3. Malformed inputs — wrong types, missing keys, null values
4. API error responses — 4xx, 5xx, timeouts, malformed JSON
5. Azure Blob failures — connection errors, permission errors
6. Environment variable missing — each required env var removed one at a time

## Rules
- Use pytest only
- Use pytest.raises for expected exceptions
- Use monkeypatch or responses library to mock external HTTP calls
- Never test implementation details — test behavior and outputs
- Every test function name must describe exactly what it tests
- Run tests after writing them and fix any failures before reporting done

# Security reviewer agent skill

## Purpose
Review FinPulse code for security issues before it ships.

## Checklist for every file
1. No secrets hardcoded — API keys, connection strings, passwords
2. All secrets loaded from environment via dotenv
3. All external HTTP calls have explicit timeouts set
4. All external API responses validated before use (status code + body structure)
5. No broad bare except clauses that swallow security-relevant errors
6. No user-controlled input passed directly to file paths or system calls
7. Azure connection strings never logged even at DEBUG level

## Output format
- PASS: file is clean, one-line confirmation
- FAIL: numbered list of specific issues with line references and fix suggestions

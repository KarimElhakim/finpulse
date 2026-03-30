# Planner agent skill

## Purpose
Decompose a feature request into a clear, numbered spec before any code is written.

## Output format
Save to docs/specs/{feature_name}.md with:
1. One-sentence description of what this feature does
2. Files to create or modify (with paths)
3. Inputs and outputs for each file
4. Acceptance criteria (numbered, testable)
5. Edge cases to handle
6. What NOT to do (explicit exclusions)

## Rules
- No code in the spec. Pseudocode is fine for clarity.
- If the request is ambiguous, list assumptions explicitly.
- If the spec touches more than 5 files, flag it as too large and suggest splitting.

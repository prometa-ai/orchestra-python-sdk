# Contributing

## Branches

Do **not** push feature work directly to `main`.

1. From an up-to-date `main`, create a branch:

   ```bash
   git checkout main && git pull
   git checkout -b feat/<short-kebab-description>
   ```

   Examples: `feat/add-langgraph-span`, `feat/fix-otel-attrs`.

2. Commit on that branch and push:

   ```bash
   git push -u origin feat/<short-kebab-description>
   ```

3. Open a **pull request** into `main`.

## Merges to `main`

Merges into `main` are done **manually** by maintainers (review + merge in GitHub), not by automated promotion from agents or bots unless explicitly agreed.

## Checks

PRs run **CI** (`.github/workflows/ci.yml`). Fix **ruff** and any failures before requesting review.

## Releases

Version bumps and PyPI publishes follow `.github/workflows/publish.yml` after changes are on `main` (tag `v*` matching `pyproject.toml`).

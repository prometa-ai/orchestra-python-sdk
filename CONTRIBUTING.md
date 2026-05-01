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

## Integrations must use GitHub PR merge only

**`main` is updated only by merging a pull request in GitHub** (Merge / Squash / Rebase — whichever the repo allows). That is the single integration path.

Do **not**:

- `git push origin main` for routine changes (includes agents and local laptops).
- Land work by merging locally into `main` and pushing `main`, bypassing a PR.

Maintainers complete integration with the **Merge** (or equivalent) control on the PR. Agents and automation should push **`feat/...`** (or fix/chore branches), leave **`main`** unchanged, and let a human merge on GitHub.

Exceptions (rare, explicit break-glass): documented outage or repo rescue — call them out in the PR description if you ever must diverge.

## Checks

PRs run **CI** (`.github/workflows/ci.yml`). Fix **ruff** and any failures before requesting review.

## Releases

Version bumps and PyPI publishes follow `.github/workflows/publish.yml` after changes are on `main` (tag `v*` matching `pyproject.toml`).

---
name: setup-ci-cd
description: Create CI/CD pipelines for a dlt+Dagster workspace. Covers GitHub Actions (lint, validate, Docker publish, Dagster Cloud deploy), Dockerfile, dagster_cloud.yaml, and Dependabot. Use when setting up continuous integration or deployment for the first time or adding new workflows.
argument-hint: "[workflow-type]"
---

# Set up CI/CD for dlt+Dagster workspace

Create GitHub Actions workflows, Dockerfile, and deployment configuration for the pipeline workspace.

Parse `$ARGUMENTS`:
- `workflow-type` (optional): `ci`, `docker`, `dagster-cloud`, `all` (default: `all`)

## Prerequisites

- `pyproject.toml` exists with project dependencies
- `data_pipelines/definitions.py` exists and imports successfully
- `workspace.yaml` exists pointing to definitions

## Step 1: CI workflow (lint + validate)

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

defaults:
  run:
    working-directory: .

jobs:
  lint-and-validate:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v5
        with:
          version: "latest"

      - name: Python
        run: uv python install 3.12

      - name: Sync (dev tools + app)
        run: uv sync --group dev

      - name: Ruff
        run: uv run ruff check data_pipelines rest_api_pipeline.py

      - name: Ruff format (check only)
        run: uv run ruff format --check data_pipelines rest_api_pipeline.py

      - name: Import definitions
        run: uv run python -c "from data_pipelines.definitions import defs; assert defs is not None"

      - name: Validate Dagster definitions
        run: uv run dagster definitions validate -w workspace.yaml
```

**Key decisions:**
- `concurrency` with `cancel-in-progress` prevents queued duplicate runs
- `uv sync --group dev` includes ruff for linting
- Import test catches missing dependencies before Dagster validation
- `working-directory: .` assumes the workflow runs from the project root. If inside a monorepo, set this to the relative path.

## Step 2: Docker publish workflow

Create `.github/workflows/docker-publish.yml` for GHCR image publishing on pushes to main and version tags.

**Required:** a `Dockerfile` (see step 4).

The workflow builds and pushes to `ghcr.io/<owner>/<repo>/data-pipelines` with tags derived from the git ref.

## Step 3: Dagster Cloud deploy workflow

Create `.github/workflows/deploy-dagster-cloud.yml` using the official `dagster-io/dagster-cloud-action` for serverless deployment.

**Required secrets/variables in GitHub repo settings:**
- Secret: `DAGSTER_CLOUD_API_TOKEN`
- Variable: `DAGSTER_CLOUD_ORGANIZATION`

**Required file:** `dagster_cloud.yaml` (see step 5).

## Step 4: Dockerfile

Create `Dockerfile` at the project root:

```dockerfile
FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md workspace.yaml dagster_cloud.yaml ./
COPY data_pipelines ./data_pipelines

RUN uv sync --no-dev --no-editable

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

CMD ["python", "-c", "from data_pipelines.definitions import defs; print('definitions ok')"]
```

**Key decisions:**
- No secrets baked in; inject at runtime (Dagster Cloud env vars, K8s secrets)
- `uv sync --no-dev` excludes ruff and test dependencies
- For reproducible builds: commit `uv.lock` and use `--frozen`
- CMD is a sanity check; override in orchestrator

Create `.dockerignore`:
```
.venv/
.git/
.ruff_cache/
__pycache__/
*.secrets.toml
.dlt/secrets.toml
.env
```

## Step 5: dagster_cloud.yaml

```yaml
locations:
  - location_name: data_pipelines
    code_source:
      package_name: data_pipelines.definitions
    working_directory: ./
```

## Step 6: Dependabot

Create `.github/dependabot.yml`:
```yaml
version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
```

## Step 7: Validate locally

Run the same checks CI will run:

```bash
uv sync --group dev
uv run ruff check data_pipelines rest_api_pipeline.py
uv run ruff format --check data_pipelines rest_api_pipeline.py
uv run dagster definitions validate -w workspace.yaml
```

## Monorepo considerations

If the project lives inside a monorepo:
- Copy `.github/` into the real repo root
- Set `defaults.run.working-directory` in each workflow to the project's relative path
- Add path filters to `on.push.paths` and `on.pull_request.paths` to avoid unnecessary runs

## Next steps

- Push to GitHub and verify CI passes
- For Docker: verify `docker build -t test .` succeeds locally
- For Dagster Cloud: configure secrets in GitHub, then push to trigger deployment

# Dagster code location / dlt runtime image (Hybrid agent or custom orchestration).
# Secrets are not baked in; inject at runtime (Dagster Cloud, K8s secrets, env).
FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md workspace.yaml dagster_cloud.yaml ./
COPY data_pipelines ./data_pipelines

# Install app (no lockfile in repo by default — reproducible builds: commit uv.lock and use --frozen)
RUN uv sync --no-dev --no-editable

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Default: prove image can load definitions (override CMD in orchestrator)
CMD ["python", "-c", "from data_pipelines.definitions import defs; print('definitions ok')"]

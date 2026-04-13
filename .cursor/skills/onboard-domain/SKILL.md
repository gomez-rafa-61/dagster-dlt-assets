---
name: onboard-domain
description: Add a new data source domain to the pipeline workspace. Use when the user wants to add a new API, database, or file source as a domain folder under data_pipelines/. Covers the full cycle from template copy to Dagster wiring.
argument-hint: "[domain-name]"
---

# Onboard a new pipeline domain

Add a new data source as a domain folder under `data_pipelines/`, following the established pattern used by sharepoint, deputy, and leaftrade.

Parse `$ARGUMENTS`:
- `domain-name` (required): short snake_case name for the domain (e.g. `hubspot`, `quickbooks`)

## Prerequisites

- A working dlt source exists (built via `create-rest-api-pipeline` or manually)
- Credentials are configured in `.dlt/secrets.toml` (via `setup-secrets`)
- The pipeline has been debugged and validated (`debug-pipeline`, `validate-data`)

## Steps

### 1. Copy the template

```
cp -r data_pipelines/_template data_pipelines/<domain>/
```

Verify the new folder contains: `__init__.py`, `sources.py`, `assets.py`, `README.md`.

### 2. Implement sources.py

Replace the stub in `sources.py` with your real dlt sources/resources. Follow the established patterns:

**Module structure:**
- Config helpers reading from `dlt.config["sources.<domain>.*"]`
- Secrets via `dlt.secrets["sources.<domain>.*"]` (never read files directly)
- HTTP/fetch helpers (shared across resources in the domain)
- Resource factory functions for consistent `@dlt.resource` creation
- One `@dlt.source` per pipeline (one resource per source in this workspace)

**Resource conventions:**
- `primary_key` on every resource
- `write_disposition`: `"replace"` for dimension/full-load, `"merge"` for incremental
- Docstrings describing business meaning on every `@dlt.source`

**One source per pipeline pattern** (used by deputy, leaftrade):
```python
@dlt.source(name="<domain>_<resource>")
def <domain>_<resource>_source() -> Iterator[Any]:
    """Business description of this resource."""
    yield _resource_instance
```

### 3. Implement assets.py

Create `@dlt_assets` definitions wiring each source to Dagster. Use the `add-dagster-orchestration` skill for the full pattern, or follow this template:

```python
import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from data_pipelines.<domain>.sources import <domain>_<resource>_source
from data_pipelines.translators import TaggedDltTranslator

_translator = TaggedDltTranslator(
    source_tag="<DomainDisplayName>",
    descriptions={"<resource>": "Business description."},
)

@dlt_assets(
    dlt_source=<domain>_<resource>_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="<domain>_<resource>",
        destination="snowflake",
        dataset_name="<DOMAIN_UPPER>",
        dev_mode=False,
    ),
    name="<domain>_<resource>",
    group_name="<domain>_<resource>",
    dagster_dlt_translator=_translator,
)
def <domain>_<resource>_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)
```

**Do not** use `from __future__ import annotations` in assets files -- Dagster needs real type annotations on `context`.

### 4. Configure TOML

Add non-secret config to `.dlt/config.toml`:
```toml
[sources.<domain>]
base_url = "https://api.example.com"
snowflake_dataset_name = "<DOMAIN_UPPER>"
```

Add secrets via MCP (`secrets_update_fragment`) or `setup-secrets` skill.

### 5. Wire into definitions.py

In `data_pipelines/definitions.py`:
1. Add import for each `@dlt_assets` function from `data_pipelines.<domain>.assets`
2. Append each asset definition to the `assets=[...]` list in `Definitions`

### 6. Add jobs in jobs.py

In `data_pipelines/jobs.py`:
1. Import `AssetSelection, define_asset_job` (already imported)
2. Add one `define_asset_job` per asset group:
```python
<domain>_<resource>_job = define_asset_job(
    name="<domain>_<resource>_job",
    description="<Business description> → Snowflake.",
    selection=AssetSelection.groups("<domain>_<resource>"),
)
```
3. Append all new jobs to `ALL_JOBS`
4. Optionally add `ScheduleDefinition` entries to `OPTIONAL_SCHEDULES`

### 7. Validate

```bash
uv run ruff check data_pipelines/<domain>/
uv run python -c "from data_pipelines.definitions import defs; assert defs is not None"
uv run dagster definitions validate -w workspace.yaml
```

### 8. Update README.md

Add the new domain to the Domains table and mermaid diagram in `README.md`. Add job names to the Dagster section.

## Checklist

- [ ] `data_pipelines/<domain>/__init__.py` exists
- [ ] `data_pipelines/<domain>/sources.py` has real sources with docstrings
- [ ] `data_pipelines/<domain>/assets.py` has `@dlt_assets` definitions
- [ ] `.dlt/config.toml` has `[sources.<domain>]` section
- [ ] Secrets configured via MCP (never committed)
- [ ] `definitions.py` imports and registers all assets
- [ ] `jobs.py` has one job per asset group, added to `ALL_JOBS`
- [ ] `dagster definitions validate` passes
- [ ] `README.md` updated with new domain

## Next steps

- Run the Dagster UI (`dagster dev -w workspace.yaml`) and materialize new assets
- Use `validate-data` to verify loaded data shape
- Use `adjust-endpoint` to remove dev limits and harden for production

---
name: add-dagster-orchestration
description: Wire dlt pipelines into Dagster as software-defined assets. Use when adding @dlt_assets, TaggedDltTranslator, define_asset_job, ScheduleDefinition, and registering in Definitions.
argument-hint: "[domain-name] [resource-list]"
---

# Add Dagster orchestration for dlt pipelines

Wire existing dlt sources into Dagster as software-defined assets with jobs, tags, and optional schedules.

Parse `$ARGUMENTS`:
- `domain-name` (required): the domain folder name under `data_pipelines/`
- `resource-list` (optional): comma-separated resource names to wire; defaults to all sources in the domain

## Prerequisites

- `data_pipelines/<domain>/sources.py` exists with working `@dlt.source` functions
- `data_pipelines/translators.py` exists with `TaggedDltTranslator`
- `data_pipelines/definitions.py` and `data_pipelines/jobs.py` exist

## Steps

### 1. Read existing sources

Read `data_pipelines/<domain>/sources.py` to identify:
- All `@dlt.source` functions and their names
- Resource names, write dispositions, primary keys
- Source docstrings (reused as Dagster descriptions)

### 2. Create the translator

In `assets.py`, define a `TaggedDltTranslator` instance with:
- `source_tag`: display name for the source (e.g. `"Deputy"`, `"LeafTrade"`)
- `descriptions`: dict mapping resource names to business-level descriptions

```python
from data_pipelines.translators import TaggedDltTranslator

_DESCRIPTIONS: dict[str, str] = {
    "resource_a": "Business description of resource A.",
    "resource_b": "Business description of resource B.",
}

_translator = TaggedDltTranslator(
    source_tag="<DisplayName>",
    descriptions=_DESCRIPTIONS,
)
```

### 3. Create @dlt_assets definitions

One `@dlt_assets` function per source (one-source-per-pipeline pattern):

```python
import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

@dlt_assets(
    dlt_source=<domain>_<resource>_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="<domain>_<resource>",
        destination="snowflake",
        dataset_name=_dataset_name(),
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

**Key conventions:**
- `pipeline_name` = `<domain>_<resource>` (unique per asset group)
- `name` and `group_name` match the pipeline name
- `dataset_name` is shared across the domain (read from config with a helper)
- `dev_mode=False` for production assets
- `dagster_dlt_translator` adds tags and descriptions

**Dataset name helper** (shared across all assets in a domain):
```python
def _dataset_name() -> str:
    return str(dlt.config.get("sources.<domain>.snowflake_dataset_name") or "<DOMAIN_UPPER>")
```

**Critical:** do not use `from __future__ import annotations` in assets files. Dagster inspects the `context: AssetExecutionContext` annotation at import time.

### 4. Create jobs

In `data_pipelines/jobs.py`, add one `define_asset_job` per asset group:

```python
<domain>_<resource>_job = define_asset_job(
    name="<domain>_<resource>_job",
    description="<Business description> → Snowflake.",
    selection=AssetSelection.groups("<domain>_<resource>"),
)
```

Append each job to `ALL_JOBS`.

### 5. Add optional schedules

If the domain needs automated runs:

```python
<domain>_daily_schedule = ScheduleDefinition(
    name="<domain>_<resource>_daily",
    cron_schedule="0 6 * * *",
    job=<domain>_<resource>_job,
    execution_timezone="UTC",
)
```

Append to `OPTIONAL_SCHEDULES`. To activate, import and add to `Definitions(schedules=[...])` in `definitions.py`.

### 6. Register in definitions.py

1. Import all `@dlt_assets` functions from `data_pipelines.<domain>.assets`
2. Append to `assets=[...]` in the `Definitions(...)` call

### 7. Validate

```bash
uv run python -c "from data_pipelines.definitions import defs; assert defs is not None"
uv run dagster definitions validate -w workspace.yaml
```

Run `dagster dev -w workspace.yaml` and verify assets appear in the UI under the correct groups.

## Patterns from this workspace

**TaggedDltTranslator** (defined in `data_pipelines/translators.py`):
- Injects a `source` tag on every asset spec via `merge_attributes`
- Overrides description per resource via `replace_attributes`
- Shared across all assets in a domain (single instance)

**One pipeline per resource** vs **one pipeline per domain**:
- This workspace uses **one pipeline per resource** (each `@dlt_assets` has its own `dlt.pipeline`). This allows independent materialization and failure isolation.
- Trade-off: more jobs to manage, but each can run and retry independently.

## Next steps

- Test with `dagster dev -w workspace.yaml` and materialize individual assets
- Use `validate-data` to inspect loaded data
- Add schedules when ready for production

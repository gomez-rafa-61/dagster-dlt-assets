# Task Inventory: dlt + Dagster Pipeline Workspace

Actionable steps that built this codebase, organized by phase. Each task is concise and reproducible for future projects.

---

## Phase 0 -- Project Bootstrap

| # | Task | Details |
|---|------|---------|
| 1 | Initialize Python project with `uv` | Create `pyproject.toml` with `project.name`, `python >= 3.10`, empty `dependencies` |
| 2 | Add core dependencies | `dlt[snowflake,workspace]`, `dagster`, `dagster-webserver`, `dagster-dlt`, `requests`; dev group: `ruff` |
| 3 | Run `uv sync` | Generate virtual environment and lockfile |
| 4 | Scaffold `.dlt/` directory | Create `config.toml` (runtime settings), `secrets.toml.example` (template with placeholders), `.toolkits` marker |
| 5 | Create `.gitignore` | Exclude `.venv/`, `*.secrets.toml`, `__pycache__/`, `uv.lock` (or include lockfile for reproducibility) |
| 6 | Create `.cursorignore` | Exclude `secrets.toml`, `*.secrets.toml` from AI context |
| 7 | Set up `workspace.yaml` | Point Dagster at `data_pipelines.definitions:defs` with `working_directory: .` |
| 8 | Write initial `README.md` | Architecture mermaid diagram, setup commands, domain table, CI/CD section |

## Phase 1 -- Domain Architecture

| # | Task | Details |
|---|------|---------|
| 9 | Create `data_pipelines/` package | `__init__.py` with package docstring |
| 10 | Create `data_pipelines/definitions.py` | Empty `Definitions(assets=[], resources={"dlt": DagsterDltResource()})` as single entry point |
| 11 | Create `data_pipelines/jobs.py` | Empty `ALL_JOBS = []` list for job definitions |
| 12 | Create `data_pipelines/translators.py` | `TaggedDltTranslator(DagsterDltTranslator)` subclass adding `source` tag and per-resource descriptions to asset specs |
| 13 | Create `data_pipelines/_template/` | Copy-paste scaffold (`sources.py`, `assets.py`, `__init__.py`, `README.md`) with step-by-step onboarding instructions |

## Phase 2 -- SharePoint Domain (Microsoft Graph + CSV)

| # | Task | Details |
|---|------|---------|
| 14 | Create `data_pipelines/sharepoint/` package | `__init__.py` |
| 15 | Implement OAuth2 client-credentials token acquisition | `requests.post` to `login.microsoftonline.com`, cache token in module variable |
| 16 | Implement `sharepoint_graph_source` | dlt REST API config with `graph_site` and `sharepoint_folder_children` resources; OData `@odata.nextLink` `json_link` paginator; `$top=200`; `replace` write disposition |
| 17 | Implement CSV download resources | `sweed_product_map`, `rtl_product_mapping`, `lt_product_mapping`; download via Graph API, parse with `csv.DictReader`, yield rows; `replace` |
| 18 | Create `sharepoint/assets.py` | Four `@dlt_assets` groups (graph + 3 CSVs) with `TaggedDltTranslator(source_tag="SharePoint")` and Snowflake destination |
| 19 | Wire SharePoint into orchestration | Add assets to `definitions.py`, add four jobs + optional daily cron schedules in `jobs.py` |
| 20 | Create `rest_api_pipeline.py` | Standalone CLI entrypoint for SharePoint loads without Dagster (graph vs csv subcommand) |
| 21 | Configure `[sources.sharepoint_graph]` | In `config.toml`: `tenant_id`, `client_id`, `api_url`, `site_encoded_key`, `folder_path`, dataset names |
| 22 | Set up SharePoint secrets | `client_secret` in `secrets.toml` via MCP/CLI (never read directly) |

## Phase 3 -- MSSQL Server Domain

| # | Task | Details |
|---|------|---------|
| 23 | Create `data_pipelines/mssqlserver/` package | `__init__.py` |
| 24 | Implement `mssqlserver_source` | Wrapper around `dlt.sources.sql_database.sql_database` with `pymssql` driver; configurable `mssql_schema`, `table_names`, `defer_table_reflect` |
| 25 | Create `mssqlserver/assets.py` | Single `@dlt_assets` group targeting Snowflake |
| 26 | Write `mssqlserver/README.md` | Domain-specific setup notes (firewall, connection string, `defer_table_reflect` gotchas) |
| 27 | Comment out MSSQL in `definitions.py` | Disabled until firewall and reflection issues resolved; document the blocker |

## Phase 4 -- Deputy Domain (REST POST QUERY API)

| # | Task | Details |
|---|------|---------|
| 28 | Create `data_pipelines/deputy/` package | `__init__.py` |
| 29 | Research Deputy API | `POST /api/v1/resource/{Segment}/QUERY` with JSON body: `search` on `Modified gt cursor`, `sort` by `Modified ASC`, `start` offset pagination |
| 30 | Implement custom paginated fetcher | Bearer token auth, `page_size=500`, offset loop until empty page; yield pages as lists |
| 31 | Implement 6 incremental sources | `timesheet_incremental`, `roster_incremental`, `contact_incremental`, `timesheet_pay_return_incremental`, `employee_incremental` (404-safe), `employee_history_incremental`; `dlt.sources.incremental("Modified")`, `merge`, `primary_key="Id"` |
| 32 | Implement 10 dimension (full-load) sources | `company`, `operational_unit`, `pay_period`, `employee`, `employee_agreement`, `employment_contract`, `role`, `company_period`, `pay_rules`, `stress_profile`; `replace` |
| 33 | Create `deputy/assets.py` | 16 `@dlt_assets` groups with `TaggedDltTranslator(source_tag="Deputy")` |
| 34 | Wire 16 Deputy assets into orchestration | Add to `definitions.py`, add 16 jobs in `jobs.py` |
| 35 | Configure `[sources.deputy]` | In `config.toml`: `base_url`, `snowflake_dataset_name`, per-stream `initial_modified_*` dates |
| 36 | Set up Deputy secrets | `api_token` in `secrets.toml` |
| 37 | Write `_test_deputy_connection.py` | Ad hoc connectivity script testing auth header styles (redacted output) |

## Phase 5 -- LeafTrade Domain (REST GET API)

| # | Task | Details |
|---|------|---------|
| 38 | Create `data_pipelines/leaftrade/` package | `__init__.py` |
| 39 | Research LeafTrade API | v3/v4 endpoints at `app.leaf.trade`, `Authorization: {api_key}` header (raw, not Bearer), `page`+`page_size` GET pagination, `results` data selector |
| 40 | Implement paginated fetcher | GET with page/page_size params; stop when batch empty or shorter than page_size; 404/500 logged as empty |
| 41 | Implement 7 full-load sources | `dispensaries`, `accounts`, `strains`, `dispensary_classes`, `categories`, `stock_locations`, `vendor_users_new`; `replace` |
| 42 | Implement 5 incremental sources | `products`, `batches`, `product_variants`, `stock`, `inventory`; `primary_key="id"`, `incremental("updated_at")`, query param `updated_at_after`; `merge` |
| 43 | Create `leaftrade/assets.py` | 12 `@dlt_assets` groups with `TaggedDltTranslator(source_tag="LeafTrade")` |
| 44 | Wire 12 LeafTrade assets into orchestration | Add to `definitions.py`, add 12 jobs in `jobs.py` |
| 45 | Configure `[sources.leaftrade]` | In `config.toml`: `base_url`, `snowflake_dataset_name`, endpoint paths |
| 46 | Set up LeafTrade secrets | `api_key` in `secrets.toml` |
| 47 | Store reference Airbyte manifest | Save `leaftrade.yaml` (declarative v7.10.0) under `.raw_json_config/` for API shape documentation (not read at runtime) |

## Phase 6 -- Snowflake Destination Configuration

| # | Task | Details |
|---|------|---------|
| 48 | Configure `[destination.snowflake]` | In `config.toml`: `dataset_name` defaults per domain, Snowflake connection settings |
| 49 | Set up Snowflake secrets | Credentials in `secrets.toml`: host, account, user, password/key-pair, warehouse, role |
| 50 | Create `secrets.toml.example` | Template showing all expected secret sections with meaningful placeholders |

## Phase 7 -- CI/CD and Containerization

| # | Task | Details |
|---|------|---------|
| 51 | Create `.github/workflows/ci.yml` | On push/PR: `uv sync`, Ruff lint + format check, import `defs`, `dagster definitions validate` |
| 52 | Create `.github/workflows/docker-publish.yml` | Build Dockerfile, push to `ghcr.io` on pushes to main and `v*` tags |
| 53 | Create `.github/workflows/deploy-dagster-cloud.yml` | Dagster Cloud serverless deployment (requires `DAGSTER_CLOUD_API_TOKEN` secret) |
| 54 | Create `Dockerfile` | Python 3.12 slim, `uv sync --no-dev`, copy `data_pipelines` + configs; CMD validates definitions import |
| 55 | Create `.dockerignore` | Exclude `.venv/`, `.git/`, secrets |
| 56 | Create `dagster_cloud.yaml` | Code location metadata for Dagster Cloud |
| 57 | Create `.github/dependabot.yml` | Automated dependency updates |

## Phase 8 -- Agent Tooling (Cursor Skills and Rules)

| # | Task | Details |
|---|------|---------|
| 58 | Create `.cursor/mcp.json` | Register `dlt-workspace-mcp` server (`uv run dlt ai mcp --stdio`) |
| 59 | Create `init-dlthub-workspace.mdc` rule | Session startup checks (`uv`, venv, `dlt ai status`), communication norms, secrets handling policy, toolkit requirements |
| 60 | Create `rest-api-pipeline-workflow.mdc` rule | Ordered skill workflow for REST ingestion: `find-source` through `view-data` with handover points |
| 61 | Create `find-source` skill | Classify request, search verified sources, web search, decide REST vs handoff, present options |
| 62 | Create `create-rest-api-pipeline` skill | Scaffold via `dlt init`, research API, implement single endpoint, configure TOML, first run |
| 63 | Create `debug-pipeline` skill | Increase verbosity, run, interpret errors (stuck/0 rows/incremental), inspect traces and load packages, clean up |
| 64 | Create `validate-data` skill | Schema mermaid, data review, type fixes with `processing_steps`, nested structure handling |
| 65 | Create `view-data` skill | `dlt.attach`, `dataset()` API, ibis expressions, ReadableRelation, raw SQL, charts handoff |
| 66 | Create `adjust-endpoint` skill | Remove `.add_limit()` safely (require explicit paginator first), verify pagination, incremental loading |
| 67 | Create `new-endpoint` skill | Research endpoint, declarative vs custom `@dlt.resource`, test in isolation with `with_resources`, validate |
| 68 | Create `setup-secrets` skill | MCP-first secrets workflow, meaningful placeholders, `dlt.secrets[]` for Python access, CLI fallback |
| 69 | Create `toolkit-dispatch` skill | Route by intent to installed toolkits, install missing ones, verify MCP |
| 70 | Create `improve-skills` skill | Session-end review, map learnings to skills, propose diffs, apply |

## Phase 9 -- Reference Materials and Housekeeping

| # | Task | Details |
|---|------|---------|
| 71 | Store reference API configs | Deputy (`deputy_source.json`, `deputy_custom_source.ymal`), SQL Server (`sqlserver.json`), Snowflake destination (`dest_raw_snowflake.json`) under `.raw_json_config/` |
| 72 | Create `_tmp_jsonpath_test.py` | One-off test for JSONPath expressions (OData nextLink) |
| 73 | Update `README.md` | Keep domain table, asset names, job names, and CI/CD sections current with each new domain |

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 0 | 1--8 | Project bootstrap: `uv`, dependencies, `.dlt/`, `.gitignore`, Dagster workspace |
| 1 | 9--13 | Domain architecture: package structure, definitions, jobs, translator, template |
| 2 | 14--22 | SharePoint domain: Graph API, CSV downloads, OAuth2, Dagster assets |
| 3 | 23--27 | MSSQL domain: `sql_database` wrapper, blocked by firewall |
| 4 | 28--37 | Deputy domain: POST QUERY, 16 sources (6 incremental + 10 dimension), custom pagination |
| 5 | 38--47 | LeafTrade domain: GET pagination, 12 sources (7 full + 5 incremental) |
| 6 | 48--50 | Snowflake destination: config, secrets, example template |
| 7 | 51--57 | CI/CD: GitHub Actions, Docker, Dagster Cloud, Dependabot |
| 8 | 58--70 | Agent tooling: MCP, 2 rules, 10 skills |
| 9 | 71--73 | Reference materials: API configs, ad hoc tests, README maintenance |

**Total: 73 tasks across 10 phases (0--9)**

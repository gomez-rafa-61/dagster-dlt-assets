# MSSQL Server → Snowflake (dlt `sql_database`)

Loads tables from an Azure SQL / SQL Server schema into Snowflake using dlt’s [SQL database source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database).

## Configuration

### Source (see `.raw_json_config/sqlserver.json` for field mapping)

In **`.dlt/config.toml`**, section `[sources.mssqlserver]`:

- `host`, `port`, `database`, `username`, `mssql_schema` (e.g. `dbo`)
- Optional: `driver` (default `mssql+pymssql`)
- `defer_table_reflect` — if `true`, Dagster can load **without** opening a SQL connection (no Azure firewall rule needed at import time); you must set a non-empty `table_names` list. If `false`, dlt reflects all tables in `mssql_schema` when the code location loads (SQL Server must be reachable from that machine).
- Optional: `table_names` — TOML array; when `defer_table_reflect` is `false`, omit it to load **all** tables in `mssql_schema`
- Optional: `snowflake_dataset_name` — Snowflake **schema** name for this pipeline (dlt `dataset_name`; default `MSSQLSERVER`)

In **`.dlt/secrets.toml`** (see `.dlt/secrets.toml.example`):

- Either **`password`** under `[sources.mssqlserver]`, **or**
- A full SQLAlchemy URL in **`connection_string`** (then split host/user/password in config are not used for the URL)

You can also set `SOURCES__MSSQLSERVER__PASSWORD` (or the matching key for `connection_string`) in the environment instead of `secrets.toml`.

Never commit real passwords or keys. The JSON files under `.raw_json_config/` are reference-only.

### Destination (see `.raw_json_config/dest_raw_snowflake.json`)

Snowflake credentials and account settings live in **`.dlt/secrets.toml`** under `[destination.snowflake.credentials]` (and related keys), same as other pipelines in this workspace. Align **host/account**, **user**, **private key** (key-pair auth), **role**, **warehouse**, and **database** with your Snowflake setup.

`[destination.snowflake]` in `config.toml` already sets `enable_dataset_name_normalization = false` so schema names keep their casing.

## Dagster

- Assets are registered in `data_pipelines/definitions.py`.
- Job: `mssqlserver_job` in `data_pipelines/jobs.py` (group `mssqlserver`).

## Run locally

From the **project root**:

```bash
uv run python -c "from data_pipelines.mssqlserver.sources import load_mssqlserver; load_mssqlserver()"
```

Or materialize the `mssqlserver` asset group from the Dagster UI / CLI.

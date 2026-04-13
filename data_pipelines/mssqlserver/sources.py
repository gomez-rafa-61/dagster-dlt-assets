"""Azure SQL / Microsoft SQL Server → Snowflake via dlt ``sql_database``.

Configure non-secret fields under ``[sources.mssqlserver]`` in ``.dlt/config.toml``.
Put ``password`` or a full SQLAlchemy ``connection_string`` under ``[sources.mssqlserver]``
in ``.dlt/secrets.toml`` (see domain ``README.md``).

Run with **project root** as cwd so dlt resolves ``.dlt/``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from urllib.parse import quote_plus

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.sql_database import sql_database


def _mssql_sqlalchemy_url() -> str:
    """Build ``mssql+pymssql://...`` URL, or use a full URL from secrets."""
    explicit = dlt.secrets.get("sources.mssqlserver.connection_string")
    if explicit:
        return str(explicit).strip()

    driver = (dlt.config.get("sources.mssqlserver.driver") or "mssql+pymssql").strip()
    user = str(dlt.config["sources.mssqlserver.username"]).strip()
    password = str(dlt.secrets["sources.mssqlserver.password"]).strip()
    host = str(dlt.config["sources.mssqlserver.host"]).strip()
    port = int(dlt.config.get("sources.mssqlserver.port") or 1433)
    database = str(dlt.config["sources.mssqlserver.database"]).strip()
    return (
        f"{driver}://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}"
    )


def _table_names() -> list[str] | None:
    """If set in config, load only these tables; otherwise load all tables in the SQL schema."""
    raw: Any = dlt.config.get("sources.mssqlserver.table_names")
    if raw is None:
        return None
    if isinstance(raw, str):
        parts = [t.strip() for t in raw.split(",") if t.strip()]
        return parts or None
    if isinstance(raw, (list, tuple)):
        names = [str(t).strip() for t in raw if str(t).strip()]
        return names or None
    return None


@dlt.source(name="mssqlserver")
def mssqlserver_source() -> Iterator[DltResource]:
    """Yield one dlt resource per table from the configured SQL schema.

    When ``defer_table_reflect`` is true, Dagster can import definitions without
    connecting to SQL Server; you must set ``table_names`` to a non-empty list. Set
    ``defer_table_reflect = false`` to discover all tables at import time (requires DB access).
    """
    url = _mssql_sqlalchemy_url()
    mssql_schema = str(dlt.config["sources.mssqlserver.mssql_schema"]).strip()
    tables = _table_names()
    defer = bool(dlt.config.get("sources.mssqlserver.defer_table_reflect", False))
    if defer and not tables:
        raise ValueError(
            "sources.mssqlserver: set [sources.mssqlserver] table_names to a non-empty list "
            "when defer_table_reflect is true. "
            "Or set defer_table_reflect = false to load all tables in mssql_schema "
            "(needs SQL Server reachable when Dagster loads this module)."
        )
    yield from sql_database(
        credentials=url,
        schema=mssql_schema,
        table_names=tables,
        defer_table_reflect=defer,
    )


def load_mssqlserver() -> None:
    """Standalone entrypoint: load all configured SQL Server tables to Snowflake."""
    dataset_name = dlt.config.get("sources.mssqlserver.snowflake_dataset_name") or "MSSQLSERVER"
    pipeline = dlt.pipeline(
        pipeline_name="mssqlserver",
        destination="snowflake",
        dataset_name=dataset_name,
        dev_mode=False,
    )
    load_info = pipeline.run(mssqlserver_source())
    print(load_info)  # noqa: T201

"""Dagster ``@dlt_assets`` for MSSQL Server (Azure SQL) → Snowflake."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from data_pipelines.mssqlserver.sources import mssqlserver_source


def _mssqlserver_dataset_name() -> str:
    return dlt.config.get("sources.mssqlserver.snowflake_dataset_name") or "MSSQLSERVER"


@dlt_assets(
    dlt_source=mssqlserver_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="mssqlserver",
        destination="snowflake",
        dataset_name=_mssqlserver_dataset_name(),
        dev_mode=False,
    ),
    name="mssqlserver",
    group_name="mssqlserver",
)
def mssqlserver_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

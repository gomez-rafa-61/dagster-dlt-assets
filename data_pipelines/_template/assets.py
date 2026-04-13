"""Dagster ``@dlt_assets`` for <domain> — wire after implementing ``sources.py``."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from data_pipelines._template.sources import my_domain_source


@dlt_assets(
    dlt_source=my_domain_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="my_domain",
        destination="snowflake",
        dataset_name="my_domain_data",
        dev_mode=True,
    ),
    name="my_domain",
    group_name="my_domain",
)
def my_domain_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

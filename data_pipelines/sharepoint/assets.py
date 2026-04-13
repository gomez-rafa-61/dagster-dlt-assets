"""Dagster dlt assets for SharePoint → Snowflake (Graph + 3 CSV streams)."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from data_pipelines.sharepoint.sources import (
    lt_product_mapping_source,
    rtl_product_mapping_source,
    sharepoint_graph_source,
    sweed_product_map_source,
)
from data_pipelines.translators import TaggedDltTranslator

_SHAREPOINT_TAG = "SharePoint"

_SHAREPOINT_DESCRIPTIONS: dict[str, str] = {
    "graph_site": (
        "SharePoint site metadata retrieved via Microsoft Graph API."
    ),
    "sharepoint_folder_children": (
        "File listing of the configured SharePoint document-library folder via Microsoft Graph."
    ),
    "sweed_product_map": (
        "Sweed POS product-map CSV downloaded from SharePoint. Full replace on each run."
    ),
    "RTL_Product_Mapping": (
        "RTL product-mapping CSV downloaded from SharePoint. Full replace on each run."
    ),
    "LT_Product_Mapping": (
        "LeafTrade product-mapping CSV downloaded from SharePoint. Full replace on each run."
    ),
}

_sp_translator = TaggedDltTranslator(
    source_tag=_SHAREPOINT_TAG,
    descriptions=_SHAREPOINT_DESCRIPTIONS,
)


def _sharepoint_graph_dataset_name() -> str:
    return (
        dlt.config.get("sources.sharepoint_graph.sharepoint_graph_rest_dataset_name")
        or "sharepoint_graph_data"
    )


def _sharepoint_csv_dataset_name() -> str:
    return dlt.config.get("sources.sharepoint_graph.snowflake_dataset_name") or "SHAREPOINT"


@dlt_assets(
    dlt_source=sharepoint_graph_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="sharepoint_graph",
        destination="snowflake",
        dataset_name=_sharepoint_graph_dataset_name(),
        dev_mode=False,
    ),
    name="sharepoint_graph",
    group_name="sharepoint_graph",
    dagster_dlt_translator=_sp_translator,
)
def sharepoint_graph_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=sweed_product_map_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="sharepoint_sweed_product_map",
        destination="snowflake",
        dataset_name=_sharepoint_csv_dataset_name(),
        dev_mode=False,
    ),
    name="sharepoint_sweed_product_map",
    group_name="sharepoint_sweed_product_map",
    dagster_dlt_translator=_sp_translator,
)
def sharepoint_sweed_product_map_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=rtl_product_mapping_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="sharepoint_rtl_product_mapping",
        destination="snowflake",
        dataset_name=_sharepoint_csv_dataset_name(),
        dev_mode=False,
    ),
    name="sharepoint_rtl_product_mapping",
    group_name="sharepoint_rtl_product_mapping",
    dagster_dlt_translator=_sp_translator,
)
def sharepoint_rtl_product_mapping_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=lt_product_mapping_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="sharepoint_lt_product_mapping",
        destination="snowflake",
        dataset_name=_sharepoint_csv_dataset_name(),
        dev_mode=False,
    ),
    name="sharepoint_lt_product_mapping",
    group_name="sharepoint_lt_product_mapping",
    dagster_dlt_translator=_sp_translator,
)
def sharepoint_lt_product_mapping_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

"""Dagster ``@dlt_assets`` for Leaftrade → Snowflake (twelve pipelines)."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from data_pipelines.leaftrade.sources import (
    leaftrade_accounts_source,
    leaftrade_batches_source,
    leaftrade_categories_source,
    leaftrade_dispensaries_source,
    leaftrade_dispensary_classes_source,
    leaftrade_inventory_source,
    leaftrade_product_variants_source,
    leaftrade_products_source,
    leaftrade_stock_locations_source,
    leaftrade_stock_source,
    leaftrade_strains_source,
    leaftrade_vendor_users_new_source,
)
from data_pipelines.translators import TaggedDltTranslator

_LEAFTRADE_DESCRIPTIONS: dict[str, str] = {
    "dispensaries": (
        "Dispensary directory: retail locations that purchase from the vendor."
        " Full replace on each run."
    ),
    "accounts": (
        "Vendor account records: customer accounts and business relationships."
        " Full replace on each run."
    ),
    "strains": (
        "Cannabis strain catalog: strain names, types, and genetics."
        " Full replace on each run."
    ),
    "dispensary_classes": (
        "Dispensary classification dimension: tiers or categories assigned to"
        " dispensary accounts. Full replace on each run."
    ),
    "categories": (
        "Product category taxonomy: top-level groupings for the product catalog."
        " Full replace on each run."
    ),
    "stock_locations": (
        "Warehouse / stock-location dimension: physical storage sites for inventory."
        " Full replace on each run."
    ),
    "vendor_users_new": (
        "Vendor portal user accounts: team members with access to the LeafTrade platform."
        " Full replace on each run."
    ),
    "products": (
        "Product master records: SKU details, pricing, and category assignments."
        " Incremental merge on updated_at."
    ),
    "batches": (
        "Production batches: batch numbers, test results, and harvest metadata."
        " Incremental merge on updated_at."
    ),
    "product_variants": (
        "Product variant records: weight/size variants linked to parent products."
        " Incremental merge on updated_at."
    ),
    "stock": (
        "Current stock quantities per product variant and location."
        " Incremental merge on updated_at."
    ),
    "inventory": (
        "Inventory position snapshots: on-hand, committed, and available quantities."
        " Incremental merge on updated_at."
    ),
}

_leaftrade_translator = TaggedDltTranslator(
    source_tag="LeafTrade",
    descriptions=_LEAFTRADE_DESCRIPTIONS,
)


def _leaftrade_dataset_name() -> str:
    return str(dlt.config.get("sources.leaftrade.snowflake_dataset_name") or "LEAFTRADE")


# ---------------------------------------------------------------------------
# Full-load assets (7)
# ---------------------------------------------------------------------------

@dlt_assets(
    dlt_source=leaftrade_dispensaries_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_dispensaries",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_dispensaries",
    group_name="leaftrade_dispensaries",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_dispensaries_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_accounts_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_accounts",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_accounts",
    group_name="leaftrade_accounts",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_accounts_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_strains_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_strains",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_strains",
    group_name="leaftrade_strains",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_strains_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_dispensary_classes_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_dispensary_classes",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_dispensary_classes",
    group_name="leaftrade_dispensary_classes",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_dispensary_classes_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_categories_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_categories",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_categories",
    group_name="leaftrade_categories",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_categories_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_stock_locations_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_stock_locations",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_stock_locations",
    group_name="leaftrade_stock_locations",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_stock_locations_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_vendor_users_new_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_vendor_users_new",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_vendor_users_new",
    group_name="leaftrade_vendor_users_new",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_vendor_users_new_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


# ---------------------------------------------------------------------------
# Incremental assets (5)
# ---------------------------------------------------------------------------

@dlt_assets(
    dlt_source=leaftrade_products_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_products",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_products",
    group_name="leaftrade_products",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_products_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_batches_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_batches",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_batches",
    group_name="leaftrade_batches",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_batches_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_product_variants_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_product_variants",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_product_variants",
    group_name="leaftrade_product_variants",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_product_variants_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_stock_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_stock",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_stock",
    group_name="leaftrade_stock",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_stock_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=leaftrade_inventory_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="leaftrade_inventory",
        destination="snowflake",
        dataset_name=_leaftrade_dataset_name(),
        dev_mode=False,
    ),
    name="leaftrade_inventory",
    group_name="leaftrade_inventory",
    dagster_dlt_translator=_leaftrade_translator,
)
def leaftrade_inventory_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

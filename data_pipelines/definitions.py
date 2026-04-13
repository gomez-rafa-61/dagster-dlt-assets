"""Root Dagster Definitions: all dlt asset groups, shared Dlt resource, jobs."""

from __future__ import annotations

from dagster import Definitions
from dagster_dlt import DagsterDltResource

from data_pipelines.deputy.assets import (
    deputy_company_assets,
    deputy_company_period_assets,
    deputy_contact_incremental_assets,
    deputy_employee_agreement_assets,
    deputy_employee_assets,
    deputy_employee_history_incremental_assets,
    deputy_employee_incremental_assets,
    deputy_employment_contract_assets,
    deputy_operational_unit_assets,
    deputy_pay_period_assets,
    deputy_pay_rules_assets,
    deputy_role_assets,
    deputy_roster_incremental_assets,
    deputy_stress_profile_assets,
    deputy_timesheet_incremental_assets,
    deputy_timesheet_pay_return_incremental_assets,
)
from data_pipelines.jobs import ALL_JOBS
from data_pipelines.leaftrade.assets import (
    leaftrade_accounts_assets,
    leaftrade_batches_assets,
    leaftrade_categories_assets,
    leaftrade_dispensaries_assets,
    leaftrade_dispensary_classes_assets,
    leaftrade_inventory_assets,
    leaftrade_product_variants_assets,
    leaftrade_products_assets,
    leaftrade_stock_assets,
    leaftrade_stock_locations_assets,
    leaftrade_strains_assets,
    leaftrade_vendor_users_new_assets,
)

# from data_pipelines.mssqlserver.assets import mssqlserver_assets  # disabled — firewall blocks local IP
from data_pipelines.sharepoint.assets import (
    sharepoint_graph_assets,
    sharepoint_lt_product_mapping_assets,
    sharepoint_rtl_product_mapping_assets,
    sharepoint_sweed_product_map_assets,
)

defs = Definitions(
    assets=[
        sharepoint_graph_assets,
        sharepoint_sweed_product_map_assets,
        sharepoint_rtl_product_mapping_assets,
        sharepoint_lt_product_mapping_assets,
        # mssqlserver_assets,  # disabled — re-enable after firewall/defer_table_reflect is configured
        deputy_timesheet_incremental_assets,
        deputy_roster_incremental_assets,
        deputy_contact_incremental_assets,
        deputy_timesheet_pay_return_incremental_assets,
        deputy_employee_incremental_assets,
        deputy_employee_history_incremental_assets,
        deputy_company_assets,
        deputy_operational_unit_assets,
        deputy_pay_period_assets,
        deputy_employee_assets,
        deputy_employee_agreement_assets,
        deputy_employment_contract_assets,
        deputy_role_assets,
        deputy_company_period_assets,
        deputy_pay_rules_assets,
        deputy_stress_profile_assets,
        leaftrade_dispensaries_assets,
        leaftrade_accounts_assets,
        leaftrade_strains_assets,
        leaftrade_dispensary_classes_assets,
        leaftrade_categories_assets,
        leaftrade_stock_locations_assets,
        leaftrade_vendor_users_new_assets,
        leaftrade_products_assets,
        leaftrade_batches_assets,
        leaftrade_product_variants_assets,
        leaftrade_stock_assets,
        leaftrade_inventory_assets,
    ],
    resources={"dlt": DagsterDltResource()},
    jobs=ALL_JOBS,
    # To enable cron schedules, add: schedules=OPTIONAL_SCHEDULES
    # from data_pipelines.jobs import OPTIONAL_SCHEDULES
)

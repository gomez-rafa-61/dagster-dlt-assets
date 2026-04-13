"""Jobs and optional schedules for pipeline domains."""

from __future__ import annotations

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

sharepoint_graph_job = define_asset_job(
    name="sharepoint_graph_job",
    description=(
        "Materialize SharePoint Graph folder listing (graph_site, sharepoint_folder_children)."
    ),
    selection=AssetSelection.groups("sharepoint_graph"),
)

sharepoint_sweed_product_map_job = define_asset_job(
    name="sharepoint_sweed_product_map_job",
    description="Materialize Sweed POS product-map CSV from SharePoint → Snowflake.",
    selection=AssetSelection.groups("sharepoint_sweed_product_map"),
)

sharepoint_rtl_product_mapping_job = define_asset_job(
    name="sharepoint_rtl_product_mapping_job",
    description="Materialize RTL product-mapping CSV from SharePoint → Snowflake.",
    selection=AssetSelection.groups("sharepoint_rtl_product_mapping"),
)

sharepoint_lt_product_mapping_job = define_asset_job(
    name="sharepoint_lt_product_mapping_job",
    description="Materialize LeafTrade product-mapping CSV from SharePoint → Snowflake.",
    selection=AssetSelection.groups("sharepoint_lt_product_mapping"),
)

# mssqlserver_job disabled — re-enable after firewall/defer_table_reflect is configured
# mssqlserver_job = define_asset_job(
#     name="mssqlserver_job",
#     description="Materialize MSSQL Server / Azure SQL tables (sql_database → Snowflake).",
#     selection=AssetSelection.groups("mssqlserver"),
# )

deputy_timesheet_incremental_job = define_asset_job(
    name="deputy_timesheet_incremental_job",
    description="Clock-in/out timesheet entries: hours worked, breaks, costs, payroll status. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_timesheet_incremental"),
)
deputy_roster_incremental_job = define_asset_job(
    name="deputy_roster_incremental_job",
    description="Scheduled shifts and roster assignments: start/end times, employee allocation, publish status. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_roster_incremental"),
)
deputy_contact_incremental_job = define_asset_job(
    name="deputy_contact_incremental_job",
    description="Contact details for employees and Deputy users: addresses, phone numbers, emergency contacts. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_contact_incremental"),
)
deputy_timesheet_pay_return_incremental_job = define_asset_job(
    name="deputy_timesheet_pay_return_incremental_job",
    description="Payroll return line items from approved timesheets, linking hours to export categories. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_timesheet_pay_return_incremental"),
)
deputy_employee_incremental_job = define_asset_job(
    name="deputy_employee_incremental_job",
    description="Employee master records: personal details, active status, role, company, hire metadata. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_employee_incremental"),
)
deputy_employee_history_incremental_job = define_asset_job(
    name="deputy_employee_history_incremental_job",
    description="Historical change log of employee record modifications capturing prior field values. Incremental merge on Modified.",
    selection=AssetSelection.groups("deputy_employee_history_incremental"),
)
deputy_company_job = define_asset_job(
    name="deputy_company_job",
    description="Company/location dimension: physical business sites and organizational units in Deputy.",
    selection=AssetSelection.groups("deputy_company"),
)
deputy_operational_unit_job = define_asset_job(
    name="deputy_operational_unit_job",
    description="Operational unit (area/department) dimension: work areas within locations used for scheduling and reporting.",
    selection=AssetSelection.groups("deputy_operational_unit"),
)
deputy_pay_period_job = define_asset_job(
    name="deputy_pay_period_job",
    description="Pay period dimension: payroll cycle boundaries with start/end dates and period type.",
    selection=AssetSelection.groups("deputy_pay_period"),
)
deputy_employee_job = define_asset_job(
    name="deputy_employee_job",
    description="Full-load employee dimension: complete employee records for reference and lookup use.",
    selection=AssetSelection.groups("deputy_employee"),
)
deputy_employee_agreement_job = define_asset_job(
    name="deputy_employee_agreement_job",
    description="Employee agreement records linking employees to contracts, pay points, and payroll configurations.",
    selection=AssetSelection.groups("deputy_employee_agreement"),
)
deputy_employment_contract_job = define_asset_job(
    name="deputy_employment_contract_job",
    description="Employment contract templates: period types, base pay rules, employment conditions, and award structures.",
    selection=AssetSelection.groups("deputy_employment_contract"),
)
deputy_role_job = define_asset_job(
    name="deputy_role_job",
    description="Employee role/position dimension: job titles and positions assignable to employees.",
    selection=AssetSelection.groups("deputy_role"),
)
deputy_company_period_job = define_asset_job(
    name="deputy_company_period_job",
    description="Company period dimension: maps companies to pay periods, defining which pay schedule applies per location.",
    selection=AssetSelection.groups("deputy_company_period"),
)
deputy_pay_rules_job = define_asset_job(
    name="deputy_pay_rules_job",
    description="Pay rule definitions: hourly rates, multipliers, salary settings, and remuneration rules for payroll calculations.",
    selection=AssetSelection.groups("deputy_pay_rules"),
)
deputy_stress_profile_job = define_asset_job(
    name="deputy_stress_profile_job",
    description="Stress profile records: workforce demand modeling and staffing requirement parameters for scheduling.",
    selection=AssetSelection.groups("deputy_stress_profile"),
)

# --- Leaftrade full-load jobs (7) ---
leaftrade_dispensaries_job = define_asset_job(
    name="leaftrade_dispensaries_job",
    description="Leaftrade dispensaries (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_dispensaries"),
)
leaftrade_accounts_job = define_asset_job(
    name="leaftrade_accounts_job",
    description="Leaftrade accounts (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_accounts"),
)
leaftrade_strains_job = define_asset_job(
    name="leaftrade_strains_job",
    description="Leaftrade strains (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_strains"),
)
leaftrade_dispensary_classes_job = define_asset_job(
    name="leaftrade_dispensary_classes_job",
    description="Leaftrade dispensary_classes (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_dispensary_classes"),
)
leaftrade_categories_job = define_asset_job(
    name="leaftrade_categories_job",
    description="Leaftrade categories (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_categories"),
)
leaftrade_stock_locations_job = define_asset_job(
    name="leaftrade_stock_locations_job",
    description="Leaftrade stock_locations (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_stock_locations"),
)
leaftrade_vendor_users_new_job = define_asset_job(
    name="leaftrade_vendor_users_new_job",
    description="Leaftrade vendor_users_new (full load) → Snowflake.",
    selection=AssetSelection.groups("leaftrade_vendor_users_new"),
)

# --- Leaftrade incremental jobs (5) ---
leaftrade_products_job = define_asset_job(
    name="leaftrade_products_job",
    description="Leaftrade products incremental → Snowflake.",
    selection=AssetSelection.groups("leaftrade_products"),
)
leaftrade_batches_job = define_asset_job(
    name="leaftrade_batches_job",
    description="Leaftrade batches incremental → Snowflake.",
    selection=AssetSelection.groups("leaftrade_batches"),
)
leaftrade_product_variants_job = define_asset_job(
    name="leaftrade_product_variants_job",
    description="Leaftrade product_variants incremental → Snowflake.",
    selection=AssetSelection.groups("leaftrade_product_variants"),
)
leaftrade_stock_job = define_asset_job(
    name="leaftrade_stock_job",
    description="Leaftrade stock incremental → Snowflake.",
    selection=AssetSelection.groups("leaftrade_stock"),
)
leaftrade_inventory_job = define_asset_job(
    name="leaftrade_inventory_job",
    description="Leaftrade inventory incremental → Snowflake.",
    selection=AssetSelection.groups("leaftrade_inventory"),
)

# Optional: attach to Definitions(schedules=[...]) in definitions.py to enable automatic runs.
sharepoint_graph_daily_schedule = ScheduleDefinition(
    name="sharepoint_graph_daily",
    cron_schedule="0 6 * * *",
    job=sharepoint_graph_job,
    execution_timezone="UTC",
)

sharepoint_sweed_product_map_daily_schedule = ScheduleDefinition(
    name="sharepoint_sweed_product_map_daily",
    cron_schedule="0 7 * * *",
    job=sharepoint_sweed_product_map_job,
    execution_timezone="UTC",
)

sharepoint_rtl_product_mapping_daily_schedule = ScheduleDefinition(
    name="sharepoint_rtl_product_mapping_daily",
    cron_schedule="0 7 * * *",
    job=sharepoint_rtl_product_mapping_job,
    execution_timezone="UTC",
)

sharepoint_lt_product_mapping_daily_schedule = ScheduleDefinition(
    name="sharepoint_lt_product_mapping_daily",
    cron_schedule="0 7 * * *",
    job=sharepoint_lt_product_mapping_job,
    execution_timezone="UTC",
)

ALL_JOBS = [
    sharepoint_graph_job,
    sharepoint_sweed_product_map_job,
    sharepoint_rtl_product_mapping_job,
    sharepoint_lt_product_mapping_job,
    # mssqlserver_job,  # disabled
    deputy_timesheet_incremental_job,
    deputy_roster_incremental_job,
    deputy_contact_incremental_job,
    deputy_timesheet_pay_return_incremental_job,
    deputy_employee_incremental_job,
    deputy_employee_history_incremental_job,
    deputy_company_job,
    deputy_operational_unit_job,
    deputy_pay_period_job,
    deputy_employee_job,
    deputy_employee_agreement_job,
    deputy_employment_contract_job,
    deputy_role_job,
    deputy_company_period_job,
    deputy_pay_rules_job,
    deputy_stress_profile_job,
    leaftrade_dispensaries_job,
    leaftrade_accounts_job,
    leaftrade_strains_job,
    leaftrade_dispensary_classes_job,
    leaftrade_categories_job,
    leaftrade_stock_locations_job,
    leaftrade_vendor_users_new_job,
    leaftrade_products_job,
    leaftrade_batches_job,
    leaftrade_product_variants_job,
    leaftrade_stock_job,
    leaftrade_inventory_job,
]

# Default: no automatic schedules (materialize from UI or run jobs manually).
OPTIONAL_SCHEDULES = [
    sharepoint_graph_daily_schedule,
    sharepoint_sweed_product_map_daily_schedule,
    sharepoint_rtl_product_mapping_daily_schedule,
    sharepoint_lt_product_mapping_daily_schedule,
]

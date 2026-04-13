"""Dagster ``@dlt_assets`` for Deputy → Snowflake (sixteen pipelines)."""

import dlt
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

from data_pipelines.deputy.sources import (
    deputy_company_period_source,
    deputy_company_source,
    deputy_contact_incremental_source,
    deputy_employee_agreement_source,
    deputy_employee_history_incremental_source,
    deputy_employee_incremental_source,
    deputy_employee_source,
    deputy_employment_contract_source,
    deputy_operational_unit_source,
    deputy_pay_period_source,
    deputy_pay_rules_source,
    deputy_role_source,
    deputy_roster_incremental_source,
    deputy_stress_profile_source,
    deputy_timesheet_incremental_source,
    deputy_timesheet_pay_return_incremental_source,
)
from data_pipelines.translators import TaggedDltTranslator

_DEPUTY_DESCRIPTIONS: dict[str, str] = {
    "timesheet_incremental": (
        "Clock-in/out timesheet entries: hours worked, breaks, costs, payroll status."
        " Incremental merge on Modified."
    ),
    "roster_incremental": (
        "Scheduled shifts and roster assignments: start/end times, employee allocation,"
        " publish status. Incremental merge on Modified."
    ),
    "contact_incremental": (
        "Contact details for employees and Deputy users: addresses, phone numbers,"
        " emergency contacts. Incremental merge on Modified."
    ),
    "timesheet_pay_return_incremental": (
        "Payroll return line items from approved timesheets, linking hours to export"
        " categories. Incremental merge on Modified."
    ),
    "employee_incremental": (
        "Employee master records: personal details, active status, role, company,"
        " hire metadata. Incremental merge on Modified."
    ),
    "employee_history_incremental": (
        "Historical change log of employee record modifications capturing prior field"
        " values. Incremental merge on Modified."
    ),
    "company": (
        "Company/location dimension: physical business sites and organizational units"
        " in Deputy."
    ),
    "operational_unit": (
        "Operational unit (area/department) dimension: work areas within locations"
        " used for scheduling and reporting."
    ),
    "pay_period": (
        "Pay period dimension: payroll cycle boundaries with start/end dates and"
        " period type."
    ),
    "employee": (
        "Full-load employee dimension: complete employee records for reference and"
        " lookup use."
    ),
    "employee_agreement": (
        "Employee agreement records linking employees to contracts, pay points, and"
        " payroll configurations."
    ),
    "employment_contract": (
        "Employment contract templates: period types, base pay rules, employment"
        " conditions, and award structures."
    ),
    "role": (
        "Employee role/position dimension: job titles and positions assignable to"
        " employees."
    ),
    "company_period": (
        "Company period dimension: maps companies to pay periods, defining which pay"
        " schedule applies per location."
    ),
    "pay_rules": (
        "Pay rule definitions: hourly rates, multipliers, salary settings, and"
        " remuneration rules for payroll calculations."
    ),
    "stress_profile": (
        "Stress profile records: workforce demand modeling and staffing requirement"
        " parameters for scheduling."
    ),
}

_deputy_translator = TaggedDltTranslator(
    source_tag="Deputy",
    descriptions=_DEPUTY_DESCRIPTIONS,
)


def _deputy_dataset_name() -> str:
    return str(dlt.config.get("sources.deputy.snowflake_dataset_name") or "DEPUTY")


@dlt_assets(
    dlt_source=deputy_timesheet_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_timesheet_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_timesheet_incremental",
    group_name="deputy_timesheet_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_timesheet_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_roster_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_roster_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_roster_incremental",
    group_name="deputy_roster_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_roster_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_contact_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_contact_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_contact_incremental",
    group_name="deputy_contact_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_contact_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_timesheet_pay_return_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_timesheet_pay_return_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_timesheet_pay_return_incremental",
    group_name="deputy_timesheet_pay_return_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_timesheet_pay_return_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_employee_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_employee_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_employee_incremental",
    group_name="deputy_employee_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_employee_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_employee_history_incremental_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_employee_history_incremental",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_employee_history_incremental",
    group_name="deputy_employee_history_incremental",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_employee_history_incremental_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_company_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_company",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_company",
    group_name="deputy_company",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_company_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_operational_unit_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_operational_unit",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_operational_unit",
    group_name="deputy_operational_unit",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_operational_unit_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_pay_period_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_pay_period",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_pay_period",
    group_name="deputy_pay_period",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_pay_period_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_employee_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_employee",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_employee",
    group_name="deputy_employee",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_employee_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_employee_agreement_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_employee_agreement",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_employee_agreement",
    group_name="deputy_employee_agreement",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_employee_agreement_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_employment_contract_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_employment_contract",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_employment_contract",
    group_name="deputy_employment_contract",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_employment_contract_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_role_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_role",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_role",
    group_name="deputy_role",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_role_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_company_period_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_company_period",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_company_period",
    group_name="deputy_company_period",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_company_period_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_pay_rules_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_pay_rules",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_pay_rules",
    group_name="deputy_pay_rules",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_pay_rules_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=deputy_stress_profile_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="deputy_stress_profile",
        destination="snowflake",
        dataset_name=_deputy_dataset_name(),
        dev_mode=False,
    ),
    name="deputy_stress_profile",
    group_name="deputy_stress_profile",
    dagster_dlt_translator=_deputy_translator,
)
def deputy_stress_profile_assets(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
):
    yield from dlt.run(context=context)

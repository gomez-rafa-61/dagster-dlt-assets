"""Deputy resource QUERY endpoints → dlt resources (merge on ``Id``, cursor ``Modified``).

Sixteen pipelines: six incremental fact streams, three existing dimension streams,
and seven new dimension/reference streams (employee full-load, employee_agreement,
employment_contract, role, company_period, pay_rules, stress_profile).

Configure ``[sources.deputy]`` in ``.dlt/config.toml`` and ``api_token`` in ``.dlt/secrets.toml``.
Run with **project root** as cwd so dlt resolves ``.dlt/``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

import dlt
import requests
from dlt.extract.incremental import Incremental

logger = logging.getLogger(__name__)

_DEFAULT_INITIAL_2022 = "2022-06-01T00:00:00-07:00"
_DEFAULT_INITIAL_CONTACT = "2015-12-01T00:00:00-07:00"
_DEFAULT_INITIAL_DIMENSION = "1970-01-01T00:00:00+00:00"


def _base_url() -> str:
    return str(dlt.config["sources.deputy.base_url"]).rstrip("/")


def _api_token() -> str:
    return str(dlt.secrets["sources.deputy.api_token"]).strip()


def _page_size() -> int:
    return int(dlt.config.get("sources.deputy.page_size") or 500)


def _initial_modified(stream_key: str, default: str) -> str:
    key = f"sources.deputy.initial_modified_{stream_key}"
    val = dlt.config.get(key)
    return str(val).strip() if val is not None and str(val).strip() else default


def _query_url(resource_segment: str) -> str:
    return f"{_base_url()}/api/v1/resource/{resource_segment}/QUERY"


def _post_query_page(
    resource_segment: str,
    cursor_data: str,
    start: int,
    *,
    ignore_404: bool,
) -> list[dict[str, Any]]:
    url = _query_url(resource_segment)
    body: dict[str, Any] = {
        "sort": {"Modified": "asc"},
        "search": {
            "s1": {
                "type": "gt",
                "field": "Modified",
                "data": cursor_data,
            }
        },
        "start": start,
    }
    headers = {
        "Authorization": f"Bearer {_api_token()}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, json=body, headers=headers, timeout=120)
    if ignore_404 and r.status_code == 404:
        logger.warning("Deputy %s/QUERY returned 404 (ignored per config)", resource_segment)
        return []
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise TypeError(
            f"Deputy {resource_segment}/QUERY expected JSON array, got {type(data).__name__}"
        )
    return data


def _iter_all_rows(
    resource_segment: str,
    cursor_data: str,
    *,
    ignore_404: bool,
) -> Iterator[dict[str, Any]]:
    page_size = _page_size()
    start = 0
    while True:
        batch = _post_query_page(resource_segment, cursor_data, start, ignore_404=ignore_404)
        if not batch:
            break
        yield from batch
        start += len(batch)
        if len(batch) < page_size:
            break


def _resource_factory(
    table_name: str,
    resource_segment: str,
    stream_key: str,
    default_initial: str,
    *,
    ignore_404: bool = False,
) -> Callable[[], Any]:
    initial = _initial_modified(stream_key, default_initial)

    @dlt.resource(name=table_name, primary_key="Id", write_disposition="merge")
    def _resource(
        modified: Incremental[str] = dlt.sources.incremental(
            "Modified",
            initial_value=initial,
            row_order="asc",
        ),
    ) -> Iterator[dict[str, Any]]:
        cursor = modified.last_value
        if cursor is None:
            cursor = initial
        cursor_str = str(cursor)
        yield from _iter_all_rows(
            resource_segment,
            cursor_str,
            ignore_404=ignore_404,
        )

    _resource.__name__ = table_name
    return _resource


_timesheet_incremental_res = _resource_factory(
    "timesheet_incremental",
    "Timesheet",
    "timesheet_incremental",
    _DEFAULT_INITIAL_2022,
)
_roster_incremental_res = _resource_factory(
    "roster_incremental",
    "Roster",
    "roster_incremental",
    _DEFAULT_INITIAL_2022,
)
_contact_incremental_res = _resource_factory(
    "contact_incremental",
    "Contact",
    "contact_incremental",
    _DEFAULT_INITIAL_CONTACT,
)
_timesheet_pay_return_incremental_res = _resource_factory(
    "timesheet_pay_return_incremental",
    "TimesheetPayReturn",
    "timesheet_pay_return_incremental",
    _DEFAULT_INITIAL_2022,
)
_employee_incremental_res = _resource_factory(
    "employee_incremental",
    "Employee",
    "employee_incremental",
    _DEFAULT_INITIAL_2022,
    ignore_404=True,
)
_employee_history_incremental_res = _resource_factory(
    "employee_history_incremental",
    "EmployeeHistory",
    "employee_history_incremental",
    _DEFAULT_INITIAL_2022,
)
_company_res = _resource_factory(
    "company",
    "Company",
    "company",
    _DEFAULT_INITIAL_DIMENSION,
)
_operational_unit_res = _resource_factory(
    "operational_unit",
    "OperationalUnit",
    "operational_unit",
    _DEFAULT_INITIAL_DIMENSION,
)
_pay_period_res = _resource_factory(
    "pay_period",
    "PayPeriod",
    "pay_period",
    _DEFAULT_INITIAL_DIMENSION,
)
_employee_res = _resource_factory(
    "employee",
    "Employee",
    "employee",
    _DEFAULT_INITIAL_DIMENSION,
)
_employee_agreement_res = _resource_factory(
    "employee_agreement",
    "EmployeeAgreement",
    "employee_agreement",
    _DEFAULT_INITIAL_DIMENSION,
)
_employment_contract_res = _resource_factory(
    "employment_contract",
    "EmploymentContract",
    "employment_contract",
    _DEFAULT_INITIAL_DIMENSION,
)
_role_res = _resource_factory(
    "role",
    "EmployeeRole",
    "role",
    _DEFAULT_INITIAL_DIMENSION,
)
_company_period_res = _resource_factory(
    "company_period",
    "CompanyPeriod",
    "company_period",
    _DEFAULT_INITIAL_DIMENSION,
)
_pay_rules_res = _resource_factory(
    "pay_rules",
    "PayRules",
    "pay_rules",
    _DEFAULT_INITIAL_DIMENSION,
)
_stress_profile_res = _resource_factory(
    "stress_profile",
    "StressProfile",
    "stress_profile",
    _DEFAULT_INITIAL_DIMENSION,
)


@dlt.source(name="deputy_timesheet_incremental")
def deputy_timesheet_incremental_source() -> Iterator[Any]:
    """Clock-in/out timesheet entries: hours worked, breaks, costs, payroll status. Incremental merge on Modified."""
    yield _timesheet_incremental_res


@dlt.source(name="deputy_roster_incremental")
def deputy_roster_incremental_source() -> Iterator[Any]:
    """Scheduled shifts and roster assignments: start/end times, employee allocation, publish status. Incremental merge on Modified."""
    yield _roster_incremental_res


@dlt.source(name="deputy_contact_incremental")
def deputy_contact_incremental_source() -> Iterator[Any]:
    """Contact details for employees and Deputy users: addresses, phone numbers, emergency contacts. Incremental merge on Modified."""
    yield _contact_incremental_res


@dlt.source(name="deputy_timesheet_pay_return_incremental")
def deputy_timesheet_pay_return_incremental_source() -> Iterator[Any]:
    """Payroll return line items from approved timesheets, linking hours to export categories. Incremental merge on Modified."""
    yield _timesheet_pay_return_incremental_res


@dlt.source(name="deputy_employee_incremental")
def deputy_employee_incremental_source() -> Iterator[Any]:
    """Employee master records: personal details, active status, role, company, hire metadata. Incremental merge on Modified."""
    yield _employee_incremental_res


@dlt.source(name="deputy_employee_history_incremental")
def deputy_employee_history_incremental_source() -> Iterator[Any]:
    """Historical change log of employee record modifications capturing prior field values. Incremental merge on Modified."""
    yield _employee_history_incremental_res


@dlt.source(name="deputy_company")
def deputy_company_source() -> Iterator[Any]:
    """Company/location dimension: physical business sites and organizational units in Deputy."""
    yield _company_res


@dlt.source(name="deputy_operational_unit")
def deputy_operational_unit_source() -> Iterator[Any]:
    """Operational unit (area/department) dimension: work areas within locations used for scheduling and reporting."""
    yield _operational_unit_res


@dlt.source(name="deputy_pay_period")
def deputy_pay_period_source() -> Iterator[Any]:
    """Pay period dimension: payroll cycle boundaries with start/end dates and period type."""
    yield _pay_period_res


@dlt.source(name="deputy_employee")
def deputy_employee_source() -> Iterator[Any]:
    """Full-load employee dimension: complete employee records for reference and lookup use."""
    yield _employee_res


@dlt.source(name="deputy_employee_agreement")
def deputy_employee_agreement_source() -> Iterator[Any]:
    """Employee agreement records linking employees to contracts, pay points, and payroll configurations."""
    yield _employee_agreement_res


@dlt.source(name="deputy_employment_contract")
def deputy_employment_contract_source() -> Iterator[Any]:
    """Employment contract templates: period types, base pay rules, employment conditions, and award structures."""
    yield _employment_contract_res


@dlt.source(name="deputy_role")
def deputy_role_source() -> Iterator[Any]:
    """Employee role/position dimension: job titles and positions assignable to employees."""
    yield _role_res


@dlt.source(name="deputy_company_period")
def deputy_company_period_source() -> Iterator[Any]:
    """Company period dimension: maps companies to pay periods, defining which pay schedule applies per location."""
    yield _company_period_res


@dlt.source(name="deputy_pay_rules")
def deputy_pay_rules_source() -> Iterator[Any]:
    """Pay rule definitions: hourly rates, multipliers, salary settings, and remuneration rules for payroll calculations."""
    yield _pay_rules_res


@dlt.source(name="deputy_stress_profile")
def deputy_stress_profile_source() -> Iterator[Any]:
    """Stress profile records: workforce demand modeling and staffing requirement parameters for scheduling."""
    yield _stress_profile_res



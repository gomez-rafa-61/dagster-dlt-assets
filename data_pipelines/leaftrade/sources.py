"""Leaftrade vendor REST API → dlt resources (12 streams, per-resource pipelines).

Configure ``[sources.leaftrade]`` in ``.dlt/config.toml`` and ``api_key`` in ``.dlt/secrets.toml``.
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

_DEFAULT_START_DATE = "2019-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _base_url() -> str:
    return str(dlt.config.get("sources.leaftrade.base_url") or "https://app.leaf.trade").rstrip("/")


def _api_key() -> str:
    return str(dlt.secrets["sources.leaftrade.api_key"]).strip()


def _page_size() -> int:
    return int(dlt.config.get("sources.leaftrade.page_size") or 100)


def _start_date() -> str:
    val = dlt.config.get("sources.leaftrade.start_date")
    return str(val).strip() if val is not None and str(val).strip() else _DEFAULT_START_DATE


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_page(
    endpoint_path: str,
    page: int,
    page_size: int,
    extra_params: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    url = f"{_base_url()}{endpoint_path}"
    params: dict[str, Any] = {"page": page, "page_size": page_size}
    if extra_params:
        params.update(extra_params)
    headers = {"Authorization": _api_key()}
    r = requests.get(url, params=params, headers=headers, timeout=120)
    if r.status_code in (404, 500):
        logger.warning("Leaftrade %s returned %s (ignored)", endpoint_path, r.status_code)
        return []
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        return data.get("results", [])
    return []


def _iter_all_pages(
    endpoint_path: str,
    page_size: int,
    extra_params: dict[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    page = 1
    while True:
        batch = _get_page(endpoint_path, page, page_size, extra_params)
        if not batch:
            break
        yield from batch
        if len(batch) < page_size:
            break
        page += 1


# ---------------------------------------------------------------------------
# Resource factories
# ---------------------------------------------------------------------------

def _full_load_factory(
    table_name: str,
    endpoint_path: str,
) -> Callable[[], Any]:
    @dlt.resource(name=table_name, primary_key="id", write_disposition="replace")
    def _resource() -> Iterator[dict[str, Any]]:
        yield from _iter_all_pages(endpoint_path, _page_size())

    _resource.__name__ = table_name
    return _resource


def _incremental_factory(
    table_name: str,
    endpoint_path: str,
    sort_params: dict[str, str],
) -> Callable[[], Any]:
    initial = _start_date()

    @dlt.resource(name=table_name, primary_key="id", write_disposition="merge")
    def _resource(
        updated_at: Incremental[str] = dlt.sources.incremental(
            "updated_at",
            initial_value=initial,
            row_order="asc",
        ),
    ) -> Iterator[dict[str, Any]]:
        cursor = updated_at.last_value
        if cursor is None:
            cursor = initial
        extra_params = {**sort_params, "updated_at_after": str(cursor)}
        yield from _iter_all_pages(endpoint_path, _page_size(), extra_params)

    _resource.__name__ = table_name
    return _resource


# ---------------------------------------------------------------------------
# Full-load resources (7)
# ---------------------------------------------------------------------------

_dispensaries_res = _full_load_factory("dispensaries", "/api/v4/vendor/dispensaries/")
_accounts_res = _full_load_factory("accounts", "/api/v3/vendor/accounts/")
_strains_res = _full_load_factory("strains", "/api/v4/vendor/strains/")
_dispensary_classes_res = _full_load_factory("dispensary_classes", "/api/v3/vendor/dispensary-classes/")
_categories_res = _full_load_factory("categories", "/api/v4/vendor/categories/")
_stock_locations_res = _full_load_factory("stock_locations", "/api/v4/vendor/stock-locations/")
_vendor_users_new_res = _full_load_factory("vendor_users_new", "/api/v4/vendor/vendor-users/")

# ---------------------------------------------------------------------------
# Incremental resources (5)
# ---------------------------------------------------------------------------

_products_res = _incremental_factory(
    "products",
    "/api/v4/vendor/products/",
    {"sort": "updated", "direction": "asc"},
)
_batches_res = _incremental_factory(
    "batches",
    "/api/v4/vendor/batches/",
    {"ordering": "updated_at", "direction": "asc"},
)
_product_variants_res = _incremental_factory(
    "product_variants",
    "/api/v4/vendor/product-variants/",
    {"ordering": "updated_at", "direction": "asc"},
)
_stock_res = _incremental_factory(
    "stock",
    "/api/v4/vendor/stock/",
    {"ordering": "updated_at", "direction": "asc"},
)
_inventory_res = _incremental_factory(
    "inventory",
    "/api/v3/vendor/inventory/",
    {"ordering": "updated_at", "direction": "asc"},
)


# ---------------------------------------------------------------------------
# Sources (one per resource, matching deputy pattern)
# ---------------------------------------------------------------------------

@dlt.source(name="leaftrade_dispensaries")
def leaftrade_dispensaries_source() -> Iterator[Any]:
    yield _dispensaries_res


@dlt.source(name="leaftrade_accounts")
def leaftrade_accounts_source() -> Iterator[Any]:
    yield _accounts_res


@dlt.source(name="leaftrade_strains")
def leaftrade_strains_source() -> Iterator[Any]:
    yield _strains_res


@dlt.source(name="leaftrade_dispensary_classes")
def leaftrade_dispensary_classes_source() -> Iterator[Any]:
    yield _dispensary_classes_res


@dlt.source(name="leaftrade_categories")
def leaftrade_categories_source() -> Iterator[Any]:
    yield _categories_res


@dlt.source(name="leaftrade_stock_locations")
def leaftrade_stock_locations_source() -> Iterator[Any]:
    yield _stock_locations_res


@dlt.source(name="leaftrade_vendor_users_new")
def leaftrade_vendor_users_new_source() -> Iterator[Any]:
    yield _vendor_users_new_res


@dlt.source(name="leaftrade_products")
def leaftrade_products_source() -> Iterator[Any]:
    yield _products_res


@dlt.source(name="leaftrade_batches")
def leaftrade_batches_source() -> Iterator[Any]:
    yield _batches_res


@dlt.source(name="leaftrade_product_variants")
def leaftrade_product_variants_source() -> Iterator[Any]:
    yield _product_variants_res


@dlt.source(name="leaftrade_stock")
def leaftrade_stock_source() -> Iterator[Any]:
    yield _stock_res


@dlt.source(name="leaftrade_inventory")
def leaftrade_inventory_source() -> Iterator[Any]:
    yield _inventory_res

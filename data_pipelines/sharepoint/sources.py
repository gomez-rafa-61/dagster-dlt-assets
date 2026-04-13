"""Microsoft Graph → Snowflake: SharePoint folder listing and CSV downloads.

Uses a **service principal** (client credentials) for Graph; configure ``tenant_id`` / ``client_id``
in ``.dlt/config.toml`` and ``client_secret`` in ``.dlt/secrets.toml``.

See: https://learn.microsoft.com/en-us/graph/api/site-get
     https://learn.microsoft.com/en-us/graph/api/driveitem-list-children
     https://learn.microsoft.com/en-us/graph/api/driveitem-get-content
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import dlt
import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

# In-process cache: Graph client-credentials tokens are reused until near expiry.
_graph_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}


def _get_graph_access_token() -> str:
    """Acquire an app-only Microsoft Graph token (OAuth2 client credentials).

    Expects ``tenant_id`` and ``client_id`` under ``[sources.sharepoint_graph]`` in
    ``config.toml``, and ``client_secret`` in ``.dlt/secrets.toml`` (same section).
    """
    now = time.time()
    cached: str | None = _graph_token_cache["token"]
    exp: float = _graph_token_cache["expires_at"]
    if cached and now < exp - 60:
        return cached

    tenant_id = dlt.config["sources.sharepoint_graph.tenant_id"]
    client_id = dlt.config["sources.sharepoint_graph.client_id"]
    client_secret = dlt.secrets["sources.sharepoint_graph.client_secret"]
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    r = requests.post(
        token_url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=120,
    )
    r.raise_for_status()
    body = r.json()
    access_token = body["access_token"]
    expires_in = int(body.get("expires_in", 3600))
    _graph_token_cache["token"] = access_token
    _graph_token_cache["expires_at"] = now + expires_in
    return access_token


def _graph_site_id(graph_base_url: str, access_token: str, sharepoint_site_key: str) -> str:
    base = graph_base_url.rstrip("/")
    site_enc = quote(sharepoint_site_key.strip(), safe="")
    url = f"{base}/sites/{site_enc}"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()["id"]


def _graph_drive_item_content_url(
    graph_base_url: str,
    site_id: str,
    folder_path: str,
    file_name: str,
) -> str:
    base = graph_base_url.rstrip("/")
    folder = folder_path.strip().strip("/")
    segments = [p for p in folder.split("/") if p] + [file_name.strip()]
    path_enc = "/".join(quote(seg, safe="") for seg in segments)
    return f"{base}/sites/{site_id}/drive/root:/{path_enc}:/content"


def _iter_sharepoint_csv_rows(file_name: str) -> Iterator[dict[str, Any]]:
    """Fetch file from configured SharePoint folder via Graph; yield normalized CSV rows."""
    access_token = _get_graph_access_token()
    sharepoint_site_key = dlt.config["sources.sharepoint_graph.sharepoint_site_key"]
    sharepoint_folder_path = dlt.config["sources.sharepoint_graph.sharepoint_folder_path"]
    graph_base_url = (
        dlt.config.get("sources.sharepoint_graph.graph_base_url")
        or "https://graph.microsoft.com/v1.0/"
    )

    site_id = _graph_site_id(graph_base_url, access_token, sharepoint_site_key)
    url = _graph_drive_item_content_url(
        graph_base_url,
        site_id,
        sharepoint_folder_path,
        file_name,
    )
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=300,
    )
    r.raise_for_status()
    text = r.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield {k: (v if v != "" else None) for k, v in row.items()}


@dlt.source(name="sharepoint_graph")
def sharepoint_graph_source(
    sharepoint_site_key: str = dlt.config.value,
    sharepoint_folder_path: str = dlt.config.value,
    graph_base_url: str = "https://graph.microsoft.com/v1.0/",
) -> Iterator[Any]:
    """Load SharePoint folder children via Microsoft Graph (path under document library root).

    Authenticates with a **service principal**: ``tenant_id`` and ``client_id`` in config,
    ``client_secret`` in ``.dlt/secrets.toml``. Token scope ``https://graph.microsoft.com/.default``.

    Args:
        sharepoint_site_key: Site segment as ``{hostname}:/{server-relative-site-path}``, e.g.
            ``curaleaf.sharepoint.com:/sites/DataWonderland``.
        sharepoint_folder_path: Folder path inside the **default** document library root.
            If listing is empty or 404, try prefixing ``Shared Documents/`` (see config comments).
        graph_base_url: Microsoft Graph root (override per environment if needed).

    Example:
        ``pipeline.run(sharepoint_graph_source())`` with config/secrets filled in.
    """
    access_token = _get_graph_access_token()
    site_enc = quote(sharepoint_site_key.strip(), safe="")
    site_path = f"sites/{site_enc}"

    folder = sharepoint_folder_path.strip().strip("/")
    folder_enc = "/".join(quote(part, safe="") for part in folder.split("/") if part)
    children_path = f"sites/{{resources.graph_site.id}}/drive/root:/{folder_enc}:/children"

    config: RESTAPIConfig = {
        "client": {
            "base_url": graph_base_url,
            "auth": {"type": "bearer", "token": access_token},
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "graph_site",
                "endpoint": {
                    "path": site_path,
                    "data_selector": "$",
                    "paginator": {"type": "single_page"},
                },
            },
            {
                "name": "sharepoint_folder_children",
                "endpoint": {
                    "path": children_path,
                    "data_selector": "value",
                    "params": {"$top": 200},
                    "paginator": {
                        "type": "json_link",
                        "next_url_path": '"@odata.nextLink"',
                    },
                },
            },
        ],
    }
    yield from rest_api_resources(config)


@dlt.resource(name="sweed_product_map", write_disposition="replace")
def sweed_product_map_rows() -> Iterator[dict[str, Any]]:
    """Download configured CSV file from SharePoint folder via Graph."""
    sharepoint_csv_file_name = dlt.config["sources.sharepoint_graph.sharepoint_csv_file_name"]
    yield from _iter_sharepoint_csv_rows(sharepoint_csv_file_name)


@dlt.resource(
    name="rtl_product_mapping",
    table_name="RTL_Product_Mapping",
    write_disposition="replace",
)
def rtl_product_mapping_rows() -> Iterator[dict[str, Any]]:
    """Map RTL_Product_Mapping.csv to table RTL_Product_Mapping."""
    yield from _iter_sharepoint_csv_rows("RTL_Product_Mapping.csv")


@dlt.resource(
    name="lt_product_mapping",
    table_name="LT_Product_Mapping",
    write_disposition="replace",
)
def lt_product_mapping_rows() -> Iterator[dict[str, Any]]:
    """Map LT_Product_Mapping.csv to table LT_Product_Mapping."""
    yield from _iter_sharepoint_csv_rows("LT_Product_Mapping.csv")


@dlt.source(name="sharepoint_sweed_product_map")
def sweed_product_map_source() -> Iterator[Any]:
    """Sweed POS product-map CSV from SharePoint → Snowflake (full replace)."""
    yield sweed_product_map_rows()


@dlt.source(name="sharepoint_rtl_product_mapping")
def rtl_product_mapping_source() -> Iterator[Any]:
    """RTL product-mapping CSV from SharePoint → Snowflake (full replace)."""
    yield rtl_product_mapping_rows()


@dlt.source(name="sharepoint_lt_product_mapping")
def lt_product_mapping_source() -> Iterator[Any]:
    """LeafTrade product-mapping CSV from SharePoint → Snowflake (full replace)."""
    yield lt_product_mapping_rows()


@dlt.source(name="sharepoint_csv")
def sharepoint_csv_source() -> Iterator[Any]:
    """Bundle CSV resources for CLI entry point (backward compat)."""
    yield sweed_product_map_rows()
    yield rtl_product_mapping_rows()
    yield lt_product_mapping_rows()


def load_sweed_product_map_csv() -> None:
    """Load SharePoint CSVs into Snowflake (see ``.dlt/config.toml`` for dataset/schema)."""
    dataset_name = dlt.config.get("sources.sharepoint_graph.snowflake_dataset_name") or "SHAREPOINT"
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_sweed_product_map",
        destination="snowflake",
        dataset_name=dataset_name,
        dev_mode=False,
    )
    load_info = pipeline.run(sharepoint_csv_source())
    print(load_info)  # noqa: T201


def load_sharepoint_drive_root() -> None:
    """Graph site + folder children → Snowflake (sharepoint_graph_rest_dataset_name)."""
    dataset_name = (
        dlt.config.get("sources.sharepoint_graph.sharepoint_graph_rest_dataset_name")
        or "sharepoint_graph_data"
    )
    pipeline = dlt.pipeline(
        pipeline_name="sharepoint_graph",
        destination="snowflake",
        dataset_name=dataset_name,
        dev_mode=False,
    )
    load_info = pipeline.run(sharepoint_graph_source())
    print(load_info)  # noqa: T201

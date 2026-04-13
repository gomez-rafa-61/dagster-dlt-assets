"""CLI entrypoint for SharePoint Graph → Snowflake (dlt).

Implementation lives in ``data_pipelines.sharepoint.sources``. Run from **project root** so dlt
finds ``.dlt/config.toml`` and ``.dlt/secrets.toml``:

    uv run python rest_api_pipeline.py
    uv run python rest_api_pipeline.py csv
"""

from __future__ import annotations

from data_pipelines.sharepoint.sources import load_sharepoint_drive_root, load_sweed_product_map_csv

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "csv":
        load_sweed_product_map_csv()
    else:
        load_sharepoint_drive_root()

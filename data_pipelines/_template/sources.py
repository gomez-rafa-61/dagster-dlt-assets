"""dlt sources/resources for <domain> (replace with real implementation).

Add sections to ``.dlt/config.toml`` and ``.dlt/secrets.toml`` for this source.

Run pipelines with **project root** as cwd so dlt finds ``.dlt/``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import dlt


@dlt.resource(name="example_placeholder", write_disposition="replace")
def example_placeholder() -> Iterator[dict[str, Any]]:
    """Remove this resource and add real endpoints."""
    yield {"_stub": True}


@dlt.source(name="my_domain")
def my_domain_source() -> Iterator[Any]:
    """Stub source — replace with real ``@dlt.resource`` / REST config."""
    yield example_placeholder


def load_my_domain() -> None:
    """Optional standalone entrypoint (pattern: thin script at repo root)."""
    pipeline = dlt.pipeline(
        pipeline_name="my_domain",
        destination="snowflake",
        dataset_name="my_domain_data",
        dev_mode=True,
    )
    load_info = pipeline.run(my_domain_source())
    print(load_info)  # noqa: T201

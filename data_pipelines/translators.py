"""Shared DagsterDltTranslator subclasses for tagging and describing assets."""

from __future__ import annotations

from dataclasses import dataclass, field

from dagster_dlt import DagsterDltTranslator


@dataclass
class TaggedDltTranslator(DagsterDltTranslator):
    """Injects a ``source`` tag and per-resource descriptions into every asset spec.

    Args:
        source_tag: Value for the ``source`` tag (e.g. ``"SharePoint"``, ``"Deputy"``).
        descriptions: Mapping of dlt resource name to Dagster asset description.
            Resources not present in the dict keep the default description derived
            from the resource's docstring.
    """

    source_tag: str = ""
    descriptions: dict[str, str] = field(default_factory=dict)

    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        spec = spec.merge_attributes(tags={"source": self.source_tag})
        desc = self.descriptions.get(data.resource.name)
        if desc:
            spec = spec.replace_attributes(description=desc)
        return spec

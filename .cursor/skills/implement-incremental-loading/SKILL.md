---
name: implement-incremental-loading
description: Add incremental loading to a dlt pipeline resource. Use when the user wants to load only new/updated records instead of full replace, set up merge write disposition, or configure cursor-based incremental extraction. Covers dlt.sources.incremental, merge keys, initial values, and config-driven cursors.
argument-hint: "[resource-name] [cursor-field]"
---

# Implement incremental loading

Convert a full-load resource to incremental merge so only new or modified records are extracted on each run.

Parse `$ARGUMENTS`:
- `resource-name` (required): the dlt resource to make incremental
- `cursor-field` (optional): the field to track (e.g. `Modified`, `updated_at`). If omitted, research the API to find a suitable field.

**Essential Reading:** https://dlthub.com/docs/general-usage/incremental-loading

## Prerequisites

- A working full-load pipeline exists (tested with `debug-pipeline`, validated with `validate-data`)
- The API returns a timestamp or monotonic field that indicates when records were created/modified

## Step 1: Identify the cursor field

The cursor field must be:
- Present on every record
- Monotonically increasing (newer records have higher values)
- Filterable via the API (query param, request body, or GraphQL variable)

Common patterns:
| API style | Cursor field | Filter mechanism |
|-----------|-------------|------------------|
| REST GET (LeafTrade) | `updated_at` | Query param `updated_at_after` |
| REST POST QUERY (Deputy) | `Modified` | JSON body `search.s1.data` with `gt` operator |
| REST GET (generic) | `modified_at`, `updated_at` | Query param `since`, `after`, `modified_after` |

If the API does not support server-side filtering on the cursor field, dlt can still filter client-side, but this defeats the purpose for large datasets.

## Step 2: Add dlt.sources.incremental to the resource

Change the resource decorator and function signature:

```python
from dlt.extract.incremental import Incremental

@dlt.resource(name="<resource>", primary_key="<pk>", write_disposition="merge")
def my_resource(
    cursor: Incremental[str] = dlt.sources.incremental(
        "<cursor_field>",
        initial_value="<start_date>",
        row_order="asc",
    ),
) -> Iterator[dict[str, Any]]:
    last = cursor.last_value
    if last is None:
        last = "<start_date>"
    # Pass `last` to API as filter
    yield from fetch_records_since(last)
```

**Key parameters:**
- `cursor_path` (1st arg): JSONPath to the cursor field in each record (e.g. `"Modified"`, `"updated_at"`)
- `initial_value`: starting point for the first run (ISO8601 timestamp or epoch)
- `row_order`: `"asc"` if records arrive sorted by cursor (allows dlt to optimize)

**Write disposition must be `"merge"`** so updated records overwrite existing rows.

**`primary_key` is required** for merge to identify which rows to update.

## Step 3: Configure initial value from config

Make the initial cursor value configurable per environment:

```python
def _initial_value(stream_key: str, default: str) -> str:
    key = f"sources.<domain>.initial_modified_{stream_key}"
    val = dlt.config.get(key)
    return str(val).strip() if val and str(val).strip() else default
```

In `.dlt/config.toml`:
```toml
[sources.<domain>]
initial_modified_<stream> = "2022-01-01T00:00:00Z"
```

This allows adjusting the backfill window per stream without code changes.

## Step 4: Pass cursor to API

### Pattern A: GET with query param (LeafTrade style)

```python
extra_params = {"updated_at_after": str(cursor_value), "ordering": "updated_at", "direction": "asc"}
yield from _iter_all_pages(endpoint_path, page_size, extra_params)
```

### Pattern B: POST with JSON body filter (Deputy style)

```python
body = {
    "sort": {"Modified": "asc"},
    "search": {
        "s1": {"type": "gt", "field": "Modified", "data": cursor_value}
    },
    "start": offset,
}
```

### Pattern C: Declarative REST API config

```python
"resources": [{
    "name": "my_resource",
    "endpoint": {
        "path": "records",
        "params": {
            "since": {
                "type": "incremental",
                "cursor_path": "updated_at",
                "initial_value": "2022-01-01T00:00:00Z",
            },
            "order": "asc",
        },
    },
    "primary_key": "id",
    "write_disposition": "merge",
}]
```

## Step 5: Use a resource factory for consistency

When a domain has many incremental resources sharing the same pattern, use a factory:

```python
def _incremental_factory(table_name, endpoint_path, sort_params):
    initial = _start_date()

    @dlt.resource(name=table_name, primary_key="id", write_disposition="merge")
    def _resource(
        updated_at: Incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value=initial, row_order="asc",
        ),
    ) -> Iterator[dict[str, Any]]:
        cursor = updated_at.last_value or initial
        extra = {**sort_params, "updated_at_after": str(cursor)}
        yield from _iter_all_pages(endpoint_path, _page_size(), extra)

    _resource.__name__ = table_name
    return _resource
```

## Step 6: Test incremental behavior

1. Run with `.add_limit(1)` first to verify the cursor field is extracted
2. Run full, then run again -- second run should extract only new/modified records
3. Inspect pipeline state: `dlt pipeline -v <name> info` -- check `last_value` in resource state
4. Look for `"Bind incremental on <resource>"` in INFO logs

**Ref:** https://dlthub.com/docs/general-usage/incremental/troubleshooting.md

## Common pitfalls

- **Cursor not advancing:** the cursor field is missing from yielded records, or `row_order` is wrong
- **Duplicate rows on boundary:** records with cursor == last_value may be re-fetched. Use `on_cursor_value_missing="raise"` to catch missing cursors early
- **Timezone mismatch:** API returns UTC but initial_value has offset (or vice versa). Normalize timestamps
- **API does not support server-side filter:** incremental still works (client-side dedup) but is inefficient for large tables

## Next steps

- Use `debug-pipeline` to verify incremental runs extract the expected delta
- Use `validate-data` to confirm merge produces correct row counts
- Use `adjust-endpoint` if pagination needs tuning for full incremental loads

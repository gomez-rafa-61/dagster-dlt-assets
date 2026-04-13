---
name: implement-custom-pagination
description: Implement custom pagination for APIs that don't fit dlt's declarative REST API config. Use when the API uses POST-body offsets, non-standard page tokens, or requires custom stop conditions. Covers offset-based POST pagination (Deputy pattern) and page/page_size GET pagination (LeafTrade pattern).
argument-hint: "[api-name] [pagination-style]"
---

# Implement custom pagination

Build a custom paginated fetcher for APIs that don't work with dlt's built-in `rest_api` paginators.

Parse `$ARGUMENTS`:
- `api-name` (required): which API to paginate
- `pagination-style` (optional): `offset-post`, `page-get`, or describe the pattern

## When to use custom pagination

Use **declarative paginators** (in `RESTAPIConfig`) when possible. Use custom pagination when:
- The API uses POST requests with pagination in the request body (not query params)
- Stop conditions are non-standard (e.g. empty array vs. `has_more` field)
- The offset/cursor is embedded in the request body, not the URL
- Multiple pagination schemes exist across endpoints in the same API

**Essential Reading:** https://dlthub.com/docs/general-usage/http/rest-client.md

## Pattern A: Offset-based POST pagination (Deputy style)

Used when the API accepts a POST body with a `start` offset and returns a JSON array.

### Implementation

```python
def _post_query_page(
    resource_segment: str,
    cursor_data: str,
    start: int,
    *,
    ignore_404: bool,
) -> list[dict[str, Any]]:
    url = f"{_base_url()}/api/v1/resource/{resource_segment}/QUERY"
    body: dict[str, Any] = {
        "sort": {"Modified": "asc"},
        "search": {
            "s1": {"type": "gt", "field": "Modified", "data": cursor_data}
        },
        "start": start,
    }
    headers = {
        "Authorization": f"Bearer {_api_token()}",
        "Content-Type": "application/json",
    }
    r = requests.post(url, json=body, headers=headers, timeout=120)
    if ignore_404 and r.status_code == 404:
        logger.warning("%s/QUERY returned 404 (ignored)", resource_segment)
        return []
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise TypeError(f"Expected JSON array, got {type(data).__name__}")
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
```

### Key design decisions

- **Stop condition:** empty batch OR batch smaller than page_size (both signal end of data)
- **Offset tracks actual count:** `start += len(batch)` (not `+= page_size`) handles partial pages
- **ignore_404:** some Deputy resource segments return 404 for empty data; flag allows graceful handling
- **Timeout:** 120s for large queries; tune per API

## Pattern B: Page/page_size GET pagination (LeafTrade style)

Used when the API accepts `page` and `page_size` query params and returns results in a `results` array.

### Implementation

```python
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
        logger.warning("%s returned %s (ignored)", endpoint_path, r.status_code)
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
```

### Key design decisions

- **Data selector:** API wraps results in `{"results": [...]}`, extract with `.get("results", [])`
- **Error tolerance:** 404 and 500 logged as warnings and treated as empty (graceful degradation)
- **Page starts at 1:** many APIs use 1-indexed pages (verify with the specific API)
- **Extra params:** allows passing incremental cursors, sort order, filters alongside pagination

## Shared conventions

### Config helpers

Always read base URL, auth tokens, and page size from dlt config/secrets:

```python
def _base_url() -> str:
    return str(dlt.config.get("sources.<domain>.base_url") or "<default>").rstrip("/")

def _api_key() -> str:
    return str(dlt.secrets["sources.<domain>.api_key"]).strip()

def _page_size() -> int:
    return int(dlt.config.get("sources.<domain>.page_size") or 100)
```

### Resource factory pattern

When multiple endpoints share the same pagination:

```python
def _full_load_factory(table_name: str, endpoint_path: str) -> Callable[[], Any]:
    @dlt.resource(name=table_name, primary_key="id", write_disposition="replace")
    def _resource() -> Iterator[dict[str, Any]]:
        yield from _iter_all_pages(endpoint_path, _page_size())
    _resource.__name__ = table_name
    return _resource
```

### Debugging pagination

1. Set `log_level = "INFO"` in `.dlt/config.toml` to see request counts
2. Add a counter in the loop to log page numbers: `logger.info("Page %d: %d rows", page, len(batch))`
3. Use `.add_limit(2)` on the source to test with exactly 2 pages before going unlimited
4. Watch for infinite loops: if the same offset/page produces the same results, the paginator is broken

## When to switch back to declarative

If after implementing custom pagination you realize the pattern is standard (offset, page number, cursor in response header/body), consider switching to a declarative paginator in `RESTAPIConfig` for maintainability:

```python
"paginator": {
    "type": "offset",
    "limit": 100,
    "offset_param": "start",
    "limit_param": "page_size",
}
```

**Ref:** https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic.md (pagination section)

## Next steps

- Use `debug-pipeline` with INFO logging to verify pagination completes
- Use `adjust-endpoint` to remove `.add_limit()` once pagination is confirmed working
- Combine with `implement-incremental-loading` for incremental paginated extraction

"""Test Deputy API with multiple auth methods."""
import dlt
import requests

base_url = str(dlt.config["sources.deputy.base_url"]).rstrip("/")
api_token = str(dlt.secrets["sources.deputy.api_token"]).strip()
url = f"{base_url}/api/v1/me"

print(f"Base URL: {base_url}")
print(f"Testing endpoint: {url}")
print(f"Token: {api_token[:6]}...{api_token[-4:]} ({len(api_token)} chars)")
print()

auth_methods = {
    "Bearer": {"Authorization": f"Bearer {api_token}"},
    "DeputyToken": {"Authorization": f"DeputyToken {api_token}"},
    "OAuth": {"Authorization": f"OAuth {api_token}"},
    "dp-meta-auth": {"dp-meta-auth": api_token},
    "X-Deputy-Token": {"X-Deputy-Token": api_token},
}

for name, headers in auth_methods.items():
    headers["Content-Type"] = "application/json"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        status = r.status_code
        ct = r.headers.get("content-type", "N/A")
        body = r.text[:200].strip() or "(empty)"
        print(f"[{name}] Status: {status} | Content-Type: {ct}")
        if status != 401:
            print(f"  Body: {body}")
    except Exception as e:
        print(f"[{name}] Error: {e}")
    print()

# Also try the POST /QUERY with OAuth method (matching Airbyte YAML)
print("=== POST Employee/QUERY with OAuth header ===")
headers = {
    "Authorization": f"OAuth {api_token}",
    "Content-Type": "application/json",
}
body = {
    "sort": {"Modified": "asc"},
    "search": {
        "s1": {
            "type": "gt",
            "field": "Modified",
            "data": "2025-01-01T00:00:00-07:00",
        }
    },
    "start": 0,
}
try:
    r = requests.post(
        f"{base_url}/api/v1/resource/Employee/QUERY",
        json=body,
        headers=headers,
        timeout=30,
    )
    print(f"Status: {r.status_code}")
    ct = r.headers.get("content-type", "N/A")
    print(f"Content-Type: {ct}")
    print(f"Body: {r.text[:500]}")
except Exception as e:
    print(f"Error: {e}")

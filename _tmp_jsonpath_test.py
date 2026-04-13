from dlt.common.jsonpath import find_values

d = {"@odata.nextLink": "https://next"}
for p in ("$['@odata.nextLink']", '"@odata.nextLink"'):
    try:
        print(p, "->", find_values(p, d))
    except Exception as e:
        print(p, "ERR", e)

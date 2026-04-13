# New pipeline domain (template)

1. **Copy** this folder to `data_pipelines/<domain>/` (e.g. `leaftrade`).
2. **Rename** symbols: `my_domain` → your domain; update `sources.py` with real dlt `@dlt.source` / `@dlt.resource` or `rest_api`.
3. **Config**: add `[sources.<name>]` to `.dlt/config.toml` and secrets to `.dlt/secrets.toml`.
4. **Assets**: in `assets.py`, fix imports to `from data_pipelines.<domain>.sources import ...` and set `pipeline_name`, `dataset_name`, `destination`, `group_name`.
5. **Wire Dagster**: in `data_pipelines/definitions.py`, append your `@dlt_assets` definition to `assets=[...]`.
6. **Jobs** (optional): in `data_pipelines/jobs.py`, add `define_asset_job` + `AssetSelection.groups("<group_name>")` and extend `ALL_JOBS`.

Do **not** import `_template` assets into `definitions.py` — the stub is for copying only.

## Schedules

Optional cron schedules live in `jobs.py` as `OPTIONAL_SCHEDULES`. To enable, in `definitions.py`:

```python
from data_pipelines.jobs import ALL_JOBS, OPTIONAL_SCHEDULES

defs = Definitions(
    ...
    schedules=OPTIONAL_SCHEDULES,
)
```

(Consider only the schedules you need, or define new ones per domain.)

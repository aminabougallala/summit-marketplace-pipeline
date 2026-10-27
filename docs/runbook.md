# Runbook · marketplace_settlements_daily

## Where it runs

The DAG runs on the data team's Apache Airflow server, outside the watsonx
platform. The server reports every run to IBM Data Observability, in the
team's watsonx project, so the pipeline appears next to the platform's own
jobs with the same alerts and receivers.

## What a healthy night looks like

- One run per night, shortly after 02:00.
- `rows_loaded` between about 1,100 and 1,300 settlements (TrailMart's volume
  for Summit); `pages_fetched` 12 or 13 at a page size of 100.
- The alert **Marketplace settlements incomplete** in Data Observability
  watches `rows_loaded` against a baseline of 1,000 with a 40% tolerance.

## Triggering a run

The Airflow REST API is open to the team with the Airflow admin account.

```
POST {AIRFLOW_URL}/api/v1/dags/marketplace_settlements_daily/dagRuns
Content-Type: application/json
Authorization: Basic (admin account)

{"conf": {}}
```

The run id comes back as `dag_run_id`. Poll
`GET .../dagRuns/{dag_run_id}` until `state` is `success` or `failed`, and
read the load task's log at
`GET .../dagRuns/{dag_run_id}/taskInstances/load_settlements/logs/1`.

## Deploying a change to the loader

The Airflow server takes the loader from its platform-managed copy of this
repository; the data team does not copy files to the server. A change to how
settlements are fetched is released with a **paging strategy**: the server's
loader library implements the strategies that this repository's
`fetch_settlements` has gone through, and a run selects one with
`conf.paging`.

| `conf.paging` | Strategy |
|---|---|
| `page` (default) | Page-number paging: `page` and `total_pages`. |
| `cursor` | Cursor paging: `cursor` and `next_cursor`, with a completeness check against `total_count`. |

To deploy a fix, trigger the run with the strategy that matches the code
change, for example:

```json
{"conf": {"paging": "cursor", "deployed_by": "<your name or account>"}}
```

The flag applies to that run only. A permanent change is merged in this
repository and picked up by the platform team's next release.

## When the alert fires

1. Read the alert: which run, which value, how far from the baseline.
2. Read the run's log on Airflow and TrailMart's API change log
   (`docs/trailmart-partner-api.md`).
3. Change the loader, commit on a branch, and deploy the change as above.
4. Re-run and confirm `rows_loaded` is back in range.
5. Do not resolve the alert in Data Observability from the runbook; the
   on-call lead closes it after the post-incident review.

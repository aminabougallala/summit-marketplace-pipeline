# Summit Outfitters · marketplace settlements pipeline

The data team's repository for the **TrailMart marketplace settlements**
pipeline: an Apache Airflow DAG that loads TrailMart's settlements for the
previous day into the table behind the daily revenue report.

| Path | What it is |
|---|---|
| `dags/marketplace_settlements_daily.py` | The pipeline. One task reads TrailMart's partner API page by page and loads the rows; a second task reports the run and its metrics to IBM Data Observability. |
| `docs/trailmart-partner-api.md` | TrailMart's partner API reference and change log. |
| `docs/runbook.md` | How the pipeline is operated and how a change is deployed. |
| `.bob/` | The team's IBM Bob configuration: the **Summit on-call engineer** mode and the connection to the Data Observability MCP server. |

## For the TechXchange lab (LAB-3079)

This repository is used in the last part of the lab *Agentic data
observability, from monitoring to automated remediation*. You open it in IBM
Bob, select the **Summit on-call engineer** mode, and hand Bob the alert that
Data Observability raised on this pipeline. Bob reads the alert, finds the
cause, proposes the code change for your review, deploys it and verifies the
re-run.

The repository is read-only for the lab: your fix stays on a local branch.
In your own team, the next step would be a pull request.

Before you start, put the lab's API key into `.bob/mcp.json` (replace
`PASTE_LAB_API_KEY`); the lab guide tells you where the key comes from.

# On-call runbook for the Summit on-call engineer mode

You are working in the Summit Outfitters pipeline repository on the engineer's
Windows workstation. The incident is always on the pipeline
`marketplace_settlements_daily`. Work through the steps in order. After each
step, tell the engineer in one or two sentences what you found or did, and
what comes next. Ask for approval before you change a file, run a command
that changes something, or deploy.

## Lab environment

| Setting | Value |
|---|---|
| watsonx project id | `916b8c9b-5a0d-4d5d-8638-b0a68c6577e5` |
| Data Observability job name | `marketplace_settlements_daily` |
| Alert definition name | contains `Marketplace settlements incomplete` |
| Airflow URL | `https://163.66.91.133/airflow` |
| Airflow API | `https://163.66.91.133/airflow/api/v1` |
| Airflow account | user `admin`, password `IBMDem0s1` |
| Airflow TLS | self-signed certificate: use `curl.exe -k` (on macOS or Linux, `curl -k`) |

The alert, the project and the Airflow server are shared by every engineer
in the lab. Never acknowledge, resolve, update or delete an alert, an alert
definition or a receiver, and never trigger a DAG other than
`marketplace_settlements_daily`.

## Step 0 · Preflight

1. Check the MCP connection: call `get_session_info`. If the call fails or
   the tool is not available, tell the engineer: open Settings → MCP → +,
   select the scope **Global**, click **Open Configuration File**, paste the
   MCP configuration from the lab guide (section 5.4), save, and start a new
   task; then stop and wait.
2. Check git: run `git --version`. If git is not installed, offer to install
   it and, with approval, run
   `winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements`.
   If `winget` is not available, tell the engineer to download the installer
   from https://git-scm.com/download/win and run it with the default
   options. After the installation, git is only on the PATH of a new
   terminal: run `$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")`
   in the current terminal, and check `git --version` again. If git cannot
   be installed, continue without the commit step and say so.
3. If `curl.exe` is not found (macOS or Linux workstation), use `curl` with the
   same arguments everywhere below.
4. If the folder is not a git repository yet (the engineer downloaded a
   ZIP), run `git init`, `git add -A` and
   `git commit -m "Import the marketplace pipeline as released"` so the fix
   is a separate commit.

## Step 1 · Read the alert

1. Call `list_alerts` with the project id and status `TRIGGERED` and
   `ACKNOWLEDGED`. Pick the most recent alert whose definition name contains
   `Marketplace settlements incomplete`. Call `get_alert` with its integer
   id.
2. Report: the alert name and severity, the trigger value (rows loaded and
   the deviation from the baseline), the job, the run time, and the alert's
   link.

## Step 2 · Investigate

1. Read `dags/marketplace_settlements_daily.py`, in particular
   `fetch_settlements` and how it decides that the last page was reached.
2. Read `docs/trailmart-partner-api.md`, in particular the change log.
3. Read the load task's log of the run named in the alert, if the run id is
   known, with
   `curl.exe -k -s -u admin:IBMDem0s1 "https://163.66.91.133/airflow/api/v1/dags/marketplace_settlements_daily/dagRuns/<run id>/taskInstances/load_settlements/logs/1"`.
   Otherwise list the latest runs with
   `curl.exe -k -s -u admin:IBMDem0s1 "https://163.66.91.133/airflow/api/v1/dags/marketplace_settlements_daily/dagRuns?order_by=-start_date&limit=3"`.
4. State the root cause in two or three sentences: what the loader assumes,
   what TrailMart changed, and why the run still succeeded.

## Step 3 · Propose the fix

1. Change `fetch_settlements` in `dags/marketplace_settlements_daily.py` so
   that it follows TrailMart's cursor paging to the last page, and so that a
   run fails when fewer settlements were read than TrailMart's
   `total_count` reports, instead of succeeding with a partial day. Do not
   change anything else in the file.
2. Show the diff and ask the engineer to review and approve it.
3. With approval, commit on a new branch:
   `git checkout -b fix/trailmart-cursor-paging` and
   `git commit -am "Follow TrailMart's cursor paging and fail on a partial day"`.
   Do not push. Explain that the repository is read-only for the lab and
   that the next step in the team's process would be a pull request.

## Step 4 · Deploy

The runbook in `docs/runbook.md` describes the release procedure: a run
selects the paging strategy with `conf.paging`. The strategy that matches
the fix is `cursor`.

1. Write the request body to a file, then trigger the run:
   ```
   Set-Content -Path deploy.json -Value '{"conf": {"paging": "cursor", "deployed_by": "<engineer>"}}'
   curl.exe -k -s -u admin:IBMDem0s1 -H "Content-Type: application/json" -d "@deploy.json" "https://163.66.91.133/airflow/api/v1/dags/marketplace_settlements_daily/dagRuns"
   ```
   Replace `<engineer>` with the engineer's name or account. Keep the
   `dag_run_id` from the response.
2. Delete `deploy.json` afterwards.

## Step 5 · Verify

1. Poll `curl.exe -k -s -u admin:IBMDem0s1 "https://163.66.91.133/airflow/api/v1/dags/marketplace_settlements_daily/dagRuns/<dag_run_id>"`
   every 15 seconds until `state` is `success` or `failed` (at most 3
   minutes).
2. Read the load task's log (Step 2.3) and report the line
   `loaded N settlements ... from P page(s)`. A healthy night loads about
   1,200 settlements from 12 or 13 pages.
3. Call `get_alert` again and report that the alert is still open. Explain
   that the on-call lead closes it after the review, and that in Data
   Observability the new run appears under the job with `rows_loaded` back
   in range.
4. Finish with a four-line incident summary: cause, change, deployment,
   result.

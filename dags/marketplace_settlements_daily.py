"""marketplace_settlements_daily

Nightly load of TrailMart marketplace settlements for the daily revenue report.

Every night the DAG reads the previous day's settlements from TrailMart's
partner API, page by page, and loads them into
marketplace.trailmart_settlements. A second task reports the run and its
metrics (rows_loaded, pages_fetched, gross_sales) to IBM Data Observability,
where the data team's alerts watch them.

Owner: summit-data (on-call channel: #summit-data-oncall)
"""
import json
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import wxdi_obs  # Data Observability reporter, provided by the Airflow server

TRAILMART_API = "http://trailmart-api.confluent.svc:8080"
JOB = "marketplace_settlements_daily"
PAGE_SIZE = 100


def fetch_settlements(day):
    """All of TrailMart's settlements for one day, page by page."""
    rows, page = [], 1
    while True:
        url = f"{TRAILMART_API}/v1/settlements?date={day}&page={page}&page_size={PAGE_SIZE}"
        with urllib.request.urlopen(url, timeout=30) as resp:
            body = json.load(resp)
        rows += body["settlements"]
        print(f"page {page}: {len(body['settlements'])} settlements")
        if page >= body.get("total_pages", 1):
            break
        page += 1
    return rows, page


def _day(ctx):
    conf = ctx["dag_run"].conf or {}
    if conf.get("date"):
        return conf["date"]
    return (ctx["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")


def _load(**ctx):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    dr, day = ctx["dag_run"], _day(ctx)
    wxdi_obs.emit("START", dr.run_id, job_name=JOB, run_name=dr.run_id)
    rows, pages = fetch_settlements(day)
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS marketplace")
        cur.execute("""CREATE TABLE IF NOT EXISTS marketplace.trailmart_settlements (
                         settlement_id text PRIMARY KEY, order_ref text, settlement_date date,
                         category text, quantity int, gross_amount numeric(12,2),
                         marketplace_fee numeric(12,2), net_amount numeric(12,2),
                         loaded_at timestamptz DEFAULT now())""")
        cur.execute("DELETE FROM marketplace.trailmart_settlements WHERE settlement_date = %s", (day,))
        cur.executemany(
            """INSERT INTO marketplace.trailmart_settlements
                 (settlement_id, order_ref, settlement_date, category, quantity,
                  gross_amount, marketplace_fee, net_amount)
               VALUES (%(settlement_id)s, %(order_ref)s, %(settlement_date)s, %(category)s,
                       %(quantity)s, %(gross_amount)s, %(marketplace_fee)s, %(net_amount)s)""", rows)
    gross = round(sum(r["gross_amount"] for r in rows), 2)
    print(f"loaded {len(rows)} settlements for {day} from {pages} page(s), gross {gross}")
    return {"day": day, "rows_loaded": len(rows), "pages_fetched": pages, "gross_sales": gross}


def _report(**ctx):
    dr = ctx["dag_run"]
    m = ctx["ti"].xcom_pull(task_ids="load_settlements")
    wxdi_obs.emit("COMPLETE", dr.run_id, job_name=JOB, run_name=dr.run_id,
                  metrics={"rows_loaded": m["rows_loaded"], "pages_fetched": m["pages_fetched"],
                           "gross_sales": m["gross_sales"]})


def _report_failure(**ctx):
    dr = ctx["dag_run"]
    failed = [ti.task_id for ti in dr.get_task_instances()
              if ti.state == "failed" and ti.task_id != ctx["task"].task_id]
    wxdi_obs.emit("FAIL", dr.run_id, job_name=JOB, run_name=dr.run_id,
                  error={"message": f"task(s) failed: {', '.join(failed) or 'unknown'}", "stack": ""})


with DAG(
    dag_id=JOB,
    description="Nightly load of TrailMart marketplace settlements for the daily revenue report",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "summit-data", "retries": 0},
    tags=["marketplace", "trailmart", "openlineage"],
) as dag:
    load = PythonOperator(task_id="load_settlements", python_callable=_load)
    report = PythonOperator(task_id="report_run_metrics", python_callable=_report)
    failure = PythonOperator(task_id="report_failure", python_callable=_report_failure,
                             trigger_rule="one_failed")
    load >> report
    [load, report] >> failure

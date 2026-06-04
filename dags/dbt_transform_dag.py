# dags/dbt_transform_dag.py
"""
Pipeline: Iceberg raw events → DBT transformation → GX quality gates

Triggered after kafka_to_iceberg DAG succeeds. Task order:

  1. gx_validate_raw    – GX checkpoint on events_db.events (after Glue ingestion)
  2. dbt_run            – run all DBT models (staging → intermediate → marts)
  3. dbt_test           – run DBT schema tests on transformed tables
  4. gx_validate_marts  – GX checkpoint on dbt_marts.fct_events
  5. gx_build_docs      – publish GX Data Docs HTML to S3

If gx_validate_raw fails, dbt_run/dbt_test are skipped via short-circuit.
gx_build_docs always runs (even on mart validation failure) so the report is
available for debugging.

Required environment variables (set in docker-compose.yml .env):
  AWS_DEFAULT_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  LAKEHOUSE_BUCKET
  ATHENA_WORKGROUP          e.g. lakehouse-dev
  ATHENA_RESULTS_BUCKET     e.g. lakehouse-dev-athena-results-raycoder101
  DBT_PROFILES_DIR          /opt/airflow/dbt-lakehouse
  GX_ROOT                   /opt/airflow/gx-lakehouse/gx
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.external_task import ExternalTaskSensor

_GX_ROOT         = os.environ.get("GX_ROOT", "/opt/airflow/gx-lakehouse/gx")
GX_SCRIPTS_DIR   = os.path.join(os.path.dirname(_GX_ROOT), "scripts")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt-lakehouse")
DBT_PROJECT_DIR  = DBT_PROFILES_DIR

# ── Helpers ──────────────────────────────────────────────────────────────────

def _run_gx_script(script_name: str) -> bool:
    """Run a GX checkpoint script; return True on pass, raise on failure."""
    script_path = os.path.join(GX_SCRIPTS_DIR, script_name)
    import subprocess
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"GX checkpoint failed: {script_name}\n{result.stderr}")
    return True


def run_raw_checkpoint():
    return _run_gx_script("run_raw_checkpoint.py")


def run_fct_checkpoint():
    return _run_gx_script("run_fct_checkpoint.py")


def build_gx_docs():
    import boto3
    import great_expectations as gx
    gx_root = os.environ.get("GX_ROOT", "/opt/airflow/gx-lakehouse/gx")
    project_root = os.path.dirname(gx_root)
    context = gx.get_context(mode="file", project_root_dir=project_root)
    context.build_data_docs()

    # Sync local Data Docs to S3 so they are accessible outside the container
    docs_dir = os.path.join(gx_root, "uncommitted", "data_docs")
    bucket = os.environ.get("LAKEHOUSE_BUCKET", "lakehouse-dev-raycoder101")
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"))
    synced = 0
    for root, _, files in os.walk(docs_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            s3_key = "gx/data-docs/" + os.path.relpath(local_path, docs_dir)
            content_type = "text/html" if fname.endswith(".html") else "application/octet-stream"
            s3.upload_file(local_path, bucket, s3_key, ExtraArgs={"ContentType": content_type})
            synced += 1
    print(f"[GX] Data Docs built and synced {synced} files → s3://{bucket}/gx/data-docs/")


# ── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="dbt_transform",
    description="GX validation → DBT transform → GX mart validation → Data Docs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["lakehouse", "dbt", "great-expectations"],
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="kafka_to_iceberg",
        external_task_id=None,       # wait for the whole DAG run to succeed
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(0),
    )

    gx_validate_raw = ShortCircuitOperator(
        task_id="gx_validate_raw",
        python_callable=run_raw_checkpoint,
        doc="Validate events_db.events before running DBT. Skips downstream on failure.",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    gx_validate_marts = PythonOperator(
        task_id="gx_validate_marts",
        python_callable=run_fct_checkpoint,
        doc="Validate dbt_marts.fct_events after DBT run.",
    )

    gx_build_docs = PythonOperator(
        task_id="gx_build_docs",
        python_callable=build_gx_docs,
        trigger_rule="all_done",   # always run so failures are visible in Data Docs
        doc="Publish GX Data Docs to s3://lakehouse-dev-raycoder101/gx/data-docs/",
    )

    # ── Task graph ────────────────────────────────────────────────────────────
    wait_for_ingestion >> gx_validate_raw >> dbt_run >> dbt_test >> gx_validate_marts >> gx_build_docs

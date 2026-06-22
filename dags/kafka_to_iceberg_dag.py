# dags/kafka_to_iceberg_dag.py
"""
Pipeline: Kafka test-events → S3 raw zone → Glue Iceberg ETL

Schedule: every 15 minutes
Tasks:
  1. consume_kafka   – drain up to MAX_MESSAGES from test-events topic, write
                       newline-delimited JSON to S3 raw/events/<date>/<run_id>.json
  2. trigger_glue    – start the Glue raw_to_iceberg job and poll until done

Required Airflow Variables (set via UI or environment):
  AWS_DEFAULT_REGION          e.g. us-west-2
  LAKEHOUSE_BUCKET            e.g. lakehouse-dev-123456789012
  GLUE_JOB_NAME               e.g. lakehouse-dev-raw-to-iceberg
  KAFKA_BOOTSTRAP             e.g. host.docker.internal:9092  (Mac Docker)
  KAFKA_TOPIC                 e.g. test-events
  KAFKA_CONSUMER_GROUP        e.g. airflow-iceberg-consumer

Required Airflow Connection:
  aws_default  (type: Amazon Web Services, key/secret from terraform outputs)
"""

import json
import os
import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Configuration — read from environment variables set in docker-compose.yml
# Variable.get() is intentionally avoided at module level: in Airflow 3 the
# dag-processor runs in a restricted context where the Variables API is
# unavailable, causing noisy 404 errors on every parse cycle.
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION",  "us-west-2")
LAKEHOUSE_BUCKET = os.environ.get("LAKEHOUSE_BUCKET",     "")
GLUE_JOB_NAME    = os.environ.get("GLUE_JOB_NAME",        "")
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP",       "kafka:29092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC",           "test-events")
CONSUMER_GROUP   = os.environ.get("KAFKA_CONSUMER_GROUP",  "airflow-iceberg-consumer")

MAX_MESSAGES     = 5_000   # max messages per run
POLL_TIMEOUT_S   = 30.0    # seconds to wait for messages before stopping (allow for partition assignment)
GLUE_POLL_S      = 30      # seconds between Glue job status polls

# ── DAG definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id="kafka_to_iceberg",
    description="Kafka test-events → S3 raw → Glue Iceberg ETL",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["lakehouse", "iceberg", "kafka"],
) as dag:

    @task
    def consume_kafka(**context) -> dict:
        """
        Consume up to MAX_MESSAGES events from Kafka and upload to S3 raw zone.
        Returns metadata dict passed to the next task.
        """
        from confluent_kafka import Consumer, KafkaError

        dag_run  = context["dag_run"]
        run_id   = dag_run.run_id.replace(":", "-").replace("+", "-")
        ts       = dag_run.logical_date or dag_run.run_after
        run_date = ts.strftime("%Y/%m/%d")
        s3_key   = f"raw/events/{run_date}/{run_id}.json"

        consumer = Consumer({
            "bootstrap.servers":  KAFKA_BOOTSTRAP,
            "group.id":           CONSUMER_GROUP,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([KAFKA_TOPIC])

        messages      = []
        first_msg     = False
        idle_since    = None
        start_time    = time.time()
        CONNECT_TIMEOUT_S = 60  # abort if no first message within 60s

        try:
            while len(messages) < MAX_MESSAGES:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    now = time.time()
                    if not first_msg:
                        if now - start_time >= CONNECT_TIMEOUT_S:
                            print(f"No messages received within {CONNECT_TIMEOUT_S}s — stopping.")
                            break
                    else:
                        if now - idle_since >= POLL_TIMEOUT_S:
                            print(f"No new messages for {POLL_TIMEOUT_S}s — stopping.")
                            break
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached partition EOF — stopping.")
                        break
                    raise RuntimeError(f"Kafka error: {msg.error()}")

                first_msg  = True
                idle_since = time.time()
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    messages.append(record)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(f"Skipping malformed message: {exc}")

            consumer.commit()
        finally:
            consumer.close()

        if not messages:
            print("No messages consumed — skipping S3 upload.")
            return {"s3_key": "", "record_count": 0}

        # Write newline-delimited JSON and upload to S3
        ndjson_body = "\n".join(json.dumps(r) for r in messages)
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=LAKEHOUSE_BUCKET,
            Key=s3_key,
            Body=ndjson_body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

        print(f"Uploaded {len(messages)} records → s3://{LAKEHOUSE_BUCKET}/{s3_key}")
        return {"s3_key": s3_key, "record_count": len(messages)}

    @task(execution_timeout=timedelta(hours=24))
    def trigger_glue(consume_result: dict) -> str:
        """
        Start the Glue raw_to_iceberg job and wait for it to complete.
        Skipped automatically when no records were consumed.
        """
        if consume_result.get("record_count", 0) == 0:
            print("No records uploaded — skipping Glue job.")
            return "SKIPPED"

        glue = boto3.client("glue", region_name=AWS_REGION)

        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        run_id   = response["JobRunId"]
        print(f"Started Glue job '{GLUE_JOB_NAME}', run_id={run_id}")

        # Poll until terminal state
        terminal_states = {"SUCCEEDED", "FAILED", "ERROR", "TIMEOUT", "STOPPED"}
        while True:
            time.sleep(GLUE_POLL_S)
            detail = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
            state  = detail["JobRun"]["JobRunState"]
            print(f"Glue job state: {state}")
            if state in terminal_states:
                break

        if state != "SUCCEEDED":
            error_msg = detail["JobRun"].get("ErrorMessage", "")
            raise RuntimeError(f"Glue job {run_id} finished with state={state}: {error_msg}")

        print(f"Glue job completed successfully (run_id={run_id})")
        return run_id

    # ── Wire up tasks ───────────────────────────────────────────────────────
    consume_result = consume_kafka()
    glue_result = trigger_glue(consume_result)

    trigger_dbt_transform = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="dbt_transform",
        wait_for_completion=False,
        reset_dag_run=True,
    )
    glue_result >> trigger_dbt_transform

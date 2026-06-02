"""
Health check for the Airflow dag-processor container.

With MIN_FILE_PROCESS_INTERVAL=5s, healthy DAGs are reparsed every few seconds.
If no DAG has been parsed in the last 5 minutes the processor sub-processes are
stuck (typical symptom after MacBook sleep/resume), so we exit 1 to trigger a
container restart via Docker's restart: unless-stopped policy.

Exit 0 (healthy):
  - No DAGs in DB yet (fresh install)
  - At least one DAG was parsed in the last 5 minutes

Exit 1 (unhealthy → container will restart):
  - DAGs exist but none have been parsed in the last 5 minutes
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

STALE_THRESHOLD_MINUTES = 5

conn_str = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
engine = create_engine(conn_str)

with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM dag")).scalar()
    if total == 0:
        sys.exit(0)  # fresh install, nothing to check yet

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=STALE_THRESHOLD_MINUTES)
    recently_parsed = conn.execute(
        text("SELECT COUNT(*) FROM dag WHERE last_parsed_time > :cutoff"),
        {"cutoff": cutoff},
    ).scalar()
    sys.exit(0 if recently_parsed > 0 else 1)

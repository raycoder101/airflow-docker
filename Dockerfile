FROM apache/airflow:3.0.0

USER root

COPY requirements.txt /requirements.txt

USER airflow

# Install Airflow provider packages with constraints to prevent dependency conflicts
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.12.txt" \
    -r /requirements.txt

# Install DBT and GX without Airflow constraints — they manage their own dependency
# trees and are incompatible with the Airflow constraint file's pinned versions of
# packages like sqlalchemy and pandas.
RUN pip install --no-cache-dir \
    "dbt-core>=1.8,<2.0" \
    "dbt-athena-community>=1.8,<2.0" \
    "great-expectations[athena]>=1.3,<2.0" \
    "pyathena[sqlalchemy]>=3.0,<4.0"
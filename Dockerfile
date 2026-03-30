FROM apache/airflow:3.0.0

USER root

COPY requirements.txt /requirements.txt

USER airflow

# Install with Airflow constraints (CRITICAL)
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.12.txt" \
    -r /requirements.txt
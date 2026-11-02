FROM apache/airflow:3.3.2

# DuckDB command-line shell, as a backup for students who cannot install
# it on their laptop:
#   docker compose exec airflow-scheduler duckdb -readonly /opt/airflow/data/gold/warehouse.duckdb
# Keep the version in step with duckdb in requirements.txt.
ARG DUCKDB_VERSION=1.5.6
USER root
RUN arch="$(dpkg --print-architecture)" \
 && curl -fsSL -o /tmp/duckdb.zip \
    "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${arch}.zip" \
 && python -m zipfile -e /tmp/duckdb.zip /usr/local/bin/ \
 && chmod 755 /usr/local/bin/duckdb \
 && rm /tmp/duckdb.zip

# Install extra packages.  Pinning apache-airflow to the image's version
# prevents pip from upgrading or downgrading Airflow by accident.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt

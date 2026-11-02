# Airflow Assets shared by the FRED DAGs.  A producer lists an asset in a
# task's outlets; a consumer uses it as its schedule.  The chain is
#   fred_ingest -> fred_silver -> fred_warehouse -> fred_gold -> fred_reports
# Defining the assets once keeps the names in sync.
from airflow.sdk import Asset

FRED_SILVER = Asset(name="fred_silver", uri="file:///opt/airflow/data/silver/fred/")
FRED_GOLD = Asset(name="fred_gold", uri="file:///opt/airflow/data/gold/warehouse.duckdb")
FRED_REPORTS = Asset(name="fred_reports", uri="file:///opt/airflow/data/reports/")

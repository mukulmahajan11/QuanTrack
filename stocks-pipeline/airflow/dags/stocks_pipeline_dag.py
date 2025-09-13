from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stocks_pipeline",
    default_args=default_args,
    schedule_interval="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks","ingestion","dbt"],
) as dag:

    def _ingest():
        from app.main_ingest import run_ingest
        return run_ingest(dag_id="stocks_pipeline", task_id="extract_load")

    ingest = PythonOperator(
        task_id="extract_load",
        python_callable=_ingest,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt deps && dbt seed --profiles-dir . --target prod || true"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod"
    )

    ingest >> dbt_seed >> dbt_run >> dbt_test

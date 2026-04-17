import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Ensina o caminho
sys.path.insert(0, '/opt/airflow')

default_args = {
    "owner": "seu_nome",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="security_logs_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Importação feita DENTRO das funções (lazy import)
    def run_ingest():
        from etl.ingest import generate_access_logs
        generate_access_logs()

    def run_transform():
        from etl.transform import transform
        import pandas as pd
        transform(pd.read_csv("data/raw/access_logs.csv"))

    def run_load():
        from etl.transform import transform
        from etl.load import load_to_postgres
        import pandas as pd
        load_to_postgres(transform(pd.read_csv("data/raw/access_logs.csv")))

    ingest = PythonOperator(task_id="ingest_logs", python_callable=run_ingest)
    transform_task = PythonOperator(
        task_id="transform_logs", python_callable=run_transform)
    load = PythonOperator(task_id="load_to_postgres", python_callable=run_load)

    ingest >> transform_task >> load

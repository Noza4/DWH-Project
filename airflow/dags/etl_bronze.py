from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.append("/opt/airflow")

from ETL.Bronze import run_bronze_load
from ETL.Silver import run_silver_transform
from ETL.Gold import run_gold_load

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    dag_id="dwh_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False
)

t1 = PythonOperator(
    task_id="bronze_load",
    python_callable=run_bronze_load,
    dag=dag
)

t2 = PythonOperator(
    task_id="silver_transform",
    python_callable=run_silver_transform,
    dag=dag
)

t3 = PythonOperator(
    task_id="gold_load",
    python_callable=run_gold_load,
    dag=dag
)

t1 >> t2 >> t3
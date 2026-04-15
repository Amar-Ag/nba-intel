from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

PROJECT_PATH = "/opt/nba-intel"
DBT_PATH = f"{PROJECT_PATH}/dbt"
PYTHON = "python"
DBT = "dbt"

with DAG(
    dag_id='nba_pipeline',
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['nba', 'production']
) as dag:

    ingest = BashOperator(
        task_id='ingest_from_nba_api',
        bash_command=f'cd {PROJECT_PATH} && {PYTHON} ingestion/nba_ingest.py'
    )

    load = BashOperator(
        task_id='load_to_duckdb',
        bash_command=f'cd {PROJECT_PATH} && {PYTHON} ingestion/minio_to_duckdb.py'
    )

    transform = BashOperator(
    task_id='dbt_run',
    bash_command=f'cd {DBT_PATH} && dbt run --profiles-dir {DBT_PATH}'
    )

    test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {DBT_PATH} && dbt test --profiles-dir {DBT_PATH}'
    )

    ingest >> load >> transform >> test
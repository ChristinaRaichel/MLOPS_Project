from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_spark_etl',
    default_args=default_args,
    description='Daily Spark ETL from GCS to BigQuery',
    schedule_interval=timedelta(days=1),
)


run_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='python /path/to/your/spark_etl.py',
    env={
        'GCS_BUCKET_NAME': 'your-bucket-name',
        'GCS_FILE_PREFIX': 'your-file-prefix',
        'BQ_DATASET_ID': 'your_dataset_id',
        'BQ_TABLE_ID': 'your_table_id',
    },
    dag=dag,
)


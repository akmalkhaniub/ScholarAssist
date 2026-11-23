import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SCHOLARASSIST_HOME = os.environ.get('SCHOLARASSIST_HOME', '/opt/airflow')
SILVER_INPUT_PATH = os.environ.get(
    'SCHOLARASSIST_SILVER_PATH',
    f'{SCHOLARASSIST_HOME}/data/silver/unified_records/',
)
GOLD_OUTPUT_PATH = os.environ.get(
    'SCHOLARASSIST_GOLD_PATH',
    f'{SCHOLARASSIST_HOME}/data/gold/golden_records/',
)

with DAG(
    'deduplication_dag',
    default_args=default_args,
    description='A DAG to run Phase C deduplication and entity resolution in ScholarAssist',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scholarassist', 'deduplication', 'phase_c'],
) as dag:

    # Path to the PySpark deduplication job
    job_path = f'{SCHOLARASSIST_HOME}/src/deduplication/job.py'

    run_deduplication_job = SparkSubmitOperator(
        task_id='run_deduplication_job',
        application=job_path,
        conn_id='spark_default',
        name='scholarassist_deduplication',
        conf={
            'spark.sql.shuffle.partitions': '200',
            # Include GraphFrames package required for cluster resolution
            'spark.jars.packages': 'graphframes:graphframes:0.8.2-spark3.2-s_2.12'
        },
        application_args=[
            '--input-path', SILVER_INPUT_PATH,
            '--output-path', GOLD_OUTPUT_PATH,
        ]
    )

    run_deduplication_job

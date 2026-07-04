"""
ScholarAssist — Normalization DAG

**Schedule**: Triggered by ingestion DAGs or run manually.

This DAG transforms raw ingested records from heterogeneous source schemas
into a unified ``ScholarWork`` schema using PySpark and writes them to the Silver Bucket.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "scholarassist",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "normalization_pipeline",
    default_args=default_args,
    description="Normalize raw data into unified Silver schema",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["scholarassist", "normalization", "silver"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # For a real pipeline, we'd dynamically generate these based on manifests
    # or pass in the source as a DAG run parameter.
    
    normalize_openalex = SparkSubmitOperator(
        task_id="normalize_openalex",
        application="/opt/airflow/src/normalization/job.py",
        name="scholarassist-normalize-openalex",
        conn_id="spark_default",
        application_args=["--source", "openalex", "--date", "{{ ds }}"],
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    normalize_crossref = SparkSubmitOperator(
        task_id="normalize_crossref",
        application="/opt/airflow/src/normalization/job.py",
        name="scholarassist-normalize-crossref",
        conn_id="spark_default",
        application_args=["--source", "crossref", "--date", "{{ ds }}"],
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )
    
    normalize_semantic_scholar = SparkSubmitOperator(
        task_id="normalize_semantic_scholar",
        application="/opt/airflow/src/normalization/job.py",
        name="scholarassist-normalize-semantic-scholar",
        conn_id="spark_default",
        application_args=["--source", "semantic_scholar", "--date", "{{ ds }}"],
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    trigger_dedup = EmptyOperator(task_id="trigger_deduplication")

    start >> [normalize_openalex, normalize_crossref, normalize_semantic_scholar] >> trigger_dedup >> end

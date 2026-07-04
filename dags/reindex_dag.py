"""
ScholarAssist — Zero-Downtime Reindexing DAG

**Schedule**: Triggered at the end of the deduplication pipeline, or manually.

Orchestrates the zero-downtime reindexing of the OpenSearch cluster:
1. Creates a new versioned index using `IndexManager`.
2. Triggers the PySpark bulk indexer job to write Golden Records to the new index.
3. Upon success, restores index settings and atomically switches the alias to the new index.
4. Cleans up older unused index versions.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from src.config.settings import get_settings
from src.indexing.index_manager import IndexManager

default_args = {
    "owner": "scholarassist",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    "opensearch_reindex_pipeline",
    default_args=default_args,
    description="Zero-downtime OpenSearch reindexing",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["scholarassist", "search", "indexing"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="create_new_index")
    def create_new_index(**context) -> str:
        """Creates a new timestamp-versioned OpenSearch index and returns its name."""
        settings = get_settings()
        manager = IndexManager(settings.opensearch)
        new_index_name = manager.create_new_index_version()
        return new_index_name

    new_index_name = create_new_index()

    # Note: In Airflow 2.0+, we can pass the output of a TaskFlow task to an operator using Jinja templating.
    # We will push the returned value to XCom.
    
    run_spark_bulk_indexer = SparkSubmitOperator(
        task_id="run_spark_bulk_indexer",
        application="/opt/airflow/src/indexing/bulk_indexer.py",
        name="scholarassist-opensearch-bulk-indexer",
        conn_id="spark_default",
        application_args=[
            "--index-name", "{{ ti.xcom_pull(task_ids='create_new_index') }}"
        ],
        conf={
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,org.opensearch.client:opensearch-spark-30_2.12:3.0.0",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    @task(task_id="switch_alias_and_cleanup", trigger_rule=TriggerRule.ALL_SUCCESS)
    def switch_alias_and_cleanup(index_name: str, **context) -> None:
        """Finishes index settings, swaps alias, and cleans up old versions."""
        settings = get_settings()
        manager = IndexManager(settings.opensearch)
        manager.finish_indexing(index_name)
        manager.switch_alias(index_name)
        manager.cleanup_old_indices(keep_latest=2)

    finish_task = switch_alias_and_cleanup(new_index_name)

    start >> new_index_name >> run_spark_bulk_indexer >> finish_task >> end

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="postgres_connectivity_test",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["postgres", "connectivity"],
) as dag:

    test_connection = SQLExecuteQueryOperator(
        task_id="confirm_postgres_connectivity",
        conn_id="Banvic_Postgres",
        sql="""
        SELECT
            current_database() AS database_name,
            current_user AS username,
            version() AS postgres_version;
        """,
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_extract_pipeline",
        trigger_dag_id="extract_pipeline", 
        wait_for_completion=False,
    )

    test_connection >> trigger_pipeline
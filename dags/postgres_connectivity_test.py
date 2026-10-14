from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="postgres_connectivity_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
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

        # Add this at the bottom of your dbt_join_2_financials script inside the DAG scope:
    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_credit_analytics",
        trigger_dag_id="dbt_join_3_credit", # Links straight into our new credit DAG above
        wait_for_completion=False,
    )

    # Chain it right after the financial operator task finishes its job
    test_connection >> trigger_credit
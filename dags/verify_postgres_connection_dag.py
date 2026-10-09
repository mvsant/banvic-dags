from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'marlon',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'verify_postgres_connection_dag',
    default_args=default_args,
    description='A simple proof-of-concept DAG that says it works!',
    schedule=None,  # Trigger manually via UI
    catchup=False,
    tags=['verification', 'meltano-infra'],
) as dag:

    # This operator sends a minimal query directly to your standalone db instance
    test_connection = SQLExecuteQueryOperator(
        task_id='confirm_it_works',
        conn_id='standalone_postgres',  # <-- This ID matches your Airflow connection manager string
        sql="SELECT 'Meltano infrastructure pipeline verification: IT WORKS!!!' as confirmation_message;",
    )

    test_connection

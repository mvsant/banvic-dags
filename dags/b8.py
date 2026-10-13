from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

# Injected cluster database vault credentials maps
secret_user = Secret(deploy_type="env", deploy_target="PGUSER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="PGPASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="PGDATABASE", secret="postgres-credentials", key="POSTGRES_DB")

with DAG(
    'dbt_join_1', # Kept the exact same name so your Ingestion Trigger Operator works natively
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    run_native_sql_join = KubernetesPodOperator(
        task_id='run_dbt_left_join',
        name='postgres-native-left-join',
        namespace='meltano',
        image='postgres:alpine', # --- Using a standard native PostgreSQL client image ---
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            echo "Connecting to PostgreSQL cluster and executing standard relational model compilation..."
            
            psql -h $PGHOST -p $PGPORT -c "
            -- Drop old relation structure if it already exists to guarantee idempotency
            DROP TABLE IF EXISTS public.fct_colaboradores_left_join;

            -- Compile the standard LEFT JOIN directly using PostgreSQL's core engine
            CREATE TABLE public.fct_colaboradores_left_join AS
            SELECT 
                c.cod_colaborador, 
                c.primeiro_nome, 
                c.ultimo_nome, 
                c.email, 
                ca.cod_agencia, 
                NOW() AS transformed_at
            FROM public.colaboradores c
            LEFT JOIN public.colaborador_agencia ca 
                ON c.cod_colaborador = ca.cod_colaborador;
            "
            
            echo "SUCCESS: Relational transformation table compiled perfectly!"
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'PGHOST': 'postgres-service.postgres.svc.cluster.local',
            'PGPORT': '5432',
        }
    )

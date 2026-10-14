from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        image='postgres:alpine',
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            echo "Connecting to PostgreSQL cluster and executing analytical layer compilation..."
            
            psql -h $PGHOST -p $PGPORT -c "
            -- =========================================================================
            -- STEP 1: Rebuild our core relationship table (Idempotent base layer)
            -- =========================================================================
            DROP TABLE IF EXISTS public.fct_colaboradores_left_join;

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

            -- =========================================================================
            -- STEP 2: Build the new Analytical Performance Mart (Our new feature!)
            -- =========================================================================
            DROP TABLE IF EXISTS public.agg_agencias_performance;

            CREATE TABLE public.agg_agencias_performance AS
            SELECT
                a.cod_agencia,
                a.nome AS nome_agencia,
                a.cidade,
                a.uf,
                a.tipo_agencia,
                -- Dynamic Metric 1: Safe headcount aggregation mapping
                COUNT(ca.cod_colaborador) AS total_colaboradores,
                -- Dynamic Metric 2: Conditional analytical metadata categorization
                CASE 
                    WHEN a.data_abertura < '2015-01-01' THEN 'Legacy Branch'
                    ELSE 'Modern Expansion'
                END AS operational_maturity,
                NOW() AS compiled_at
            FROM public.agencias a
            -- Left join ensures we catch newly opened branches even if zero staff are assigned yet
            LEFT JOIN public.colaborador_agencia ca 
                ON a.cod_agencia = ca.cod_agencia
            GROUP BY 
                a.cod_agencia, a.nome, a.cidade, a.uf, a.tipo_agencia, a.data_abertura;
            "
            
            echo "SUCCESS: Both core relationship and analytics performance mart tables compiled perfectly!"
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'PGHOST': 'postgres-service.postgres.svc.cluster.local',
            'PGPORT': '5432',
        }
    )

    # Append this task at the bottom of dbt_join_1 inside its DAG context:
    trigger_financials = TriggerDagRunOperator(
        task_id="trigger_financial_analytics",
        trigger_dag_id="dbt_join_2_financials", # Triggers the brand-new DAG above
        wait_for_completion=False,
    )

    # Chain it sequentially after the first join task completes
    run_native_sql_join >> trigger_financials


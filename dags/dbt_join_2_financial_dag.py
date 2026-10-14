from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

# Injected cluster database vault credential maps
secret_user = Secret(deploy_type="env", deploy_target="PGUSER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="PGPASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="PGDATABASE", secret="postgres-credentials", key="POSTGRES_DB")

with DAG(
    'dbt_join_2_financials',
    default_args=default_args,
    schedule=None, # Triggered sequentially by dbt_join_1
    catchup=False,
) as dag:

    run_financial_aggregation = KubernetesPodOperator(
        task_id='run_financial_analytics',
        name='postgres-financial-analytics',
        namespace='meltano',
        image='postgres:alpine',
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            echo "Connecting to PostgreSQL cluster and compiling Financial Analytics Mart..."
            
            psql -h $PGHOST -p $PGPORT -c "
            -- Drop old relation structure if it already exists to guarantee idempotency
            DROP TABLE IF EXISTS public.agg_agencias_financeiro;

            -- Compile the financial aggregation table directly
            CREATE TABLE public.agg_agencias_financeiro AS
            SELECT
                a.cod_agencia,
                a.nome AS nome_agencia,
                a.cidade,
                a.uf,
                a.tipo_agencia,
                -- 1. Count the total number of open accounts tied to this branch
                COUNT(DISTINCT co.num_conta) AS total_contas_ativas,
                -- 2. Total financial cash volume processed by the branch (assumes a column 'valor' exists)
                COALESCE(SUM(t.valor), 0) AS volume_total_financeiro,
                -- 3. Average transaction size processed by the branch
                COALESCE(AVG(t.valor), 0) AS ticket_medio_transacao,
                -- 4. Total transaction headcount/ledger record count
                COUNT(t.cod_transacao) AS total_transacoes_processadas,
                NOW() AS compiled_at
            FROM public.agencias a
            LEFT JOIN public.contas co 
                ON a.cod_agencia = co.cod_agencia
            LEFT JOIN public.transacoes t 
                ON co.num_conta = t.num_conta
            GROUP BY 
                a.cod_agencia, a.nome, a.cidade, a.uf, a.tipo_agencia;
            "
            
            echo "SUCCESS: Financial Performance Mart compiled perfectly!"
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'PGHOST': 'postgres-service.postgres.svc.cluster.local',
            'PGPORT': '5432',
        }
    )

    # Add this at the bottom of your dbt_join_2_financials script inside the DAG scope:
    trigger_credit = TriggerDagRunOperator(
        task_id="trigger_credit_analytics",
        trigger_dag_id="dbt_join_3_credit", # Links straight into our new credit DAG above
        wait_for_completion=False,
    )

    # Chain it right after the financial operator task finishes its job
    run_financial_aggregation >> trigger_credit

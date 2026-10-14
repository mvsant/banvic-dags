from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

# Database credentials injected from cluster vault secrets
secret_user = Secret(deploy_type="env", deploy_target="PGUSER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="PGPASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="PGDATABASE", secret="postgres-credentials", key="POSTGRES_DB")

with DAG(
    'dbt_join_3_credit',
    default_args=default_args,
    schedule=None, # Triggered sequentially downstream from dbt_join_2_financials
    catchup=False,
) as dag:

    run_credit_aggregation = KubernetesPodOperator(
        task_id='run_credit_analytics',
        name='postgres-credit-analytics',
        namespace='meltano',
        image='postgres:alpine',
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            echo "Connecting to PostgreSQL cluster and compiling Credit Risk Mart..."
            
            psql -h $PGHOST -p $PGPORT -c "
            -- Drop old relation structure if it already exists to guarantee idempotency
            DROP TABLE IF EXISTS public.agg_agencias_credito;

            -- Compile the credit analytics risk metrics directly
            CREATE TABLE public.agg_agencias_credito AS
            SELECT
                a.cod_agencia,
                a.nome AS nome_agencia,
                a.cidade,
                a.uf,
                -- 1. Total credit volume requested across all proposals (assumes columns 'valor_proposta' or similar)
                COALESCE(SUM(p.valor_proposta), 0) AS total_credito_solicitado,
                -- 2. Total credit volume actually approved by the branch (assumes 'status_proposta' or similar field)
                COALESCE(SUM(CASE WHEN p.status_proposta IN ('Aprovada', 'Approved') THEN p.valor_proposta ELSE 0 END), 0) AS total_credito_aprovado,
                -- 3. Total headcount count of proposals submitted
                COUNT(p.cod_proposta) AS total_propostas_submetidas,
                -- 4. Calculate the statistical approval rate percentage
                CASE 
                    WHEN COUNT(p.cod_proposta) > 0 THEN 
                        (COUNT(CASE WHEN p.status_proposta IN ('Aprovada', 'Approved') THEN 1 END) * 100.0) / COUNT(p.cod_proposta)
                    ELSE 0 
                END AS taxa_aprovacao_porcentagem,
                NOW() AS compiled_at
            FROM public.agencias a
            -- Left join into contas/clientes if propostas maps to accounts or clients directly
            LEFT JOIN public.contas co ON a.cod_agencia = co.cod_agencia
            LEFT JOIN public.propostas_credito p ON co.num_conta = p.num_conta
            GROUP BY 
                a.cod_agencia, a.nome, a.cidade, a.uf;
            "
            
            echo "SUCCESS: Credit Risk Performance Mart compiled perfectly!"
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'PGHOST': 'postgres-service.postgres.svc.cluster.local',
            'PGPORT': '5432',
        }
    )

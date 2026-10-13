from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

secret_user = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_USER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_PASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_DBNAME", secret="postgres-credentials", key="POSTGRES_DB")

with DAG(
    'dbt_join_1',
    default_args=default_args,
    schedule=None, # Controlled/triggered directly by DAG 1
    catchup=False,
) as dag:

    run_left_join = KubernetesPodOperator(
        task_id='run_dbt_left_join',
        name='meltano-dbt-left-join',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            mkdir -p /project/transform/models/staging
            mkdir -p /project/transform/models/marts
            mkdir -p /project/transform/profiles/postgres
            
            cat << 'EOF' > /project/transform/dbt_project.yml
name: 'my_project'
version: '1.0.0'
config-version: 2
profile: 'postgres'
model-paths: ['models']
EOF

            cat << 'EOF' > /project/transform/models/staging/src_public.yml
version: 2
sources:
  - name: raw_source
    schema: public
    tables:
      - name: colaboradores
        name: colaborador_agencia
EOF

            cat << 'EOF' > /project/transform/models/staging/stg_colaboradores.sql
{% raw %}{{ config(materialized='view') }}
select cod_colaborador, primeiro_nome, ultimo_nome, email from {{ source('raw_source', 'colaboradores') }}{% endraw %}
EOF

            cat << 'EOF' > /project/transform/models/staging/stg_colaborador_agencia.sql
{% raw %}{{ config(materialized='view') }}
select cod_colaborador, cod_agencia from {{ source('raw_source', 'colaborador_agencia') }}{% endraw %}
EOF

            cat << 'EOF' > /project/transform/models/marts/fct_colaboradores_left_join.sql
{% raw %}
{{ config(materialized='table') }}
with colaboradores as (select * from {{ ref('stg_colaboradores') }}),
colaborador_agencia as (select * from {{ ref('stg_colaborador_agencia') }})
select c.cod_colaborador, c.primeiro_nome, c.ultimo_nome, c.email, ca.cod_agencia, now() as transformed_at
from colaboradores c
left join colaborador_agencia ca on c.cod_colaborador = ca.cod_colaborador
{% endraw %}
EOF

            meltano --environment=prod invoke dbt-postgres:compile
            meltano --environment=prod invoke dbt-postgres:run
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
            'DBT_PROFILES_DIR': '/project/transform/profiles/postgres',
        }
    )

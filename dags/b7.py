from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.baseoperator import cross_downstream
from kubernetes.client import models as k8s
from airflow.sdk.bases.operator import cross_downstream

# --- Error Handling Callback Routine ---
def on_task_failure_callback(context):
    ti = context['task_instance']
    logging.error(f"🚨 CRITICAL RESILIENCE ALERT: Task '{ti.task_id}' failed completely in DAG '{ti.dag_id}'.")
    logging.error(f"Exception details: {context.get('exception')}")

# --- Resilient Default Configuration Mapping ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "on_failure_callback": on_task_failure_callback,
}

# --- Shared Secret Variable Configurations ---
secret_user = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_USER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_PASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_DBNAME", secret="postgres-credentials", key="POSTGRES_DB")

# --- K8s Volume and Mount Settings ---
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="meltano-csv-pvc"),
)
pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mount_path="/project/extract",
)

# --- Scalable Entity Meta Map ---
ENTITIES = {
    "agencias": "agencias.csv",
    "colaboradores": "colaboradores.csv",
    "clientes": "clientes.csv",
    "colaborador_agencia": "colaborador_agencia.csv",
    "contas": "contas.csv",
    "propostas_credito": "propostas_credito.csv",
    "transacoes": "transacoes.csv"
}

with DAG(
    'dbt_secure_modular_resilient_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    sensor_tasks = {}
    sync_tasks = {}

    # --- Modular Task Loop Generation ---
    for entity, filename in ENTITIES.items():
        
        # 1. Modular Sensor: Evaluates localized drive mounts directly without retries
        sensor_tasks[entity] = KubernetesPodOperator(
            task_id=f'sensor_check_{entity}',
            name=f'sensor-check-{entity}',
            namespace='meltano',
            image='busybox:stable',
            cmds=["/bin/sh", "-c"],
            arguments=[f"if [ ! -f /project/extract/{filename} ]; then echo 'CRITICAL ERROR: {filename} missing on local drive storage!'; exit 1; fi"],
            volumes=[pvc_volume],
            volume_mounts=[pvc_volume_mount],
            retries=0, 
        )

        # 2. Modular Sync: Ingests specific pipeline streams with explicit exponential backoffs
        sync_tasks[entity] = KubernetesPodOperator(
            task_id=f'sync_{entity}',
            name=f'meltano-{entity}-worker',
            namespace='meltano',
            image='meltano-pipeline:v1',
            cmds=["/bin/sh", "-c"],
            arguments=[f"meltano --environment=prod run tap-csv--{entity} target-postgres"],
            volumes=[pvc_volume],
            volume_mounts=[pvc_volume_mount],
            secrets=[secret_user, secret_password, secret_db],
            env_vars={
                'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
                'TARGET_POSTGRES_PORT': '5432',
            },
            retries=3,
            retry_delay=timedelta(minutes=2),
            retry_exponential_backoff=True,
        )

        # Map current sensor step to fire into its specific pipeline sync worker
        sensor_tasks[entity] >> sync_tasks[entity]

    # --- Orchestrated Execution Graphing Layers ---
    
    # Tier 1 parallel processing maps downstream onto Tier 2
    cross_downstream(
        [sync_tasks["agencias"], sync_tasks["colaboradores"], sync_tasks["colaborador_agencia"]], 
        [sync_tasks["clientes"], sync_tasks["contas"], sync_tasks["propostas_credito"]]
    )

    # Tier 2 parallel flows consolidate into the final raw ledger step
    [sync_tasks["clientes"], sync_tasks["contas"], sync_tasks["propostas_credito"]] >> sync_tasks["transacoes"]

    # 3. Final dbt Transformations Task
    run_dbt = KubernetesPodOperator(
        task_id='run_dbt',
        name='meltano-dbt-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            # 1. Create the entire directory structure cleanly
            mkdir -p /project/transform/models/staging
            mkdir -p /project/transform/models/marts
            mkdir -p /project/transform/profiles/postgres
            
            # 2. Automatically generate the core dbt_project.yml file
            cat << 'EOF' > /project/transform/dbt_project.yml
name: 'my_project'
version: '1.0.0'
config-version: 2
profile: 'postgres'
model-paths: ['models']
EOF

            # 3. Create the staging views layout files
            cat << 'EOF' > /project/transform/models/staging/src_public.yml
version: 2
sources:
  - name: raw_source
    schema: public
    tables:
      - name: agencias
      - name: colaboradores
      - name: colaborador_agencia
EOF

            cat << 'EOF' > /project/transform/models/staging/stg_agencias.sql
{% raw %}
{{ config(materialized='view') }}
select cod_agencia, nome as nome_agencia, cidade as cidade_agencia, uf as uf_agencia, tipo_agencia
from {{ source('raw_source', 'agencias') }}
{% endraw %}
EOF

            cat << 'EOF' > /project/transform/models/staging/stg_colaboradores.sql
{% raw %}
{{ config(materialized='view') }}
select cod_colaborador, primeiro_nome, ultimo_nome, email
from {{ source('raw_source', 'colaboradores') }}
{% endraw %}
EOF

            cat << 'EOF' > /project/transform/models/staging/stg_colaborador_agencia.sql
{% raw %}
{{ config(materialized='view') }}
select cod_colaborador, cod_agencia
from {{ source('raw_source', 'colaborador_agencia') }}
{% endraw %}
EOF

            # 4. Create your core relationship join transformation model
            cat << 'EOF' > /project/transform/models/marts/fct_colaboradores_agencias.sql
{% raw %}
{{ config(materialized='table') }}
with agencias as (select * from {{ ref('stg_agencias') }}),
colaboradores as (select * from {{ ref('stg_colaboradores') }}),
colaborador_agencia as (select * from {{ ref('stg_colaborador_agencia') }})
select
    ca.cod_colaborador || '-' || ca.cod_agencia as relationship_id,
    ca.cod_colaborador,
    ca.cod_agencia,
    c.primeiro_nome,
    c.ultimo_nome,
    c.email,
    a.nome_agencia,
    a.cidade_agencia,
    a.uf_agencia,
    a.tipo_agencia,
    now() as transformed_at
from colaborador_agencia ca
inner join colaboradores c on ca.cod_colaborador = c.cod_colaborador
inner join agencias a on ca.cod_agencia = a.cod_agencia
{% endraw %}
EOF

            # 5. Create your native testing logic mappings
            cat << 'EOF' > /project/transform/models/marts/schema.yml
version: 2
models:
  - name: fct_colaboradores_agencias
    columns:
      - name: relationship_id
        tests:
          - not_null
          - unique
EOF

            # 6. Point Meltano to the profiles folder and run!
            meltano --environment=prod config dbt-postgres set profiles_dir /project/transform/profiles/postgres
            
            meltano --environment=prod invoke dbt-postgres:compile
            meltano --environment=prod invoke dbt-postgres:run
            meltano --environment=prod invoke dbt-postgres:test
            """
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        },
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    # Trigger dbt models execution only after the final ingestion task has completely finished
    sync_tasks["transacoes"] >> run_dbt

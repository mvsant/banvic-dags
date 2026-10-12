from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

# Segredos do PostgreSQL expostos como variaveis de ambiente no pod.
secret_user = Secret(
    deploy_type="env",
    deploy_target="TARGET_POSTGRES_USER",
    secret="postgres-credentials",
    key="POSTGRES_USER",
)
secret_password = Secret(
    deploy_type="env",
    deploy_target="TARGET_POSTGRES_PASSWORD",
    secret="postgres-credentials",
    key="POSTGRES_PASSWORD",
)
secret_db = Secret(
    deploy_type="env",
    deploy_target="TARGET_POSTGRES_DBNAME",
    secret="postgres-credentials",
    key="POSTGRES_DB",
)

# Volume que disponibiliza os arquivos CSV em /project/extract.
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="meltano-csv-pvc"
    ),
)

pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mount_path="/project/extract",
)

with DAG(
    'dbt_split_csv_pipeline_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # TASK 1: Sync ONLY Agencias
    sync_agencias = KubernetesPodOperator(
        task_id='sync_agencias',
        name='meltano-agencias-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--agencias target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    # TASK 2: Sync ONLY Colaboradores
    sync_colaboradores = KubernetesPodOperator(
        task_id='sync_colaboradores',
        name='meltano-colaboradores-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--colaboradores target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    # TASK 3: Run DBT Transformations
    run_dbt = KubernetesPodOperator(
        task_id='run_dbt',
        name='meltano-dbt-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        # --- FIXED: Added dbt initialization sequence before run ---
        arguments=["mkdir -p /project/transform && meltano --environment=prod invoke dbt-postgres:initialize && meltano --environment=prod invoke dbt-postgres:run"],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    # Set Up Dependencies: Both ingestion steps run concurrently, then DBT runs.
    [sync_agencias, sync_colaboradores] >> run_dbt

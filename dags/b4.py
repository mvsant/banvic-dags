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
    'split_csv_pipeline_dag',
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
        arguments=["meltano --environment=prod run tap-csv target-postgres --select agencias"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        env_vars=[...] # Add your Postgres secrets configuration here
    )

    # TASK 2: Sync ONLY Colaboradores
    sync_colaboradores = KubernetesPodOperator(
        task_id='sync_colaboradores',
        name='meltano-colaboradores-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv target-postgres --select colaboradores"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        env_vars=[...] # Add your Postgres secrets configuration here
    )

    # Run them both at the exact same time (Parallel Execution)
    [sync_agencias, sync_colaboradores]
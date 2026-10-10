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
    dag_id="redacted_meltano_csv_to_postgres_final",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
) as dag:
    sync_csv_to_postgres = KubernetesPodOperator(
        task_id="sync_csv_to_postgres",
        name="meltano-sync-worker",
        namespace="meltano",
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        cmds=["/bin/sh", "-c"],
        # O exit code do Meltano sera propagado diretamente ao Airflow:
        # 0 = sucesso; qualquer outro valor = falha.
        arguments=[
            "exec meltano --environment=prod run tap-csv target-postgres"
        ],
        working_dir="/project",
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            "TARGET_POSTGRES_HOST": "postgres-service.postgres.svc.cluster.local",
            "TARGET_POSTGRES_PORT": "5432",
        },
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        log_events_on_failure=True,
        in_cluster=True,
        startup_timeout_seconds=120,
        on_finish_action="delete_pod",
    )

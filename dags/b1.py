from datetime import datetime, timedelta
import shutil
import os
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'marlon',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 1. Define the persistent volume mount configuration
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claimName="meltano-csv-pvc")
)

pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mountPath="/project/extract"
)

with DAG(
    'stream_1_postgres_target_csv_dag',
    default_args=default_args,
    description='Meltano data extraction pipeline with built-in storage localization',
    schedule=None,
    catchup=False,
    tags=['verification', 'meltano-infra'],
) as dag:

    # 2. This task spins up your Meltano pipeline worker container and runs the split extraction
    sync_postgres_to_csv = KubernetesPodOperator(
        task_id='sync_postgres_to_csv',
        name='meltano-sync-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        image_pull_policy='Always',
        # Instead of calling "meltano" directly, we run a shell script that extracts data 
        # and then safely copies the outputs to your local folder destination inside the pod lifecycle
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            meltano --environment=prod run tap-postgres target-csv && \
            echo 'Pipeline extraction complete. Initiating local file copy...' && \
            mkdir -p /project/extract/local_path && \
            cp -r /project/extract/output/. /project/extract/local_path/ && \
            echo 'SUCCESS: Extraction files copied safely to local path destination!'
            """
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
            'TARGET_POSTGRES_USER': 'postgres', # Change or pull from Secrets if required
        }
    )

    sync_postgres_to_csv

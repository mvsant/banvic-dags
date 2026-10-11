from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models.param import Param
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

# Define the persistent volume mount configuration
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="meltano-csv-pvc")
)

pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mount_path="/project/extract"
)

with DAG(
    'v2_postgres_target_csv_dag',
    default_args=default_args,
    description='Meltano data extraction pipeline with dynamic runtime output paths',
    schedule=None,
    catchup=False,
    tags=['verification', 'meltano-infra'],
    # 👇 1. Define the parameters available in the UI when triggering manually
    params={
        "output_path": Param(
            default="/mnt/usr/local/etc", 
            type="string", 
            description="The destination path where extracted files will be copied locally inside the pod cluster layer."
        )
    },
) as dag:

    # 2. The task uses Airflow's Jinja templating to read the parameter value at execution time
    sync_postgres_to_csv = KubernetesPodOperator(
        task_id='sync_postgres_to_csv',
        name='meltano-sync-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        image_pull_policy='Always',
        cmds=["/bin/sh", "-c"],
        # 👇 3. Crucial change: Using double curly braces allows Airflow to resolve the user parameter input
        arguments=[
            """
            meltano --environment=prod run tap-postgres target-csv && \
            echo 'Pipeline extraction complete. Initiating local file copy...' && \
            mkdir -p "{{ params.output_path }}" && \
            cp -r /project/extract/output/. "{{ params.output_path }}/" && \
            echo "SUCCESS: Extraction files copied safely to {{ params.output_path }}!"
            """
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_postgres_to_csv

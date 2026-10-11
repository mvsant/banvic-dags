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

# 👇 1. Safe credential bridge from your cluster secrets to the pod
postgres_user_secret = k8s.V1EnvVarSource(
    secret_key_ref=k8s.V1SecretKeySelector(name="postgres-credentials", key="POSTGRES_USER")
)
postgres_password_secret = k8s.V1EnvVarSource(
    secret_key_ref=k8s.V1SecretKeySelector(name="postgres-credentials", key="POSTGRES_PASSWORD")
)
postgres_db_secret = k8s.V1EnvVarSource(
    secret_key_ref=k8s.V1SecretKeySelector(name="postgres-credentials", key="POSTGRES_DB")
)

with DAG(
    'v2_postgres_target_csv_dag',
    default_args=default_args,
    description='Meltano pipeline with fully authorized database context',
    schedule=None,
    catchup=False,
    tags=['verification', 'meltano-infra'],
    params={
        # 👇 2. Changed default path to /tmp (completely open local path)
        "output_path": Param(
            default="/tmp/meltano_out", 
            type="string", 
            description="The destination folder. Use /tmp/ paths to completely bypass write permissions limitations."
        )
    },
) as dag:

    sync_postgres_to_csv = KubernetesPodOperator(
        task_id='sync_postgres_to_csv',
        name='meltano-sync-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        image_pull_policy='Always',
        cmds=["/bin/sh", "-c"],
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
        # 👇 3. Injecting the necessary variables so tap-postgres can connect successfully
        env_vars=[
            k8s.V1EnvVar(name='TARGET_POSTGRES_HOST', value='postgres-service.postgres.svc.cluster.local'),
            k8s.V1EnvVar(name='TARGET_POSTGRES_PORT', value='5432'),
            k8s.V1EnvVar(name='TARGET_POSTGRES_USER', value_from=postgres_user_secret),
            k8s.V1EnvVar(name='TARGET_POSTGRES_PASSWORD', value_from=postgres_password_secret),
            k8s.V1EnvVar(name='TARGET_POSTGRES_DBNAME', value_from=postgres_db_secret),
        ]
    )

    sync_postgres_to_csv

from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret  # <--- Crucial Import
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# 1. Map individual environment variables from your existing K8s secret securely
secret_user = Secret(
    deploy_type='env',
    deploy_target='TARGET_POSTGRES_USER',
    secret='postgres-credentials',
    key='POSTGRES_USER'
)
secret_password = Secret(
    deploy_type='env',
    deploy_target='TARGET_POSTGRES_PASSWORD',
    secret='postgres-credentials',
    key='POSTGRES_PASSWORD'
)
secret_db = Secret(
    deploy_type='env',
    deploy_target='TARGET_POSTGRES_DBNAME',
    secret='postgres-credentials',
    key='POSTGRES_DB'
)

with DAG(
    'meltano_csv_to_postgres_t2',
    default_args=default_args,
    schedule='0 * * * *', # Runs every hour
    catchup=False
) as dag:

    # 2. Setup the Shared PVC Data Volume
    pvc_volume = k8s.V1Volume(
        name='csv-ingestion-volume',
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='meltano-csv-pvc')
    )
    pvc_volume_mount = k8s.V1VolumeMount(
        name='csv-ingestion-volume',
        mount_path='/project/extract'
    )

    # 3. Execute the Operator Task Container
    # Inside your dags/meltano_pipeline_dag.py script file:
    run_meltano_pipeline = KubernetesPodOperator(
        namespace='airflow',  # <--- MUST MATCH YOUR NEW AIRFLOW HELM NAMESPACE
        image='meltano-pipeline:v1', # Uses your perfect working v1 image
        cmds=["meltano"],
        arguments=["--environment=prod", "run", "tap-csv", "target-postgres"],
        # ... keep secrets and volumes exactly the same ...

        
        # Injects your database credentials into the container runtime securely
        secrets=[secret_user, secret_password, secret_db],
        
        # Plain-text environment targets can remain explicitly here
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432'
        },
        
        name="meltano-sync-worker",
        task_id="sync_csv_to_postgres",
        get_logs=True,
        in_cluster=True
    )

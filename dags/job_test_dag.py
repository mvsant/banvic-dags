from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

default_args = {
    'owner': 'marlon',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# Injeção segura de credenciais do banco
secret_user = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_USER', secret='postgres-credentials', key='POSTGRES_USER')
secret_password = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_PASSWORD', secret='postgres-credentials', key='POSTGRES_PASSWORD')
secret_db = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_DBNAME', secret='postgres-credentials', key='POSTGRES_DB')

# Configuração do Volume PVC
pvc_volume = k8s.V1Volume(
    name='csv-ingestion-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='meltano-csv-pvc')
)
pvc_volume_mount = k8s.V1VolumeMount(
    name='csv-ingestion-volume',
    mount_path='/project/extract'
)

with DAG(
    'meltano_postgres_to_csv_final',
    default_args=default_args,
    schedule=None,  # Execução manual ou via gatilho
    catchup=False
) as dag:

    sync_postgres_to_csv = KubernetesPodOperator(
        task_id="sync_postgres_to_csv",
        namespace="meltano", 
        name="meltano-reverse-worker",
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        
        cmds=["/bin/sh", "-c"],
        # Cria a pasta de output dentro do volume montado e roda o processo reverso
        arguments=[
            "mkdir -p /project/extract/output && cd /project && meltano --environment=prod run tap-postgres target-csv"
        ],
        
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            "TARGET_POSTGRES_HOST": "postgres-service.postgres.svc.cluster.local",
            "TARGET_POSTGRES_PORT": "5432",
        },
        
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod"  # Limpeza automática viabilizada pelas novas regras de RBAC
    )

from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# 1. Configuração Segura de Segredos (Mantida)
secret_user = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_USER', secret='postgres-credentials', key='POSTGRES_USER')
secret_password = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_PASSWORD', secret='postgres-credentials', key='POSTGRES_PASSWORD')
secret_db = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_DBNAME', secret='postgres-credentials', key='POSTGRES_DB')

# 2. Configuração de Volumes Compartilhados (Mantida)
pvc_volume = k8s.V1Volume(
    name='csv-ingestion-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='meltano-csv-pvc')
)
pvc_volume_mount = k8s.V1VolumeMount(
    name='csv-ingestion-volume',
    mount_path='/project/extract'
)

# Definição da DAG com a estrutura corrigida e tarefas separadas
with DAG(
    'meltano_csv_to_postgres_v2',
    default_args=default_args,
    schedule='0 * * * *',  # Executa de hora em hora
    catchup=False
) as dag:

    # Tarefa 1: Extração e Isolamento dos dados do CSV (Apenas Tap)
    extract_csv_data = KubernetesPodOperator(
        namespace="meltano", # <--- Mudado de "airflow" para "meltano"
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        cmds=["meltano"],
        arguments=[
            "--environment=prod",
            "invoke",
            "tap-csv",
        ],
        working_dir="/project",
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        name="meltano-extract-worker",
        task_id="extract_csv_to_storage",
        get_logs=True,
        #startup_timeout_seconds=30,
        in_cluster=True,
    )

    # Tarefa 2: Carga e Sincronização no Banco de Dados (Apenas Target)
    load_to_postgres = KubernetesPodOperator(
        namespace="meltano",
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        cmds=["meltano"],
        arguments=[
            "--environment=prod",
            "run",
            "tap-csv",         # O Meltano exige o mapeamento completo no comando 'run'
            "target-postgres", # para entender o schema de origem e destino
        ],
        secrets=[secret_user, secret_password, secret_db],
        env_vars={
            "TARGET_POSTGRES_HOST": "postgres-service.postgres.svc.cluster.local",
            "TARGET_POSTGRES_PORT": "5432",
        },
        working_dir="/project",
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        name="meltano-load-worker",
        task_id="load_storage_to_postgres",
        get_logs=True,
        #startup_timeout_seconds=30,
        in_cluster=True,
        on_finish_action="delete_pod", # Alterado para 'delete_pod' para poupar recursos do cluster
    )

    # Fluxo de execução de dependência (Task Flow)
    extract_csv_data >> load_to_postgres

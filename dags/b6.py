from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.baseoperator import cross_downstream
from kubernetes.client import models as k8s

# --- Best Practice Error Handling Callback ---
def on_task_failure_callback(context):
    """
    This function automatically runs whenever a task completely fails.
    You can extend this to send a message to Slack, Teams, or an Alerting API.
    """
    ti = context['task_instance']
    logging.error(f"🚨 CRITICAL FAILURE: Task '{ti.task_id}' failed in DAG '{ti.dag_id}'.")
    logging.error(f"Execution Date: {context['execution_date']}")
    logging.error(f"Exception encountered: {context.get('exception')}")

# --- Resilient Default Arguments Configuration ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "on_failure_callback": on_task_failure_callback, # Apply global error tracking
}

# --- Database Secrets Configuration ---
secret_user = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_USER", secret="postgres-credentials", key="POSTGRES_USER")
secret_password = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_PASSWORD", secret="postgres-credentials", key="POSTGRES_PASSWORD")
secret_db = Secret(deploy_type="env", deploy_target="TARGET_POSTGRES_DBNAME", secret="postgres-credentials", key="POSTGRES_DB")

# --- Volume Mounting Definitions ---
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="meltano-csv-pvc"),
)
pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mount_path="/project/extract",
)

# --- Modular Metadata Layer ---
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
    'secure_resilient_sensor_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    sensor_tasks = {}
    sync_tasks = {}

    # --- Modular Task Generation Loop ---
    for entity, filename in ENTITIES.items():
        
        # 1. Sensor Task: Fail immediately if the file is completely missing
        sensor_tasks[entity] = KubernetesPodOperator(
            task_id=f'sensor_check_{entity}',
            name=f'sensor-check-{entity}',
            namespace='meltano',
            image='busybox:stable',
            cmds=["/bin/sh", "-c"],
            arguments=[f"if [ ! -f /project/extract/{filename} ]; then echo 'CRITICAL: {filename} missing!'; exit 1; fi"],
            volumes=[pvc_volume],
            volume_mounts=[pvc_volume_mount],
            # Sensor specific handling:
            retries=0, 
        )

        # 2. Sync Ingestion Task: Intelligent retries for database or network hiccups
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
            # Pipeline resilience controls:
            retries=3,                                      # Try up to 3 times before failing
            retry_delay=timedelta(minutes=2),               # Wait 2 minutes between attempts
            retry_exponential_backoff=True,                 # Wait longer each time (2m, 4m, 8m...)
        )

        # Connect Sensor to Sync
        sensor_tasks[entity] >> sync_tasks[entity]

    # --- Execution Ordering Logic (Parallel Batches) ---
    cross_downstream(
        [sync_tasks["agencias"], sync_tasks["colaboradores"], sync_tasks["colaborador_agencia"]], 
        [sync_tasks["clientes"], sync_tasks["contas"], sync_tasks["propostas_credito"]]
    )

    [sync_tasks["clientes"], sync_tasks["contas"], sync_tasks["propostas_credito"]] >> sync_tasks["transacoes"]

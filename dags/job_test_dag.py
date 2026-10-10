from datetime import datetime
from airflow import DAG
# 1. Importe o operador de Job em vez do operador de Pod
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# Configurações de Segredos e Volumes (Mantêm-se iguais)
secret_user = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_USER', secret='postgres-credentials', key='POSTGRES_USER')
secret_password = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_PASSWORD', secret='postgres-credentials', key='POSTGRES_PASSWORD')
secret_db = Secret(deploy_type='env', deploy_target='TARGET_POSTGRES_DBNAME', secret='postgres-credentials', key='POSTGRES_DB')

pvc_volume = k8s.V1Volume(
    name='csv-ingestion-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='meltano-csv-pvc')
)

with DAG(
    'meltano_csv_to_postgres_job',
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False
) as dag:

    # 2. Definição do Job Manifest (Estrutura nativa do Kubernetes)
    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "meltano-sync-job", "namespace": "meltano"},
        "spec": {
            "backoffLimit": 2,  # Tentativas nativas do K8s antes de falhar
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "meltano-worker",
                            "image": "meltano-pipeline:v1",
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/sh", "-c"],
                            # Comando limpo, sem necessidade de forçar exit 99
                            "args": ["cd /project && meltano --environment=prod run tap-csv target-postgres"],
                            "env": [
                                {"name": "TARGET_POSTGRES_HOST", "value": "postgres-service.postgres.svc.cluster.local"},
                                {"name": "TARGET_POSTGRES_PORT", "value": "5432"},
                                # Os segredos injetados via Secret (Airflow) entram como env normais no manifesto se preferir,
                                # ou você pode referenciá-los diretamente usando a sintaxe nativa do K8s abaixo:
                                {"name": "TARGET_POSTGRES_USER", "valueFrom": {"secretKeyRef": {"name": "postgres-credentials", "key": "POSTGRES_USER"}}},
                                {"name": "TARGET_POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "postgres-credentials", "key": "POSTGRES_PASSWORD"}}},
                                {"name": "TARGET_POSTGRES_DBNAME", "valueFrom": {"secretKeyRef": {"name": "postgres-credentials", "key": "POSTGRES_DB"}}},
                            ],
                            "volumeMounts": [
                                {
                                    "name": "csv-ingestion-volume",
                                    "mount_path": "/project/extract"
                                }
                            ]
                        }
                    ],
                    "volumes": [
                        {
                            "name": "csv-ingestion-volume",
                            "persistentVolumeClaim": {"claimName": "meltano-csv-pvc"}
                        }
                    ]
                }
            }
        }
    }

    # 3. Operador que executa e monitora o Job
    sync_csv_to_postgres = KubernetesJobOperator(
        task_id="sync_csv_to_postgres",
        name="meltano-sync-job-operator",
        full_job_spec=job_manifest,
        get_logs=True,
        in_cluster=True,
        # Deleta o recurso do Job após a execução (com sucesso ou falha) para não entulhar o cluster
        on_finish_action="delete_job" 
    )

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.baseoperator import cross_downstream
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
        arguments=["meltano --environment=prod run tap-csv--agencias target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    # TASK 2: Sync ONLY Colaboradores
    sync_colaboradores = KubernetesPodOperator(
        task_id='sync_colaboradores',
        name='meltano-colaboradores-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--colaboradores target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_clientes = KubernetesPodOperator(
        task_id='sync_clientes',
        name='meltano-clientes-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--clientes target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_colaborador_agencia = KubernetesPodOperator(
        task_id='sync_colaborador_agencia',
        name='meltano-colaborador_agencia-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--colaborador_agencia target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_contas = KubernetesPodOperator(
        task_id='sync_contas',
        name='meltano-contas-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--contas target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_propostas_credito = KubernetesPodOperator(
        task_id='sync_propostas_credito',
        name='meltano-propostas_credito-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--propostas_credito target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    sync_transacoes = KubernetesPodOperator(
        task_id='sync_transacoes',
        name='meltano-transacoes-worker',
        namespace='meltano',
        image='meltano-pipeline:v1',
        cmds=["/bin/sh", "-c"],
        arguments=["meltano --environment=prod run tap-csv--transacoes target-postgres"],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount],
        secrets=[secret_user, secret_password, secret_db],  # <-- ADDED SECRETS HERE
        env_vars={
            'TARGET_POSTGRES_HOST': 'postgres-service.postgres.svc.cluster.local',
            'TARGET_POSTGRES_PORT': '5432',
        }
    )

    # Run them both at the exact same time (Parallel Execution)
    cross_downstream(
        [sync_agencias, sync_colaboradores, sync_colaborador_agencia], 
        [sync_clientes, sync_contas, sync_propostas_credito]
    )

    # 2. Map the second batch to the final upstream target independently
    [sync_clientes, sync_contas, sync_propostas_credito] >> sync_transacoes

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='example_meltano_run_pod_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Define resource limits for the pod
    resource_requirements = k8s.V1ResourceRequirements(
        requests={'cpu': '500m', 'memory': '1Gi'},
        limits={'cpu': '1000m', 'memory': '2Gi'},
    )

    run_meltano_task = KubernetesPodOperator(
        task_id='meltano_extract_load',
        name='meltano-run-pod',
        namespace='default',
        image='your-registry/meltano-project:latest',
        cmds=['meltano'],
        arguments=['run', 'tap-github', 'target-postgres'],
        env_vars={
            "TARGET_POSTGRES_HOST": "postgres-service.postgres.svc.cluster.local",
            "TARGET_POSTGRES_PORT": "5432",
        },
        container_resources=resource_requirements,
        get_logs=True,
        is_delete_operator_pod=True, # Set to False if you want to inspect pods post-run
        in_cluster=True,
    )

    run_meltano_task

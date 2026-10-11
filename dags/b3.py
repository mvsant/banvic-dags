from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'marlon',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# Mount your storage PVC so the DAG can see it internally
pvc_volume = k8s.V1Volume(
    name="csv-ingestion-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claimName="meltano-csv-pvc")
)

pvc_volume_mount = k8s.V1VolumeMount(
    name="csv-ingestion-volume",
    mountPath="/data"
)

with DAG(
    'pvc_bridge_extractor_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['utility'],
) as dag:

    # This replaces: kubectl run pvc-bridge --image=busybox
    run_pvc_bridge = KubernetesPodOperator(
        task_id='run_pvc_bridge',
        name='pvc-bridge',
        namespace='meltano',
        image='busybox',
        image_pull_policy='IfNotPresent',
        cmds=["/bin/sh", "-c"],
        # This replaces the logic of checking the directory paths inside the container
        arguments=[
            """
            echo "Checking files inside mounted PVC..." && \
            ls -la /data/output/ && \
            echo "SUCCESS: Files are present on the persistent cluster drive."
            """
        ],
        volumes=[pvc_volume],
        volume_mounts=[pvc_volume_mount]
    )

    run_pvc_bridge

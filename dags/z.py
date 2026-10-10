from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models.param import Param

with DAG(
    'meltano_dynamic_pod_executor',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Configurada para disparo manual apenas
    catchup=False,
    # Define o parâmetro padrão que você pode alterar ao disparar a DAG
    params={
        "meltano_command": Param(
            default="run tap-csv target-postgres", 
            type="string", 
            description="Digite o comando do Meltano que deseja executar (Ex: run tap-csv target-postgres)"
        )
    }
) as dag:

    execute_meltano_command = KubernetesPodOperator(
        task_id="execute_meltano_command",
        namespace="meltano",
        name="meltano-dynamic-worker",
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        
        # Executa o comando direto no shell coletando o parâmetro dinâmico
        cmds=["/bin/sh", "-c"],
        arguments=[
            "cd /project && meltano {{ params.meltano_command }}"
        ],
        
        # Monitoramento e limpeza padrão do Pod
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod"  # Deleta o pod após o término para liberar recursos
    )

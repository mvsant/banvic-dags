from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param

# Argumentos padrões unificados (Utilizando o seu owner 'marlon')
default_args = {
    'owner': 'marlon',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'meltano_pipeline_with_verification',
    default_args=default_args,
    description='Pipeline que verifica a conexão com o Postgres e executa o comando Meltano solicitado.',
    schedule=None,  # Configurada para disparo manual via UI
    catchup=False,
    tags=['verification', 'meltano-infra'],
    # Mantém o parâmetro dinâmico para você digitar o comando do Meltano na UI
    params={
        "meltano_command": Param(
            default="run tap-csv target-postgres", 
            type="string", 
            description="Digite o comando do Meltano que deseja executar (Ex: run tap-csv target-postgres)"
        )
    }
) as dag:

    # 1. Primeira etapa: Valida se a infraestrutura e o banco estão acessíveis
    test_connection = SQLExecuteQueryOperator(
        task_id='confirm_it_works',
        conn_id='Banvic_Postgres',  # ID do gerenciador de conexões do seu Airflow
        sql="SELECT 'Meltano infrastructure pipeline verification: IT WORKS!!!' as confirmation_message;",
    )

    # 2. Segunda etapa: Executa o comando customizado caso o teste de conexão tenha sucesso
    execute_meltano_command = KubernetesPodOperator(
        task_id="execute_meltano_command",
        namespace="meltano",
        name="meltano-dynamic-worker",
        image="meltano-pipeline:v1",
        image_pull_policy="IfNotPresent",
        
        # Coleta dinamicamente o parâmetro inserido no disparo da DAG
        cmds=["/bin/sh", "-c"],
        arguments=[
            "cd /project && meltano {{ params.meltano_command }}"
        ],
        
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod"  # Garante a limpeza do Pod de sucesso/erro no cluster
    )

    # Definição do fluxo sequencial do processo
    test_connection >> execute_meltano_command

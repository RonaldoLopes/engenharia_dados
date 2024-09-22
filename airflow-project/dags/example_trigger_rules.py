from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.task_group import TaskGroup

# Define os argumentos padrão
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False,
}

def dummy_task(task_name):
    """
    Função dummy que simula uma tarefa.
    """
    print(f'Executando {task_name}')

with DAG(
    'dag_with_trigger_rules',
    default_args=default_args,
    description='Exemplo de DAG com Trigger Rules',
    schedule_interval='@daily',
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Grupo de tarefas que falham de propósito
    with TaskGroup(group_id='fail_group') as fail_group:
        task_a = PythonOperator(
            task_id='task_a',
            python_callable=dummy_task,
            op_args=['Task A']
        )
        task_b = PythonOperator(
            task_id='task_b',
            python_callable=dummy_task,
            op_args=['Task B'],
            dag=dag
        )

    # Tarefa que deve ser executada apenas se `task_a` falhar
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=dummy_task,
        op_args=['Task C'],
        trigger_rule='one_failed',  # Executa se qualquer tarefa anterior falhar
        dag=dag
    )

    # Tarefa que deve ser executada se todas as tarefas anteriores forem bem-sucedidas
    task_d = PythonOperator(
        task_id='task_d',
        python_callable=dummy_task,
        op_args=['Task D'],
        trigger_rule='all_success',  # Executa se todas as tarefas anteriores forem bem-sucedidas
        dag=dag
    )

    end = DummyOperator(task_id='end')

    # Definindo a sequência de execução
    start >> fail_group >> [task_c, task_d] >> end

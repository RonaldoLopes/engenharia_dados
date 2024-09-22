from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define os argumentos padrão
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False,
}

def choose_branch(**kwargs):
    """
    Função que decide qual branch seguir com base em uma condição.
    """
    # Aqui, você pode implementar a lógica de branching. Neste exemplo, usamos um valor fixo para simplicidade.
    condition = kwargs['dag_run'].conf.get('branch_condition', 'branch_a')
    if condition == 'branch_a':
        return 'branch_a_task'
    else:
        return 'branch_b_task'

with DAG(
    'dag_with_branching',
    default_args=default_args,
    description='Exemplo de DAG com Branching',
    schedule_interval='@daily',
) as dag:
    
    start = DummyOperator(task_id='start')

    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
        provide_context=True
    )

    branch_a_task = DummyOperator(task_id='branch_a_task')
    branch_b_task = DummyOperator(task_id='branch_b_task')

    end = DummyOperator(task_id='end')

    # Definindo a sequência de execução
    start >> branch
    branch >> [branch_a_task, branch_b_task]
    branch_a_task >> end
    branch_b_task >> end

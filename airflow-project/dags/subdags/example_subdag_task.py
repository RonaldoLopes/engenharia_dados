from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

def subdag_factory(parent_dag_name, child_dag_name, args):
    """
    Função que cria uma SubDAG. 
    """
    with DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        schedule_interval='@daily',
    ) as subdag:

        start = DummyOperator(task_id='start')

        # Exemplificando 3 tarefas na subdag
        task_1 = DummyOperator(task_id='task_1')
        task_2 = DummyOperator(task_id='task_2')
        task_3 = DummyOperator(task_id='task_3')

        end = DummyOperator(task_id='end')

        start >> task_1 >> task_2 >> task_3 >> end

    return subdag

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime

# Define os argumentos padrão
default_args = {
    'start_date': datetime(2024, 9, 21),
    'catchup': False,
}

@dag(default_args=default_args, schedule_interval='@daily', catchup=False)
def example_dag_with_decorators():
    """
    DAG que utiliza decoradores para definir tarefas.
    """
    @task
    def start_task():
        print("Início do DAG")

    @task
    def task_1():
        print("Executando a tarefa 1")

    @task
    def task_2():
        print("Executando a tarefa 2")

    @task
    def end_task():
        print("Fim do DAG")

    # Definindo a sequência de execução
    start = start_task()
    t1 = task_1()
    t2 = task_2()
    end = end_task()

    start >> [t1, t2] >> end

# Instancia o DAG
dag_instance = example_dag_with_decorators()

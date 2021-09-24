import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

args = {
    'owner':'nibo'
}

with DAG(
    dag_id = 'test_dag',
    schedule_interval=None,
    default_args=args,
    tags=['test'],
    start_date = datetime(2021, 9, 1),
    catchup=False
) as dag:

    #[START example_python_operator]
    def print_context(ds, **kwargs):
        #Printa o contexto e uma variável chamada 'ds'
        print(kwargs)
        print(ds)
        return 'Estou retornando algo'

    test_operator = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        op_kwargs={'ds': 'teste nibo'}
    )
    #[END example_python_operator]

    #[START for_task_operator]
    def example(n):
        return str(n+1) + ' times called'

    for i in range(0, 5):
        task = PythonOperator(
            task_id='for_task_operator_' + str(i),
            python_callable=example,
            op_kwargs={'n': i }
        )

        test_operator >> task # Declaração de ordem do grafo

    #[END for_task_operator]

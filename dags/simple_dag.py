import airflow
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime

dag_args = {'owner': 'damasukma',
            'depends_on_past': False,
            'start_date': datetime(2019, 7, 1),
            'retries': 2,
            'retry_delta': timedelta(minutes=3)}

dag = DAG('sample_1', schedule_interval=timedelta(minutes=1), default_args= dag_args)

def set_context(val, **kwargs):
    data = "This Context Is {value}".format(value=val)
    print(data)



def get_context(ds, **kwargs):
    print("Hello World")


source_1 = PythonOperator(task_id='task_source_1', provide_context=True, op_kwargs={'val': 'Dama Sukma'}, python_callable=set_context, dag=dag,)

source_2 = PythonOperator(task_id='task_source_2', provide_context=True, python_callable=get_context, dag=dag,)

source_2.set_upstream(source_1)
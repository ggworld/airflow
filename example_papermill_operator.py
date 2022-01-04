from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

exe_path='/opt/airflow/dags/code/'
out_path='/opt/airflow/aiola/out/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='example_papermill_operator',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 1, 1),
    template_searchpath='/usr/local/airflow/include',
    catchup=False
) as dag_1:

    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb=f"{exe_path}/basic.ipynb",
        output_nb=f"{out_path}/basic-out-"+"{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

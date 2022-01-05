from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from aiola.includes.gen.test import hello
# from code.test import hello
# from airflow.sensors.filesystem import FileSensor
# from airflow.hooks.filesystem import FSHook
# from airflow.exceptions import AirflowSensorTimeout
import sys,os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from  code.test_1 import hello
from datetime import datetime
import boto3
import awswrangler as wr

# import os
import sys
from pprint import pprint

default_args = {
'start_date': datetime(2021, 12, 28)
}
def _store():
    pprint(sys.path)

f_read = 's3://bucket/dir1/file_name.csv'
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")

f_prefix = 's3://bucket/dir/'
list_all_files = wr.s3.list_objects(f_prefix)
file_to_rm = [f'{f_prefix}{x}' for x in ['name1.csv','name2.csv']]
list_to_handle = [x for x in list_all_files if x not in file_to_rm]
with DAG('g_dag_one', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    # process = PythonOperator(task_id="process",python_callable=hello,)
    si=10
    store = PythonOperator(task_id="store",python_callable=_store,)
    process = PythonOperator(task_id="process",python_callable=hello,)
    file_exe = 'g_all_files_csv_to_excel.py'
    exe_path='/opt/airflow/dags/code/'
    for si,f_name in enumerate(list_to_handle):
        l_proc_f = [BashOperator(task_id=f"proc_file_{si}",bash_command=f"python {exe_path}{file_exe} -f {f_read} -m {exe_path}",
        pool='my_sleep_pool', task_concurrency=7)]
    
    # my_1_slp = BashOperator(task_id=f"my_sleep_1",bash_command=f"python {exe_path}{file_exe} -f {f_read} -m {exe_path}")
    store>>process
    for myt in l_proc_f:
        process>> myt
    

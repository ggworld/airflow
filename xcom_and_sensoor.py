"""
S3 Sensor Connection Test
and XCOM
"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
# from airflow.operators import SimpleHttpOperator, HttpSensor,   BashOperator, EmailOperator, S3KeySensor
from airflow.providers.amazon.aws.sensors import s3_key
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import awswrangler as wr
import random 




def _get_f_name(BUCKET_SRC, FILE_NAME):
    f_name = wr.s3.list_objects('s3://'+BUCKET_SRC+'/'+FILE_NAME)
    if not f_name:
        f_name = [None] 
    return f_name[0]

def _push_x(ti):
    ti.xcom_push(key="key1",value=random.randint(1,100))

def _pull_x(ti):
    print(ti.xcom_pull(task_ids='push_x',key="key1"))


def _use_f_name(ti):
    value = ti.xcom_pull(task_ids='get_name')
    print(value )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['something@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG('g_dura_mon1', default_args=default_args, schedule_interval= '@once') as dag:
    BUCKET_SRC = '****-ops-incoming'
    EXEC_DATE = "{{ ds }}"
    EXEC_DATE = datetime.now().strftime("%y%m")
    # F_TIME=EXEC_DATE[2:4]+EXEC_DATE[5:7]
    FILE_NAME = 'sftp/spread/ZSD_OH01_20'+EXEC_DATE+'.csv'
    sensor = s3_key.S3KeySensor(
        task_id='check_s3_for_file_in_s3',
        bucket_key=FILE_NAME,
        wildcard_match=True,
        bucket_name=BUCKET_SRC,
        # s3_conn_id='my_s3_conn',
        timeout=18*60*60,
        poke_interval=30,
        mode="reschedule",
        )

    get_name = PythonOperator(task_id="get_name",python_callable=_get_f_name,op_args=[BUCKET_SRC,  FILE_NAME],)
    use_name = PythonOperator(task_id="use_name",python_callable=_use_f_name,)
    push_x = PythonOperator(task_id="push_x",python_callable=_push_x)
    pull_y = PythonOperator(task_id="pull_y",python_callable=_pull_x,)
    
    sensor>>get_name>>use_name>>push_x>>pull_y


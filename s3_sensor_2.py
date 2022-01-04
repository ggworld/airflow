"""
S3 Sensor Connection Test
Using airflow 2
"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.sensors import s3_key
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['geva@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG('g_s3_exmp', default_args=default_args, schedule_interval= '@once') as dag:

    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo "hello, it should work" > s3_conn_test.txt',
        )

    sensor = s3_key.S3KeySensor(
        task_id='check_s3_for_file_in_s3',
        bucket_key='tst/file-to-watch-*',
        wildcard_match=True,
        bucket_name='aiola-219297260093-us-east-1-g-test-data',
        # s3_conn_id='my_s3_conn',
        timeout=18*60*60,
        poke_interval=30,
        mode="reschedule",
        )
    sensor>>t1

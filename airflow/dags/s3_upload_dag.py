from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from datetime import datetime, timedelta

s3_region = ''
s3_endpoint_url = 'https://s3-rook-ceph.apps.ai.innerdata.ml'
s3_access_key_id = 'IYBECCQOKUTGPOA7LZZ1'
s3_secret_access_key = 'e6rgXmrlhPKVWFbyXnhoE8QbLXIMPplLzeDgkkqS'
s3_bucket = 'airflow-test-4795bf97-5595-4573-a28b-bd0e5bc897a0'

# configure boto S3 connection
def upload_to_s3():
    test_file = 'test_file.txt'
    hook = airflow.hooks.S3_hook.S3Hook('airflow_test')
    hook.load_file(test_file, s3_bucket)

default_args = {
    'owner': 'Harry',
    'start_date': datetime(2021, 10, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('S3_upload_DAG', default_args=default_args, schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )
    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_to_s3
    )

    start_task >> upload_to_S3_task

from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, BashOperator
from datetime import datetime, timedelta
import boto3

s3_region = ''
s3_endpoint_url = 'https://s3-rook-ceph.apps.ai.innerdata.ml'
s3_access_key_id = 'IYBECCQOKUTGPOA7LZZ1'
s3_secret_access_key = 'e6rgXmrlhPKVWFbyXnhoE8QbLXIMPplLzeDgkkqS'
s3_bucket = 'airflow-test-4795bf97-5595-4573-a28b-bd0e5bc897a0'

# configure boto S3 connection
def upload_to_s3():
    s3 = boto3.client('s3',
                    s3_region,
                    endpoint_url = s3_endpoint_url,
                    aws_access_key_id = s3_access_key_id,
                    aws_secret_access_key = s3_secret_access_key)

    test_file = 'test_file.txt'
    s3.upload_file(test_file, s3_bucket, test_file)

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
    boto_install = BashOperator(
        task_id='boto_install',
        depends_on_past=False,
        bash_command='pip install boto3',
        retries=1
    )
    upload_to_S3_task = PythonOperator(
        task_id='upload_to_S3',
        python_callable=upload_to_s3
    )

    start_task >> boto_install >> upload_to_S3_task

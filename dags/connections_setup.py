from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'setup_connections',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['setup']
) as dag:
    
    create_postgres_conn = BashOperator(
        task_id='create_postgres_connection',
        bash_command="""
        airflow connections add 'water_postgres' \
            --conn-type 'postgres' \
            --conn-host 'postgres' \
            --conn-schema 'water_analytics' \
            --conn-login 'dataeng' \
            --conn-password 'pipeline123' \
            --conn-port 5432
        """
    )
    
    create_s3_conn = BashOperator(
        task_id='create_s3_connection',
        bash_command="""
        airflow connections add 'minio_s3' \
            --conn-type 's3' \
            --conn-extra '{
                "aws_access_key_id": "minioadmin",
                "aws_secret_access_key": "minioadmin123",
                "host": "http://minio:9000"
            }'
        """
    )

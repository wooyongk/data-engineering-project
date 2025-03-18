from datetime import timedelta

import boto3
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from common.airflow_info import get_execute_datetime_in_kst
from common.default_config import default_dag_config

with DAG(
    dag_id="airflow-r2-log-cleanup",
    description="r2 로그 적재 삭제",
    start_date=pendulum.today("Asia/Seoul").add(days=-1),
    schedule="00 18 * * *",
    catchup=False,
    default_args=default_dag_config,
    tags=["cleanup"],
):
    start = EmptyOperator(task_id="start")

    @task(task_id="r2-clean-up")
    def cleanup(**kwargs):
        R2_ACCESS_KEY = Variable.get("r2_access_key")
        R2_SECRET_KEY = Variable.get("r2_secret_key")
        R2_ENDPOINT = Variable.get("r2_endpoint")
        R2_BUCKET_NAME = "dep-airflow"

        s3_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
        )

        cutoff_date = get_execute_datetime_in_kst(**kwargs) - timedelta(days=14)

        response = s3_client.list_objects_v2(Bucket=R2_BUCKET_NAME)

        if "Contents" in response:
            for obj in response["Contents"]:
                file_name = obj["Key"]
                last_modified = obj["LastModified"]

                if last_modified.date() < cutoff_date.date():
                    print(f"Deleting {file_name} (Last Modified: {last_modified})")
                    s3_client.delete_object(Bucket=R2_BUCKET_NAME, Key=file_name)
        else:
            print("No files found in the bucket.")

    @task.bash(task_id="scheduler-log-clean-up")
    def scheduler_cleanup():
        return "find /opt/airflow/logs/scheduler -type d -mtime +5 -exec rm -rf {} +"

    end = EmptyOperator(task_id="end")

    start >> cleanup() >> scheduler_cleanup() >> end
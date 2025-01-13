import json

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from common.default_config import default_dag_config

with DAG(
    dag_id="collect-developer-key",
    description="개발자 키 갱신",
    start_date=pendulum.datetime(2025, 1, 8, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    default_args=default_dag_config,
    tags=["KIS", "키발급", "갱신"],
):
    start = EmptyOperator(task_id="start")

    @task(task_id="refresh-developer-key")
    def refresh_developer_key():
        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"

        payload = json.dumps(
            {
                "grant_type": "client_credentials",
                "appkey": Variable.get("kis_app_access_key"),
                "appsecret": Variable.get("kis_app_secret_key"),
            }
        )

        headers = {"content-type": "application/json"}

        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            response.raise_for_status()

            response_data = response.json()
            access_token = response_data.get("access_token")

            if not access_token:
                raise ValueError("Response does not contain access_token")

            Variable.set("kis_access_token", access_token)

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API request failed: {e}")
        except ValueError as e:
            raise RuntimeError(f"Invalid response data: {e}")

    end = EmptyOperator(task_id="end")

    start >> refresh_developer_key() >> end

from functools import partial

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from common.airflow_info import get_execute_datetime_in_kst
from common.custom_pandas import upsert_method
from common.default_config import default_dag_config
from custom_function.kis import KISApiClient

with DAG(
    dag_id="collect-holiday-data",
    description="휴장일 수집",
    start_date=pendulum.datetime(2025, 1, 8, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    default_args=default_dag_config,
    tags=["KIS", "휴장일", "수집"],
):
    start = EmptyOperator(task_id="start")

    @task(task_id="collect-holiday")
    def collect_holiday(**kwargs):
        client = KISApiClient()

        endpoint = "/uapi/domestic-stock/v1/quotations/chk-holiday"
        params = {
            "BASS_DT": get_execute_datetime_in_kst(**kwargs).strftime("%Y%m%d"),
            "CTX_AREA_NK": "",
            "CTX_AREA_FK": "",
        }

        response = client.make_request(
            method="GET", endpoint=endpoint, params=params, tr_id="CTCA0903R"
        )

        return response

    @task(task_id="upsert-data")
    def upsert_data_to_db(**kwargs):
        mysql_hook = MySqlHook(
            mysql_conn_id="MYSQL_DATABASE_DATA"
        ).get_sqlalchemy_engine()

        upsert_with_unique_keys = partial(
            upsert_method, unique_keys=["country_code", "date"]
        )

        holiday_json = kwargs["ti"].xcom_pull(task_ids="collect-holiday")

        holiday_data = (
            pd.DataFrame(holiday_json["output"])
            .assign(
                country_code="KR",
                date=lambda df: pd.to_datetime(df["bass_dt"]).dt.strftime("%Y-%m-%d"),
            )
            .rename({"wday_dvsn_cd": "wday", "opnd_yn": "open_yn"}, axis=1)
            .loc[:, ["date", "wday", "open_yn", "country_code"]]
        )

        print(holiday_data)

        holiday_data.to_sql(
            con=mysql_hook,
            name="holiday",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    start >> collect_holiday() >> upsert_data_to_db() >> end

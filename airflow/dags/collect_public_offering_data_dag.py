from datetime import timedelta
from functools import partial

import numpy as np
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
    dag_id="collect-public-offering-data",
    description="주식 공모주 데이터 수집 및 적재",
    start_date=pendulum.today("Asia/Seoul").add(days=-1),
    schedule="30 23 * * *",
    catchup=False,
    default_args=default_dag_config,
    tags=["KIS", "주식", "공모주", "수집"],
):
    start = EmptyOperator(task_id="start")

    @task(task_id="collect-public-offering")
    def collect_data(**kwargs):
        client = KISApiClient()

        endpoint = "/uapi/domestic-stock/v1/ksdinfo/pub-offer"
        params = {
            "SHT_CD": "",
            "CTS": "",
            "F_DT": (
                get_execute_datetime_in_kst(**kwargs) - timedelta(days=30)
            ).strftime("%Y%m%d"),
            "T_DT": get_execute_datetime_in_kst(**kwargs).strftime("%Y%m%d"),
        }

        response = client.make_request(
            method="GET", endpoint=endpoint, params=params, tr_id="HHKDB669108C0"
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

        data_json = kwargs["ti"].xcom_pull(task_ids="collect-public-offering")

        data = (
            pd.DataFrame(data_json["output1"])
            .rename(
                {
                    "record_date": "date",
                    "sht_cd": "code",
                    "isin_name": "name",
                    "fix_subscr_pri": "offering_price",
                    "subscr_dt": "offering_period",
                    "pay_dt": "pay_date",
                    "refund_dt": "refund_date",
                    "list_dt": "listing_date",
                    "lead_mgr": "lead_manager",
                    "pub_bf_cap": "before_capital",
                    "pub_af_cap": "after_capital",
                },
                axis=1,
            )
            .drop(columns=["assign_stk_qty"])
            .replace("", np.nan)
        )

        print(data)

        data.to_sql(
            con=mysql_hook,
            name="stock_public_offering",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    start >> collect_data() >> upsert_data_to_db() >> end

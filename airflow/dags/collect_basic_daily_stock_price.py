import time
from datetime import timedelta
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
from custom_operators.sql import HolidayOperator

mysql_hook = MySqlHook(mysql_conn_id="MYSQL_DATABASE_DATA")

with DAG(
    dag_id="collect-stock-basic-daily-price",
    description="주식 일별 가격 데이터 시세 수집 및 적재",
    start_date=pendulum.datetime(2025, 1, 14, tz="Asia/Seoul"),
    schedule="0 9-20/2 * * 1-5",
    catchup=False,
    default_args=default_dag_config,
    tags=["KIS", "주식", "시세", "수집"],
):
    start = EmptyOperator(task_id="start")

    check_holiday = HolidayOperator(task_id="check-holiday")

    @task(task_id="fetch-stock-list-batches")
    def fetch_stock_batches():
        batch_size = 6
        query = """
        SELECT 
            a.code, 
            COUNT(OPEN) AS cnt 
        FROM STOCK a 
        LEFT JOIN stock_basic_daily_price b 
            ON a.code = b.code 
        GROUP BY 1;
        """

        stock_records = mysql_hook.get_records(sql=query)
        batches = [
            stock_records[i : i + batch_size]
            for i in range(0, len(stock_records), batch_size)
        ]
        return batches

    @task(task_id="collect-upsert-data")
    def collect_and_update(stock_list, **kwargs):
        client = KISApiClient()
        nowtime = get_execute_datetime_in_kst(**kwargs)
        endpoint = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        upsert_with_unique_keys = partial(upsert_method, unique_keys=["date", "code"])

        stock_price = []
        for stock, data_count in stock_list:
            try:
                start_date = (
                    (nowtime - timedelta(days=5)).strftime("%Y%m%d")
                    if data_count > 0
                    else "20200101"
                )
                end_date = nowtime.strftime("%Y%m%d")

                params = {
                    "fid_cond_mrkt_div_code": "J",
                    "fid_input_iscd": stock,
                    "fid_input_date_1": start_date,
                    "fid_input_date_2": end_date,
                    "fid_period_div_code": "D",
                    "fid_org_adj_prc": 1,
                }

                response = client.make_request(
                    method="GET",
                    endpoint=endpoint,
                    params=params,
                    tr_id="FHKST03010100",
                )

                data = (
                    pd.DataFrame(response["output2"])
                    .loc[
                        :,
                        [
                            "stck_bsop_date",
                            "stck_oprc",
                            "stck_hgpr",
                            "stck_lwpr",
                            "stck_clpr",
                            "acml_vol",
                            "acml_tr_pbmn",
                        ],
                    ]
                    .rename(
                        columns={
                            "stck_bsop_date": "date",
                            "stck_oprc": "open",
                            "stck_hgpr": "high",
                            "stck_lwpr": "low",
                            "stck_clpr": "close",
                            "acml_vol": "volume",
                            "acml_tr_pbmn": "amount",
                        }
                    )
                )
                data.insert(0, "code", stock)
                data.insert(1, "currency", "KRW")
                stock_price.append(data)
                time.sleep(1)

            except Exception as stock_error:
                print(f"Error fetching data for stock {stock}: {stock_error}")

        combined_data = pd.concat(stock_price, ignore_index=True)

        combined_data.to_sql(
            con=mysql_hook.get_sqlalchemy_engine(),
            name="stock_basic_daily_price",
            if_exists="append",
            index=False,
            chunksize=5000,
            method=upsert_with_unique_keys,
        )

    stock_batches = fetch_stock_batches()

    upsert_task = collect_and_update.partial().expand(stock_list=stock_batches)

    end = EmptyOperator(task_id="end")

    start >> check_holiday >> stock_batches >> upsert_task >> end

from datetime import timedelta
from functools import partial

import FinanceDataReader as fdr
import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from common.airflow_info import get_execute_datetime_in_kst
from common.custom_pandas import upsert_method
from common.default_config import default_dag_config
from custom_operators.sql import HolidayOperator

CONN_ID = "MYSQL_DATABASE_DATA"

with DAG(
    dag_id="collect-stock-basic-daily-price",
    description="주식 일별 가격 데이터 시세 수집 및 적재",
    start_date=pendulum.datetime(2025, 1, 14, tz="Asia/Seoul"),
    schedule="0 9-20/2 * * 1-5",
    catchup=False,
    default_args=default_dag_config,
    tags=["KRX", "주식", "시세", "수집"],
):
    start = EmptyOperator(task_id="start")

    check_holiday = HolidayOperator(task_id="check-holiday")

    execute_query = SQLExecuteQueryOperator(
        task_id="check-stock-code",
        sql="SELECT a.code, COUNT(OPEN) AS cnt FROM STOCK a LEFT JOIN stock_basic_daily_price b ON a.code = b.code GROUP BY a.code;",
        conn_id=CONN_ID,
        split_statements=False,
    )

    @task(task_id="collect-upsert-data")
    def collect_and_update(**kwargs):
        mysql_hook = MySqlHook(mysql_conn_id=CONN_ID).get_sqlalchemy_engine()
        upsert_with_unique_keys = partial(upsert_method, unique_keys=["date", "code"])

        result = kwargs["ti"].xcom_pull(task_ids="check-stock-code")

        stock_price = []
        for stock, cnt in result:
            if cnt == 0:
                start_date = "2020-01-01"
            else:
                start_date = (
                    get_execute_datetime_in_kst(**kwargs) - timedelta(days=10)
                ).date()

            data = fdr.DataReader(
                symbol=f"KRX:{stock}",
                start=start_date,
                end=get_execute_datetime_in_kst(**kwargs).date(),
            ).reset_index()[
                ["Date", "Open", "High", "Low", "Close", "Volume", "Amount"]
            ]

            data.columns = data.columns.str.lower()
            data.insert(0, "code", stock)
            data.insert(1, "currency", "KRW")

            stock_price.append(data)

        combined_data = pd.concat(stock_price, ignore_index=True)

        combined_data.to_sql(
            con=mysql_hook,
            name="stock_basic_daily_price",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    start >> check_holiday >> execute_query >> collect_and_update() >> end

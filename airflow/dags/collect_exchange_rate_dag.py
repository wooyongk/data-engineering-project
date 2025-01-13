from datetime import timedelta
from functools import partial

import pandas as pd
import pendulum
import yfinance as yf
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from common.airflow_info import get_execute_datetime_in_kst
from common.custom_pandas import upsert_method
from common.default_config import default_dag_config
from custom_operators.sql import SQLColumnValueExistenceCheckOperator, HolidayOperator

CONN_ID = "MYSQL_DATABASE_DATA"
TABLE_NAME = "exchange_rate"
CURRENCY_SYMBOLS = ["USDKRW=X", "EURKRW=X", "JPYKRW=X"]
FORMATTED_CURRENCY_SYMBOLS = [item[:3] + "/" + item[3:6] for item in CURRENCY_SYMBOLS]


def transform_exchange_data(data):
    reformatted_data = []
    for column in data.columns.levels[1]:
        base_currency, quote_currency = column[:3], column[3:6]
        for date, row in data.iterrows():
            reformatted_data.append(
                {
                    "currency_pair": f"{base_currency}/{quote_currency}",
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "exchange_date": date,
                    "open_rate": row[("Open", column)],
                    "close_rate": row[("Close", column)],
                    "high_rate": row[("High", column)],
                    "low_rate": row[("Low", column)],
                }
            )
    return pd.DataFrame(reformatted_data)


with DAG(
    dag_id="collect-exchange-rate-data",
    description="환율 데이터 수집 및 적재",
    start_date=pendulum.datetime(2024, 9, 1, tz="Asia/Seoul"),
    schedule="0 9-18 * * 1-5",
    catchup=False,
    default_args=default_dag_config,
    tags=["yfinance", "환율", "수집"],
):
    start = EmptyOperator(task_id="start")

    check_holiday = HolidayOperator(task_id="check-holiday")

    validate_table_task = SQLColumnValueExistenceCheckOperator(
        task_id="validate-table",
        conn_id=CONN_ID,
        table=TABLE_NAME,
        column_name="currency_pair",
        column_values=FORMATTED_CURRENCY_SYMBOLS,
    )

    @task(task_id="branch-period")
    def branch_logic(**kwargs):
        task_instance = kwargs["ti"]
        validation_results = task_instance.xcom_pull(task_ids="validate-table")
        if not validation_results:
            raise ValueError("No results from table validation.")

        outdated = [
            CURRENCY_SYMBOLS[i]
            for i, currency in enumerate(FORMATTED_CURRENCY_SYMBOLS)
            if not validation_results.get(currency, False)
        ]
        up_to_date = [
            CURRENCY_SYMBOLS[i]
            for i, currency in enumerate(FORMATTED_CURRENCY_SYMBOLS)
            if validation_results.get(currency, False)
        ]

        task_instance.xcom_push(key="outdated", value=outdated)
        task_instance.xcom_push(key="up_to_date", value=up_to_date)

        return [
            "collect_historical" if outdated else None,
            "collect_recent" if up_to_date else None,
        ]

    @task(task_id="collect-historical")
    def collect_historical_data(**kwargs):
        task_instance = kwargs["ti"]
        outdated = task_instance.xcom_pull(key="outdated", task_ids="branch-period")
        if not outdated:
            raise AirflowSkipException("No historical data to collect.")

        data = yf.download(
            outdated,
            start="2001-01-01",
            end=(get_execute_datetime_in_kst(**kwargs) + timedelta(days=1)).date(),
        )
        return transform_exchange_data(data)

    @task(task_id="collect-recent")
    def collect_recent_data(**kwargs):
        task_instance = kwargs["ti"]
        up_to_date = task_instance.xcom_pull(key="up_to_date", task_ids="branch-period")
        if not up_to_date:
            raise AirflowSkipException("No recent data to collect.")

        data = yf.download(
            up_to_date,
            start=(get_execute_datetime_in_kst(**kwargs) + timedelta(days=-2)).date(),
            end=(get_execute_datetime_in_kst(**kwargs) + timedelta(days=1)).date(),
        )
        return transform_exchange_data(data)

    @task(task_id="upsert-data", trigger_rule="all_done")
    def upsert_data_to_db(**kwargs):
        mysql_hook = MySqlHook(mysql_conn_id=CONN_ID).get_sqlalchemy_engine()
        upsert_with_unique_keys = partial(
            upsert_method, unique_keys=["currency_pair", "exchange_date"]
        )

        task_instance = kwargs["ti"]
        historical_data = task_instance.xcom_pull(task_ids="collect-historical")
        recent_data = task_instance.xcom_pull(task_ids="collect-recent")

        combined_data = pd.concat(
            [
                historical_data if historical_data is not None else pd.DataFrame(),
                recent_data if recent_data is not None else pd.DataFrame(),
            ],
            ignore_index=True,
        )

        combined_data.to_sql(
            con=mysql_hook,
            name=TABLE_NAME,
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    (
        start
        >> check_holiday
        >> validate_table_task
        >> branch_logic()
        >> [collect_historical_data(), collect_recent_data()]
        >> upsert_data_to_db()
        >> end
    )

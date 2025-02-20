from functools import partial

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from common.custom_pandas import upsert_method
from common.default_config import default_dag_config
from custom_function.publicdata import PublicDataApiClient

with DAG(
    dag_id="collect-passenger-information-data",
    description="출입국별 승객 예고 수집 및 적재",
    start_date=pendulum.datetime(2025, 2, 18, tz="Asia/Seoul"),
    schedule="*/30 * * * *",
    catchup=False,
    default_args=default_dag_config,
    tags=["PublicData", "출입국 승객 예고", "수집"],
):

    start = EmptyOperator(task_id="start")

    @task(task_id="fetch-passenger-info-data")
    def collect_data():
        client = PublicDataApiClient()
        endpoint = "/B551177/PassengerNoticeKR/getfPassengerNoticeIKR"

        params_list = [
            {"selectdate": "0", "type": "json"},
            {"selectdate": "1", "type": "json"},
        ]

        responses = []
        for params in params_list:
            try:
                response = client.make_request(
                    method="GET", endpoint=endpoint, params=params
                )
                responses.append(response["response"]["body"].get("items", []))
            except Exception as e:
                print(f"Error fetching data: {e}")
                responses.append([])

        return responses

    @task(task_id="create-dataframe")
    def make_dataframe(**kwargs):
        response_data = kwargs["ti"].xcom_pull(task_ids="fetch-passenger-info-data")

        data = pd.concat(
            [pd.DataFrame(data)[:24] for data in response_data if data],
            ignore_index=True,
        ).rename(
            columns={
                "adate": "date",
                "atime": "time",
            }
        )

        def create_terminal_dataframe(
            column, terminal_id, arrival_departure, gate_numbers
        ):
            df = data[["date", "time", column]].copy()
            df.insert(2, "terminal_id", terminal_id)
            df.insert(3, "arrival_departure", arrival_departure)
            df.insert(4, "gate_number", gate_numbers)
            df.rename(columns={column: "expected_passengers"}, inplace=True)
            return df

        terminal_dfs = [
            create_terminal_dataframe("t1sum5", "P01", "출국장", "1,2"),
            create_terminal_dataframe("t1sum6", "P01", "출국장", "3"),
            create_terminal_dataframe("t1sum7", "P01", "출국장", "4"),
            create_terminal_dataframe("t1sum8", "P01", "출국장", "5,6"),
            create_terminal_dataframe("t2sum3", "P03", "출국장", "1"),
            create_terminal_dataframe("t2sum4", "P03", "출국장", "1"),
        ]

        final_df = pd.concat(terminal_dfs, ignore_index=True)

        return final_df

    @task(task_id="upsert-data")
    def upsert_data_to_db(**kwargs):
        mysql_hook = MySqlHook(
            mysql_conn_id="MYSQL_DATABASE_DATA"
        ).get_sqlalchemy_engine()

        upsert_with_unique_keys = partial(
            upsert_method, unique_keys=["fid", "flight_id"]
        )

        data = kwargs["ti"].xcom_pull(task_ids="create-dataframe")

        data.to_sql(
            con=mysql_hook,
            name="passenger_traffic_forecast",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    start >> collect_data() >> make_dataframe() >> upsert_data_to_db() >> end

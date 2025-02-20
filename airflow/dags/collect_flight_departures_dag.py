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
from custom_function.publicdata import PublicDataApiClient

with DAG(
    dag_id="collect-flight-departures-data",
    description="항공기 출발 정보 수집 및 적재",
    start_date=pendulum.datetime(2025, 2, 19, tz="Asia/Seoul"),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_dag_config,
    tags=["PublicData", "도착정보", "수집"],
):

    start = EmptyOperator(task_id="start")

    @task(task_id="fetch-flight-departures-data")
    def collect_data():
        client = PublicDataApiClient()

        endpoint = "/B551177/StatusOfPassengerFlightsOdp/getPassengerDeparturesOdp"

        params = {"from_time": "0000", "to_time": "2400", "lang": "k", "type": "json"}

        response = client.make_request(method="GET", endpoint=endpoint, params=params)

        return response

    @task(task_id="fetch-flight-departures-data-detail")
    def collect_data_detail(**kwargs):
        client = PublicDataApiClient()

        endpoint = "/B551177/statusOfAllFltDeOdp/getFltDeparturesDeOdp"

        params = {
            "searchdtCode": "S",
            "numOfRows": "20000",
            "searchDate": get_execute_datetime_in_kst(**kwargs).strftime("%Y%m%d"),
            "searchFrom": "0000",
            "searchTo": "2400",
            "type": "json",
        }

        response = client.make_request(method="GET", endpoint=endpoint, params=params)

        return response

    @task(task_id="merge-flight-departures-data")
    def merge_data(**kwargs):
        res_1 = kwargs["ti"].xcom_pull(task_ids="fetch-flight-departures-data")[
            "response"
        ]["body"]["items"]
        res_2 = kwargs["ti"].xcom_pull(task_ids="fetch-flight-departures-data-detail")[
            "response"
        ]["body"]["items"]

        data_1 = pd.DataFrame(res_1)[
            [
                "airline",
                "flightId",
                "airport",
                "chkinrange",
                "gatenumber",
                "remark",
                "terminalId",
                "elapsetime",
            ]
        ].rename(
            columns={
                "flightId": "flight_id",
                "airport": "arrival_airport",
                "chkinrange": "checkin_counter",
                "gatenumber": "gate_number",
                "remark": "status",
                "terminalId": "terminal_id",
                "elapsetime": "elapse_time",
            }
        )

        data_2 = pd.DataFrame(res_2)[
            [
                "fid",
                "flightId",
                "scheduleDatetime",
                "estimatedDatetime",
                "chkinRange",
                "gateNumber",
                "aircraftSubtype",
                "aircraftRegNo",
            ]
        ].rename(
            columns={
                "flightId": "flight_id",
                "scheduleDatetime": "schedule_datetime",
                "estimatedDatetime": "estimated_datetime",
                "chkinRange": "checkin_counter",
                "gateNumber": "gate_number",
                "aircraftSubtype": "aircraft_subtype",
                "aircraftRegNo": "aircraft_register_no",
            }
        )

        final_df = data_2.merge(
            data_1, on=["flight_id", "checkin_counter", "gate_number"]
        )

        print(len(data_1))
        print(len(data_2))

        return final_df

    @task(task_id="upsert-data")
    def upsert_data_to_db(**kwargs):
        mysql_hook = MySqlHook(
            mysql_conn_id="MYSQL_DATABASE_DATA"
        ).get_sqlalchemy_engine()

        upsert_with_unique_keys = partial(
            upsert_method, unique_keys=["fid", "flight_id"]
        )

        data = kwargs["ti"].xcom_pull(task_ids="merge-flight-departures-data")

        datetime_cols = ["schedule_datetime", "estimated_datetime"]
        data[datetime_cols] = data[datetime_cols].apply(pd.to_datetime, errors="coerce")

        print(data.head(5))

        data.to_sql(
            con=mysql_hook,
            name="flight_departures",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    end = EmptyOperator(task_id="end")

    (
        start
        >> collect_data()
        >> collect_data_detail()
        >> merge_data()
        >> upsert_data_to_db()
        >> end
    )

from functools import partial
from io import BytesIO

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from common.custom_pandas import upsert_method
from common.default_config import default_dag_config

KRX_BASE_URL = "https://data.krx.co.kr"
KRX_HEADERS = {
    "Referer": f"{KRX_BASE_URL}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def get_session() -> requests.Session:
    try:
        session = requests.Session()
        session.headers.update(KRX_HEADERS)
        return session
    except requests.RequestException as e:
        raise ConnectionError("Failed to create session: ", e)


def get_otp_code(session: requests.Session) -> str:
    otp_params = {
        "mktId": "ALL",
        "share": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01901",
    }
    response = session.get(
        url=f"{KRX_BASE_URL}/comm/fileDn/GenerateOTP/generate.cmd",
        params=otp_params,
    )
    response.raise_for_status()
    if not response.content:
        raise ValueError("OTP code response is empty.")
    return response.content


def download_stock_data(session: requests.Session, otp_code: str) -> pd.DataFrame:
    download_url = f"{KRX_BASE_URL}/comm/fileDn/download_csv/download.cmd"
    response = session.post(url=download_url, data={"code": otp_code})
    response.raise_for_status()

    df = pd.read_csv(
        BytesIO(response.content),
        encoding="cp949",
        usecols=list(range(7)),
        names=[
            "standard_code",
            "code",
            "company_kr_name",
            "company_kr_short_name",
            "company_eg_name",
            "listing_date",
            "market",
        ],
        header=0,
        parse_dates=["listing_date"],
    )

    return df


with DAG(
    dag_id="collect-stock-basic-data",
    description="주식 기본 데이터 수집 및 적재",
    start_date=pendulum.datetime(2024, 9, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    default_args=default_dag_config,
    tags=["주식", "KRX", "수집"],
):
    start = EmptyOperator(task_id="start")

    @task(task_id="fetch-stock-data-from-krx")
    def fetch_stock_data_from_krx() -> BytesIO:
        with get_session() as session:
            otp_code = get_otp_code(session)
            return download_stock_data(session, otp_code)

    @task(task_id="upsert-stock-data-to-db")
    def upsert_stock_data_to_db(**context) -> None:
        mysql_hook = MySqlHook(
            mysql_conn_id="MYSQL_DATABASE_DATA"
        ).get_sqlalchemy_engine()
        upsert_with_unique_keys = partial(upsert_method, unique_keys=["id", "subject"])

        data = context["ti"].xcom_pull(task_ids="fetch-stock-data-from-krx")

        data.to_sql(
            con=mysql_hook,
            name="stock",
            if_exists="append",
            index=False,
            chunksize=1000,
            method=upsert_with_unique_keys,
        )

    stock_data = fetch_stock_data_from_krx()

    end = EmptyOperator(task_id="end")

    start >> stock_data >> upsert_stock_data_to_db() >> end

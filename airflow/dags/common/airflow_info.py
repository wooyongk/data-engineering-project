import datetime


def get_dag_id(**kwargs) -> str:
    return kwargs["dag"].dag_id


def get_execute_datetime_in_utc(**kwargs) -> datetime:
    return kwargs["data_interval_end"].in_timezone("UTC")


def get_execute_datetime_in_kst(**kwargs) -> datetime:
    return kwargs["data_interval_end"].in_timezone("Asia/Seoul")

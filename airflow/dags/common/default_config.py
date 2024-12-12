import datetime


default_dag_config = {
    "owner": "engineer-team",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(seconds=15),
}

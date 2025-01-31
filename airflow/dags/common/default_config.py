import datetime

from common.telegram_notification import on_failure

default_dag_config = {
    "owner": "engineer-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=30),
    "on_failure_callback": on_failure,
    # "on_success_callback": ,
}

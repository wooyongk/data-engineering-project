from airflow.providers.telegram.operators.telegram import TelegramOperator
from common.airflow_info import get_execute_datetime_in_kst


def on_failure(kwargs):
    dag = kwargs["ti"].dag_id
    task = kwargs["ti"].task_id
    exec_date = get_execute_datetime_in_kst(**kwargs).strftime("%Y-%m-%d %H:%M:%S")
    exception = kwargs["exception"]

    message_content = (
        f"<b>dag</b> : {dag} \n"
        f"<b>task</b> : {task} \n"
        f"<b>exec date</b>: {exec_date} \n"
        f"<b>exception</b>: <blockquote>{exception}</blockquote>> \n"
    )

    send_message_telegram_task = TelegramOperator(
        task_id="send_message_telegram",
        text=message_content,
        telegram_kwargs={"parse_mode": "HTML"},
    )

    return send_message_telegram_task.execute(context=None)

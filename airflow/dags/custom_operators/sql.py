from __future__ import annotations

from typing import Any
from typing import List
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.models.skipmixin import SkipMixin
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.context import Context


class SQLColumnValueExistenceCheckOperator(BaseSQLOperator):
    template_fields: Sequence[str] = ("table_name", "column_name", "column_values")

    def __init__(
        self,
        *,
        table: str,
        column_name: str,
        column_values: List[str],
        conn_id: str,
        **kwargs,
    ):
        super().__init__(conn_id=conn_id, **kwargs)
        self.table_name = table
        self.column_name = column_name
        self.column_values = column_values

    def execute(self, context):
        self.log.info(
            "Checking values in column: %s for table: %s",
            self.column_name,
            self.table_name,
        )
        db_hook = self.get_db_hook()

        value_existence_results = {}

        for column_value in self.column_values:
            query = """
                SELECT COUNT(*)
                FROM {table}
                WHERE {column} = %s
            """.format(
                table=self.table_name, column=self.column_name
            )
            self.log.info("Executing query: %s", query)

            query_result = db_hook.get_records(query, parameters=(column_value,))

            if not query_result:
                raise AirflowException(f"No results returned for query: {query}")

            record_count = query_result[0][0]
            value_existence_results[column_value] = record_count > 0

        self.log.info("Check results: %s", value_existence_results)
        return value_existence_results


class HolidayOperator(BaseSQLOperator, SkipMixin):
    def __init__(
        self,
        *,
        mysql_conn_id: str = "MYSQL_DATABASE_DATA",
        country: str = "KR",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.country = country

    def execute(self, context: Context) -> Any:
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        query = "SELECT open_yn FROM holiday WHERE country_code = %s AND date = %s"
        query_date = (context["data_interval_end"].in_timezone("Asia/Seoul")).date()
        self.log.info(
            "Executing query for country %s on date %s.", self.country, query_date
        )

        records = mysql_hook.get_records(query, parameters=(self.country, query_date))

        if not records:
            self.log.warning(
                "No records returned from the query. Skipping downstream tasks."
            )
            self._skip_downstream_tasks(context)
            return

        result = records[0][0]
        self.log.info("Result for column 'open_yn': %s", result)

        if result and result.upper() == "Y":
            self.log.info("Proceeding with downstream tasks.")
        else:
            self.log.info("Skipping downstream tasks because 'open_yn' is not 'Y'.")
            self._skip_downstream_tasks(context)

    def _skip_downstream_tasks(self, context: Context) -> None:
        """Helper method to skip downstream tasks."""
        downstream_tasks = context["task"].get_flat_relatives(upstream=False)
        if not downstream_tasks:
            self.log.info("No downstream tasks to skip.")
            return

        self.log.info(
            "Skipping downstream tasks: %s", [t.task_id for t in downstream_tasks]
        )
        self.skip(context["dag_run"], context["logical_date"], downstream_tasks)

from __future__ import annotations

from typing import List
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import BaseSQLOperator


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

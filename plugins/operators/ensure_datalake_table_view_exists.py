import re
from pprint import pprint  # noqa
from typing import List, Optional

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template


class EnsurePostgresDatalakeTableViewExistsOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param source_schema: Source Schema name
    :type source_schema: str
    :param source_table: Source Table name
    :type source_table: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    :param destination_table: Destination Table name
    :type destination_table: str
    :param prepend_fields: Fields that you want to appear at the start of the table columns
    :type prepend_fields: Optional[List[str]]
    :param append_fields: Fields that you want to appear at the end of the table columns
    :type append_fields: Optional[List[str]]

    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        source_schema: str,
        source_table: str,
        destination_schema: str,
        destination_table: str,
        append_fields: Optional[List[str]] = [],
        prepend_fields: Optional[List[str]] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising EnsurePostgresDatalakeTableViewExistsOperator")
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.source_table = source_table
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.append_fields = append_fields
        self.prepend_fields = prepend_fields

        self.context = {
            "source_schema": source_schema,
            "source_table": source_table,
            "destination_schema": destination_schema,
            "destination_table": self.destination_table,
        }

        self.columns_template = """SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{{source_schema}}'
            AND table_name   = '{{source_table}}';

"""  # noqa

        self.create_template = (
            "DROP MATERIALIZED VIEW IF EXISTS {{ destination_schema }}.{{ destination_table }};\n"
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {{ destination_schema }}.{{ destination_table }} AS\n"
            "    SELECT {{column_names_str}} FROM {{ source_schema }}.{{ source_table }}\n"
            "WITH NO DATA;\n"
            "CREATE UNIQUE INDEX {{ destination_table }}_uidx ON {{ destination_schema }}.{{ destination_table }} (id);\n"  # noqa
            "REFRESH MATERIALIZED VIEW {{ destination_schema }}.{{ destination_table }};\n"
        )

        self.log.info("Initialised EnsurePostgresDatalakeTableViewExistsOperator OK")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)

            self.columns_sql = render_template(self.columns_template, context=context, extra_context=self.context)

            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info(f"Executing {self.columns_sql}")
                    result = conn.execute(self.columns_sql)
                    rows = result.fetchall()
                    self.log.info(f"prepend_fields = {self.prepend_fields}")
                    self.log.info(f"append_fields = {self.append_fields}")
                    # Step 1: Filter and sort the columns, excluding the prepend and append fields
                    sorted_columns = sorted(
                        [
                            f'"{row["column_name"]}"'
                            for row in rows
                            if row["column_name"] not in self.append_fields + self.prepend_fields
                        ]
                    )
                    # Check if column_names is not empty
                    if not sorted_columns:
                        raise AirflowException(f"No columns found for table {self.source_schema}.{self.source_table}")

                    # Step 2: Prepend and append the prepend and append fields respectively

                    # Prepend specified fields at the beginning only if they exist
                    prepend_columns = [
                        f'"{col}"' for col in self.prepend_fields if any(row["column_name"] == col for row in rows)
                    ]

                    # Append specified fields at the end only if they exist
                    append_columns = [
                        f'"{col}"' for col in self.append_fields if any(row["column_name"] == col for row in rows)
                    ]

                    # Combine the lists: prepend fields, sorted fields, append fields
                    final_columns_list = prepend_columns + sorted_columns + append_columns

                    # Step 3: Join the list into a string
                    column_names_str = ", ".join(final_columns_list)

                    self.create_view_sql = render_template(
                        self.create_template,
                        context=context,
                        extra_context={
                            **self.context,
                            "column_names_str": column_names_str,
                        },
                    )
                    self.log.info(f"{self.create_view_sql}")
                    conn.execute(self.create_view_sql)

                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"{self.destination_schema}.{self.destination_table} View Created"
        except Exception as e:
            self.log.error(f"An error occurred: {e}")
            raise AirflowException(e)

    def get_postgres_sqlalchemy_engine(self, hook, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        conn_uri = hook.get_uri().replace("postgres:/", "postgresql:/")
        conn_uri = re.sub(r"\?.*$", "", conn_uri)
        return create_engine(conn_uri, **engine_kwargs)

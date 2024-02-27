import re
from pprint import pprint  # noqa

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template


class AppendTransientTableDataOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param source_schema: Source Schema name
    :type source_schema: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    :param table: Table name
    :type table: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        source_schema: str,
        destination_schema: str,
        table: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising AppendTransientTableDataOperator")
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.destination_schema = destination_schema
        self.table = table

        self.context = {
            "source_schema": source_schema,
            "destination_schema": destination_schema,
            "table": table,
        }

        self.delete_template = """DELETE FROM {{ destination_schema }}.{{table}}
            WHERE
                id IN (
                    SELECT o.id
                    FROM {{source_schema}}.{{table}} o
                    WHERE
                        o.airflow_synced_at = '{{ ds }}'
                );
"""
        self.columns_template = """SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{{source_schema}}'
            AND table_name   = '{{table}}';

"""
        self.insert_template = """INSERT INTO {{destination_schema}}.{{table}} ({{column_names_str}})
            SELECT {{column_names_str}}
            FROM {{source_schema}}.{{table}};

            ;
"""

        self.log.info("AppendTransientTableDataOperator Initialised OK")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)

            self.delete_sql = render_template(self.delete_template, context=context, extra_context=self.context)
            self.columns_sql = render_template(self.columns_template, context=context, extra_context=self.context)
            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info("Appending {self.table} Data into Datalake")
                    self.log.info(f"{self.delete_sql}")
                    conn.execute(self.delete_sql)

                    self.log.info(f"{self.columns_sql}")
                    result = conn.execute(self.columns_sql)
                    column_names = [f'"{row["column_name"]}"' for row in result]
                    column_names_str = ", ".join(column_names)

                    # Check if column_names is not empty
                    if not column_names:
                        raise AirflowException(f"No columns found for table {self.source_schema}.{self.table}")

                    self.insert_sql = render_template(
                        self.insert_template,
                        context=context,
                        extra_context={
                            **self.context,
                            "column_names_str": column_names_str,
                        },
                    )
                    self.log.info(f"{self.insert_sql}")
                    conn.execute(self.insert_sql)
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"Data Appended for {self.table}"
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

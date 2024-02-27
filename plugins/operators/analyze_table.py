import re
from pprint import pprint  # noqa

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class RefreshPostgresTableStatisticsOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Source Schema name
    :type schema: str
    :param table: Source Table name
    :type table: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        table: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising RefreshPostgresTableStatisticsOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table

        self.log.info("Initialised RefreshPostgresTableStatisticsOperator")
        self.template_func = f"ANALYZE {self.schema}.{self.table};"  # noqa

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)

            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info(f"Executing {self.template_func}")
                    conn.execute(self.template_func)
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"Analyzed {self.schema}.{self.table}"
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

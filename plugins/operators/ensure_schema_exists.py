import re
from pprint import pprint  # noqa

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class EnsurePostgresSchemaExistsOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Schema name
    :typeschema: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising EnsurePostgresSchemaExistsOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema

        self.log.info("Initialised EnsurePostgresSchemaExistsOperator")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)

            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info(f"Ensuring {self.schema} schema exists")
                    self.log.info(f"Executing: CREATE SCHEMA IF NOT EXISTS {self.schema};")
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")  # noqa
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"{self.schema} Exists"
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

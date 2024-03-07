import re
from pprint import pprint  # noqa

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.render_template import render_template


class RunChecksumSQLPostgresOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Schema name
    :type schema: str
    :param filename: File name
    :type filename: str
    :param checksum: File checksum
    :type checksum: str
    :param sql: sql
    :type sql: str
    :param report_type: type of sql [report|fact|index]
    :type report_type: str
    """

    ui_color = "#f9c915"
    template_fields = "sql"
    template_ext = ".sql"
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        filename: str,
        checksum: str,
        sql: str,
        report_type: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising RunChecksumSQLPostgresOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.filename = filename
        self.checksum = checksum
        self.report_type = report_type
        self.sql_template = sql
        self.context = {
            "schema": schema,
            "filename": filename,
            "checksum": checksum,
        }
        self.preoperation_template = f"""
CREATE TABLE IF NOT EXISTS {self.schema}.report_checksums (
    id SERIAL PRIMARY KEY,
    checksum CHAR(64),
    filename TEXT,
    report_type TEXT,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    UNIQUE (filename, report_type)
);
"""

        self.log.info("Initialised RunChecksumSQLPostgresOperator")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)
            engine = self.get_postgres_sqlalchemy_engine(hook)

            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    is_modified = self._check_if_modified(conn)
                    self.context["is_modified"] = is_modified

                    self.sql = render_template(self.sql_template, context=context, extra_context=self.context)
                    self.log.info(f"Executing {self.sql}")
                    conn.execute(self.sql)
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"Dropped {self.schema}.{self.table}"
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

    def _check_if_modified(self, conn):
        # Check if the record exists and if the checksum matches
        select_query = f"""
            SELECT checksum FROM {self.schema}.report_checksums
            WHERE filename = '{self.filename}' AND report_type = '{self.report_type}';
        """
        existing_record = conn.execute(select_query).fetchone()

        # If the record does not exist or the checksum is different, perform upsert
        if not existing_record or existing_record["checksum"] != self.checksum:
            upsert_query = f"""
                INSERT INTO {self.schema}.report_checksums (filename, checksum, report_type)
                VALUES ('{self.filename}', '{self.checksum}', '{self.report_type}')
                ON CONFLICT (filename, report_type)
                DO UPDATE SET checksum = EXCLUDED.checksum, updatedat = CURRENT_TIMESTAMP;
            """
            conn.execute(upsert_query)

            # If there was no existing record or the checksums did not match, consider it modified
            return True
        else:
            # If the record exists and the checksum matches, it's not considered modified
            return False

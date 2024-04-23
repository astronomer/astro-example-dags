import os
import re
import json
from pprint import pprint  # noqa
from typing import List, Optional

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

from plugins.utils.render_template import render_template

from plugins.operators.mixins.get_columns_from_table import GetColumnsFromTableMixin


class RunChecksumSQLPostgresOperator(GetColumnsFromTableMixin, BaseOperator):
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
    :param sql_type: type of sql [report|function|index|dimension|user]
    :type sql_type: str
    :param json_schema_file_dir: Directory of Exported Json Schema Files
    :type json_schema_file_dir: Optional[str]
    :param add_table_columns_to_context: A list of tablenames to be added to context for use in template filters
    :type add_table_columns_to_context: Optional[List[str]]

    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        filename: str,
        checksum: str,
        sql: str,
        sql_type: str,
        json_schema_file_dir: str = "",
        add_table_columns_to_context: Optional[List[str]] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising RunChecksumSQLPostgresOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.filename = filename
        self.checksum = checksum
        self.sql_type = sql_type
        self.sql_template = sql
        self.json_schema_file_dir = json_schema_file_dir
        self.add_table_columns_to_context = add_table_columns_to_context or []
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
    sql_type TEXT,
    updatedat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    createdat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_filename_sql_type UNIQUE (filename, sql_type)
);
"""

        self.log.info("Initialised RunChecksumSQLPostgresOperator")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)
            tableau_user = Connection.get_connection_from_secrets("tableau_user_id")
            self.context["tableau_username"] = tableau_user.login
            self.context["tableau_password"] = tableau_user.password
            engine = self.get_postgres_sqlalchemy_engine(hook)

            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self._add_table_columns_to_context(conn)
                    self._add_event_name_ids_to_context()
                    self.preoperation_sql = render_template(
                        self.preoperation_template,
                        context=context,
                        extra_context=self.context,
                    )
                    self.log.info(f"Executing {self.preoperation_sql}")
                    conn.execute(self.preoperation_sql)

                    is_modified = self._check_if_modified(conn)
                    self.log.info(f"{self.filename}.sql is modified = {is_modified}")
                    self.context["is_modified"] = is_modified

                    self.log.info(self.sql_template)
                    self.sql = render_template(self.sql_template, context=context, extra_context=self.context)

                    # Validate the SQL to make sure it follows our naming convention
                    self._validate_sql_convention(self.sql)

                    self.log.info(f"Executing {self.sql}")
                    conn.execute(self.sql)
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return f"Run SQL for {self.schema},{self.filename}, {self.sql_type}"
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
            WHERE filename = '{self.filename}' AND sql_type = '{self.sql_type}';
        """
        existing_record = conn.execute(select_query).fetchone()

        # If the record does not exist or the checksum is different, perform upsert
        if not existing_record or existing_record["checksum"] != self.checksum:
            upsert_query = f"""
                INSERT INTO {self.schema}.report_checksums (filename, checksum, sql_type)
                VALUES ('{self.filename}', '{self.checksum}', '{self.sql_type}')
                ON CONFLICT (filename, sql_type)
                DO UPDATE SET checksum = EXCLUDED.checksum, updatedat = CURRENT_TIMESTAMP;
            """
            conn.execute(upsert_query)

            # If there was no existing record or the checksums did not match, consider it modified
            return True
        else:
            # If the record exists and the checksum matches, it's not considered modified
            return False

    def _validate_sql_convention(self, sql):
        if self.sql_type == "index":
            return
        elif self.sql_type == "user":
            return
        pattern = ""
        expected_prefix = ""
        if self.sql_type == "report":
            pattern = r"CREATE (MATERIALIZED )?VIEW IF NOT EXISTS (\w+)\.(\w+)"
            expected_prefix = "rep__"
        elif self.sql_type == "fact":
            pattern = r"CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)"
            expected_prefix = "fact__"
        elif self.sql_type == "dimension":
            pattern = r"CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)"
            expected_prefix = "dim__"
        elif self.sql_type == "function":  # This is the new case for SQL functions
            pattern = r"CREATE (OR REPLACE )?FUNCTION (\w+)\.(\w+)"
            expected_prefix = "fn__"

        if pattern:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            for match in matches:
                # Assuming the table or view name is always the last group captured
                view_or_table_name = match[-1]  # This gets the last element of the match tuple
                if not view_or_table_name.startswith(expected_prefix):
                    raise AirflowException(
                        f"{view_or_table_name} does not start with '{expected_prefix}' as required for report type '{self.sql_type}'."  # noqa
                    )

    def _add_table_columns_to_context(self, conn):
        for table in self.add_table_columns_to_context:
            self.context[f"{table}_columns"] = self.get_columns_from_table(conn, "public", table)

    def _get_event_name_ids(self):

        if self.json_schema_file_dir:
            schema_path = os.path.join(self.json_schema_file_dir, "IOrderEvent.json")

            with open(schema_path, "r") as file:
                schema = json.load(file)
                event_name_id = schema["properties"].get("event_name_id", None)
                if not event_name_id:
                    raise AirflowException(f"Schema {self.schema_file_path} does not contain event_name_id")
                event_name_ids = event_name_id.get("enum", None)
                if not event_name_ids:
                    raise AirflowException(f"Schema {self.schema_file_path} does not contain event_name_id enums")
                return [
                    event
                    for event in event_name_ids
                    if event and event not in ["statusUpdated", "sentCustomerEmail", "paymentReceived"]
                ]
        return []

    def _add_event_name_ids_to_context(self):
        self.context["event_name_ids"] = self._get_event_name_ids()

import re
from pprint import pprint  # noqa

from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class EnsureMissingColumnsPostgresFunctionOperator(BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param source_schema: Source Schema name
    :type source_schema: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        source_schema: str,
        destination_schema: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising EnsureMissingColumnsPostgresFunctionOperator")
        self.postgres_conn_id = postgres_conn_id
        self.source_schema = source_schema
        self.destination_schema = destination_schema

        self.template_func = f"""CREATE OR REPLACE FUNCTION addMissingColumns(_table TEXT)
RETURNS VOID
AS
$$
DECLARE
  str text;
  t_curs cursor for
    SELECT * FROM INFORMATION_SCHEMA.COLUMNS c_transient
    WHERE
        NOT EXISTS(
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS c_public
            WHERE c_public.table_name=_table
            AND c_public.column_name=c_transient.column_name
            AND c_public.table_schema='{self.destination_schema}'
      )
      AND c_transient.table_name=_table
      AND c_transient.table_schema='{self.source_schema}'
    ;
BEGIN
  str := '';
  FOR t_row in t_curs LOOP
    str := str ||' ADD COLUMN '||t_row.column_name||' '||t_row.data_type||' DEFAULT NULL,';
  end LOOP;
  IF (str != '') THEN
    str = trim(trailing ',' from str);
    str = 'ALTER TABLE ' || {self.destination_schema} || '.' || _table || ' ' || str;
    EXECUTE str;
  ELSE
    raise notice 'No Schema Changes Detected';
  END IF;
end;
$$
LANGUAGE plpgsql;
"""  # noqa

        self.log.info("Initialised EnsureMissingColumnsPostgresFunctionOperator")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)

            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info("Creating PLSQL Function")
                    self.log.info(f"{self.template_func}")
                    self.log.info(f"{self.source_schema}, {self.destination_schema}")
                    conn.execute(self.template_func)
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            return "FUNCTION addMissingColumns exists Ok "
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

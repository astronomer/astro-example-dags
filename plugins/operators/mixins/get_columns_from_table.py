from typing import List

from airflow.exceptions import AirflowException
from sqlalchemy.engine.base import Connection


class GetColumnsFromTableMixin:
    """
    Mixin class to fetch column names for a given table in a specified schema using an SQLAlchemy connection.
    """

    def get_columns_from_table(self, conn: Connection, schema_name: str, table_name: str) -> List[str]:
        """
        Fetches column names for a specified table and schema using an SQLAlchemy connection.

        :param conn: SQLAlchemy connection object.
        :param schema_name: Name of the schema.
        :param table_name: Name of the table.
        :return: List of column names.
        """
        # SQL query to fetch column names. Adjust the SQL based on your database type.
        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """

        materialized_sql = f"""
            SELECT attname AS column_name,
                format_type(atttypid, atttypmod) AS data_type,
                attnotnull AS is_not_nullable
            FROM pg_attribute
            JOIN pg_class c ON c.oid = pg_attribute.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = '{table_name}'  -- Replace with your materialized view name
            AND c.relkind = 'm'  -- 'm' indicates a materialized view
            AND n.nspname = '{schema_name}'
            AND attnum > 0
            AND NOT attisdropped;

        """
        self.log.info(f"get_columns_from_table: {sql}")

        print("GET COLUMNS", sql)
        # Execute the query and fetch results using SQLAlchemy
        result = conn.execute(sql)
        column_names = [f'{row["column_name"]}' for row in result]
        if not column_names:
            self.log.info("No columns returned from information_catalog query, assuming its a materialized view")
            self.log.info(f"get_columns_from_table: {materialized_sql}")
            result = conn.execute(materialized_sql)
            column_names = [f'{row["column_name"]}' for row in result]

        if not column_names:
            raise AirflowException(f"No columns found for table {schema_name}.{table_name}")

        return column_names

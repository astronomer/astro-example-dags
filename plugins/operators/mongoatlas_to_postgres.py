import re
import sys
import json
from typing import Iterable, Optional

import pandas as pd
from bson import json_util
from jpype import JPackage
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

# from jpype.types import *


class MongoAtlasToPostgresViaDataframeOperator(BaseOperator):
    """
    :param mongo_jdbc_conn_id: The Mongo JDBC connection id
    :type mongo_jdbc_conn_id: str
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param preoperation: sql statement to be executed prior to loading the data. (templated)
    :type preoperation: Optional[str]
    :param sql: SQL query to execute against the source database. (templated)
    :type sql: str
    :param table: Destination Table name
    :type table: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    :param unwind_prefix: Whether the operation is unwinding a subtable
    :type unwind_prefix: str
    """

    template_fields = ("sql", "preoperation")
    template_ext = ".sql"

    template_fields_renderers = {"preoperation": "sql", "sql": "sql"}
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        mongo_jdbc_conn_id: str = "mongo_jdbc_conn_id",
        postgres_conn_id: str = "postgres_conn_id",
        preoperation: Optional[str] = None,
        sql: str,
        table: str,
        destination_schema: str,
        unwind_prefix: Optional[str],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising MongoAtlasToPostgresViaDataframeOperator")
        self.mongo_jdbc_conn_id = mongo_jdbc_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.preoperation = preoperation
        self.sql = sql
        self.table = table
        self.destination_schema = destination_schema
        self.unwind_prefix = unwind_prefix
        self.output_encoding = sys.getdefaultencoding()
        self.log.info("Initialised MongoAtlasToPostgresViaDataframeOperator")

    def execute(self, context):
        try:
            self.log.info(f"Executing MongoAtlasToPostgresViaDataframeOperator {self.sql}")
            source_hook = BaseHook.get_hook(self.mongo_jdbc_conn_id)
            self.log.info("source_hook")
            destination_hook = BaseHook.get_hook(self.postgres_conn_id)
            self.log.info("destination_hook")
            jdbc_conn = source_hook.get_conn()
            self.log.info("jbc_conn")
            MongoBsonValue = JPackage("com.mongodb.jdbc").MongoBsonValue
            self.log.info("MongoBsonValue")

            self.log.info("Extracting data from %s", self.mongo_jdbc_conn_id)
            self.log.info("Executing: \n %s", self.sql)

            engine = self.get_postgres_sqlalchemy_engine(destination_hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info(f"Ensuring {self.destination_schema} schema exists")
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.destination_schema};")  # noqa
                    star_sql = self.render_sql(cols="*", offset="0", limit="100")
                    self.log.info(f"post render SQL = {star_sql}")
                    df = pd.read_sql(star_sql, jdbc_conn)
                    columns_to_select = self.get_select_columns(df)
                    print(columns_to_select)
                    offset = 0
                    limit = 5000  # Set your desired chunk size
                    while True:
                        select_sql = self.render_sql(
                            cols=f"{', '.join(columns_to_select)}",
                            offset=str(offset),
                            limit=str(limit),
                        )
                        self.log.info("Select SQL: \n%s", select_sql)

                        select_df = pd.read_sql(select_sql, jdbc_conn)
                        if select_df.empty:
                            break  # Break the loop if no more data is returned

                        select_df = self.transform_mongo_data(select_df, MongoBsonValue)

                        self.log.info("Postgres URI: \n %s", destination_hook.get_uri())
                        select_df.to_sql(
                            self.table,
                            conn,
                            if_exists="replace",
                            schema=self.destination_schema,
                        )
                        offset += limit

                    conn.execute(f"ALTER TABLE {self.destination_schema}.{self.table} ADD PRIMARY KEY (id);")  # noqa

                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            # with conn.cursor() as cur:
            #     if self.preoperation:
            #         self.log.info("preoperation Detected, running\n\n %s \n\n", self.preoperation)
            #         cur.execute(self.preoperation)
            #     cur.copy_expert(self.sql, f_dest.name)
            #     conn.commit()

            self.log.info("import successful")
            return f"Successfully migrated {self.table} Data"
        except Exception as e:
            self.log.error(f"An error occurred: {e}")
            raise AirflowException(e)

    @staticmethod
    def _stringify(iterable: Iterable, joinable: str = "\n") -> str:
        """
        Takes an iterable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join([json.dumps(doc, default=json_util.default) for doc in iterable])

    # def bsonToString(self, element):
    #     """
    #     Convert MongoBsonValue to string, if applicable.
    #     """
    #     if isinstance(element, MongoBsonValue):
    #         return element.toString()
    #     return element

    def objectid_to_string(self, element, MongoBsonValue):
        """
        Convert MongoBsonValue $oid to string, if applicable.
        """
        if isinstance(element, MongoBsonValue):
            return json.loads(element.toString())["$oid"]
        return element

    def contains_mongo_bson_value(self, column, MongoBsonValue):
        return any(isinstance(x, MongoBsonValue) for x in column)

    def render_sql(self, cols="*", limit="5000", offset="0"):
        return self.sql.format(cols=cols, limit=limit, offset=offset)

    def get_select_columns(self, df):
        columns = []
        for col in df.columns:
            self.log.debug(f"Checking if col {col} is eligible for querying")
            # if self.contains_mongo_bson_value(df[col], MongoBsonValue):
            #     continue
            if not self.unwind_prefix == "":
                if not col.startswith(self.unwind_prefix):
                    self.log.debug(f"ignoring column {col} because it starts with {self.unwind_prefix}")
                    continue
            self.log.debug(f"ok adding col {col} for querying")
            columns.append(col)

        # columns.append("_id")  # add this one back

        return columns

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

    def _handle_bson_value(self, element, MongoBsonValue):
        """
        Convert MongoBsonValue $oid to string, if applicable.
        """
        if isinstance(element, MongoBsonValue):
            bson = json.loads(element.toString())
            if "$oid" in bson:
                element = bson["$oid"]
            else:
                element = json.dumps(bson)
            # oid
            # array
            # object
        return element

    def transform_mongo_data(self, df, MongoBsonValue):
        # df["id"] = df["_id"].apply(
        #     self.objectid_to_string, args=[MongoBsonValue]
        # )

        # convert _id BSON to id string and drop the _id BSON
        # df["id"] = df["id"].astype("string")
        # df.drop(columns=["_id"], inplace=True)

        for col in df.columns:
            if self.contains_mongo_bson_value(df[col], MongoBsonValue):
                df[col] = df[col].apply(self._handle_bson_value, args=[MongoBsonValue])
                # speculating df[col] = df[col].astype("json") I made this up
                # research suggests that the return "type" from _handle_bson_value
                # is enough to tell pandas what the dtype is.

        df.rename(columns={"_id": "id"}, inplace=True)

        return df

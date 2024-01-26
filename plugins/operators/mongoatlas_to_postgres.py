#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import re
import sys
from typing import Iterable, Optional

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from bson import json_util
from jpype import JPackage
from sqlalchemy import create_engine

# from jpype.types import *


class MongoAtlasToPostgresViaDataframeOperator(BaseOperator):
    """
    :param mongo_jdbc_conn_id: The Mongo JDBC connection id
    :type mongo_jdbc_conn_id: str
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param preoperator: sql statement to be executed prior to loading the data. (templated)
    :type preoperator: Optional[str]
    :param sql: SQL query to execute against the source database. (templated)
    :type sql: str
    :param table: Destination Table name
    :type table: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    """

    template_fields = ("sql", "preoperator")
    template_ext = ".sql"

    template_fields_renderers = {"preoperator": "sql", "sql": "sql"}
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        mongo_jdbc_conn_id: str = "mongo_jdbc_conn_id",
        postgres_conn_id: str = "postgres_conn_id",
        preoperator: Optional[str] = None,
        sql: str,
        table: str,
        destination_schema: str,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.log.info("Initialising MongoAtlasToPostgresViaDataframeOperator")
        self.mongo_jdbc_conn_id = mongo_jdbc_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.preoperator = preoperator
        self.sql = sql
        self.table = table
        self.destination_schema = destination_schema
        self.output_encoding = sys.getdefaultencoding()
        self.log.info("Initialised MongoAtlasToPostgresViaDataframeOperator")

    def execute(self, context):
        try:

            self.log.info("Executing MongoAtlasToPostgresViaDataframeOperator")
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
                    star_sql = self.render_sql(cols="*", offset="0", limit="100")
                    df = pd.read_sql(star_sql, jdbc_conn)
                    # df["id"] = df["_id"].apply(self.objectid_to_string, args=[MongoBsonValue])
                    # df["id"] = df["id"].astype("string")
                    columns_to_select = [
                        col
                        for col in df.columns
                        if not self.contains_mongo_bson_value(df[col], MongoBsonValue)
                    ]

                    select_sql = self.render_sql(
                        cols=f"_id, {', '.join(columns_to_select)}",
                        offset="0",
                        limit="5000",
                    )
                    self.log.info("select_sql: \n %s", select_sql)

                    select_df = pd.read_sql(select_sql, jdbc_conn)
                    select_df["id"] = select_df["_id"].apply(
                        self.objectid_to_string, args=[MongoBsonValue]
                    )

                    # convert _id BSON to id string and drop the _id BSON
                    select_df["id"] = select_df["id"].astype("string")
                    select_df.drop(columns=["_id"], inplace=True)

                    self.log.info("Postgres URI: \n %s", destination_hook.get_uri())
                    select_df.to_sql(
                        self.table,
                        conn,
                        if_exists="replace",
                        schema=self.destination_schema,
                    )
                    conn.execute(
                        f"ALTER TABLE {self.destination_schema}.{self.table} ADD PRIMARY KEY (id);"
                    )

                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(
                        f"Database operation failed Rolling Back: {e}"
                    )

            # with conn.cursor() as cur:
            #     if self.preoperator:
            #         self.log.info("preoperator Detected, running\n\n %s \n\n", self.preoperator)
            #         cur.execute(self.preoperator)
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
        return joinable.join(
            [json.dumps(doc, default=json_util.default) for doc in iterable]
        )

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

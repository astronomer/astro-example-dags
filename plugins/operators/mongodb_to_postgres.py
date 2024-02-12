import re
import sys
import json
from pprint import pprint  # noqa
from typing import List, Iterable, Optional

import bsonjs
import pandas as pd
from bson import json_util
from jinja2 import Template
from pandas import DataFrame, json_normalize
from dateutil import parser
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from utils.json_schema_to_dataframe import json_schema_to_dataframe

pd.set_option("display.max_rows", None)  # or a large number instead of None
pd.set_option("display.max_columns", None)  # Display any number of columns
pd.set_option("display.max_colwidth", None)  # Display full width of each column
pd.set_option("display.width", None)  # Use maximum width available


class MongoDBToPostgresViaDataframeOperator(BaseOperator):
    """
    :param mongo_conn_id: The Mongo JDBC connection id
    :type mongo_conn_id: str
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param preoperation: sql statement to be executed prior to loading the data. (templated)
    :type preoperation: Optional[str]
    :param aggregation_query: JSON query to execute against the source collection. (templated)
    :type aggregation_query: str
    :param source_database: MongoDB Source Database name
    :type source_database: str
    :param source_collection: MongoDB Source Collection name
    :type source_collection: str
    :param destination_table: Destination Table name
    :type destination_table: str
    :param destination_schema: Destination Schema name
    :type destination_schema: str
    :param jsonschema: Source collection JsonSchema name
    :type jsonschema: str
    :param unwind: Unwind key
    :type unwind: str
    """

    template_fields = ("aggregation_query", "preoperation")
    template_ext = (".json", ".sql")

    template_fields_renderers = {"preoperation": "sql", "aggregation_query": "json"}
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        mongo_conn_id: str = "mongo_db_conn_id",
        postgres_conn_id: str = "postgres_conn_id",
        preoperation: Optional[str] = None,
        aggregation_query: str,
        source_collection: str,
        source_database: str,
        jsonschema: str,
        destination_table: str,
        destination_schema: str,
        unwind: Optional[str] = None,
        preserve_fields: Optional[List[str]] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising MongoAtlasToPostgresViaDataframeOperator")
        self.mongo_conn_id = mongo_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.preoperation = preoperation
        self.aggregation_query = aggregation_query
        self.source_collection = source_collection
        self.source_database = source_database
        self.jsonschema = jsonschema
        self.destination_table = destination_table
        self.destination_schema = destination_schema
        self.unwind = unwind
        self.preserve_fields = preserve_fields
        self.output_encoding = sys.getdefaultencoding()

        self.log.info("Initialised MongoAtlasToPostgresViaDataframeOperator")

    def execute(self, context):
        try:
            self.log.info("Executing MongoAtlasToPostgresViaDataframeOperator")
            mongo_hook = BaseHook.get_hook(self.mongo_conn_id)
            self.log.info("source_hook")
            destination_hook = BaseHook.get_hook(self.postgres_conn_id)
            self.log.info("destination_hook")

            self.log.info("Extracting data from %s", self.mongo_conn_id)
            self.log.info("Executing: \n %s", self.aggregation_query)

            self.flattened_schema = json_schema_to_dataframe(self.jsonschema, start_key=self.unwind)

            print("FLATTENED_SCHEMA", self.flattened_schema)
            engine = self.get_postgres_sqlalchemy_engine(destination_hook)
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self.log.info(f"Ensuring {self.destination_schema} schema exists")
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.destination_schema};")  # noqa
                    offset = 0
                    limit = 5000  # Set your desired chunk size

                    aggregation_query = json.loads(self.aggregation_query)

                    while True:

                        aggregation_query[-1] = {"$limit": limit}
                        aggregation_query[-2] = {"$skip": offset}
                        aggregation_query[0]["$match"]["updatedAt"]["$lte"] = context["data_interval_end"]
                        if context["prev_data_interval_start_success"]:
                            aggregation_query[0]["$match"]["updatedAt"]["$gte"] = context[
                                "prev_data_interval_start_success"
                            ]
                        else:
                            if "$gte" in aggregation_query[0]["$match"]["updatedAt"]:
                                del aggregation_query[0]["$match"]["updatedAt"]["$gte"]

                        self.log.info("Select SQL: \n%s", aggregation_query)
                        pprint(aggregation_query)

                        collection = mongo_hook.get_collection(self.source_collection)

                        results = collection.aggregate_raw_batches(pipeline=aggregation_query)
                        documents = [json.loads(bsonjs.dumps(doc)) for doc in results]
                        select_df = DataFrame(list(documents))

                        if select_df.empty:
                            self.log.info("No More Results, Data Selection is empty")
                            break  # Break the loop if no more data is returned
                        pprint(documents[0])

                        select_df = select_df.applymap(self.convert_and_prepare_mongo_structures)

                        pprint(select_df.iloc[0])
                        select_df = self.flatten_nested_columns(select_df)

                        pprint(select_df.iloc[0])
                        self.log.info("Postgres URI: \n %s", destination_hook.get_uri())
                        insert_df = self.align_to_schema_df(select_df)
                        insert_df.to_sql(
                            self.destination_table,
                            conn,
                            if_exists="append",
                            schema=self.destination_schema,
                        )
                        offset += limit

                    conn.execute(
                        f"ALTER TABLE {self.destination_schema}.{self.destination_table} ADD PRIMARY KEY (id);"  # noqa
                    )  # noqa

                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            # with conn.cursor() as cur:
            #     if self.preoperation:
            #         self.log.info("preoperation Detected, running\n\n %s \n\n", self.preoperation)
            #         cur.execute(self.preoperation)
            #     cur.copy_expert(self.aggregation_query, f_dest.name)
            #     conn.commit()

            self.log.info("import successful")
            return f"Successfully migrated {self.destination_table} Data"
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

    def convert_and_prepare_mongo_structures(self, element):
        # Convert ObjectId and Date
        if isinstance(element, dict):
            if "$oid" in element:
                return str(element["$oid"])
            elif "$date" in element:
                # Parse the date string; dateutil.parser.parse automatically detects timezone
                parsed_date = parser.parse(element["$date"])
                # If the parsed date is timezone-aware, return it directly
                if parsed_date.tzinfo is not None:
                    return pd.Timestamp(parsed_date)
                else:
                    # If it's naive, assume UTC or any other default timezone if required
                    # Alternatively, return without timezone
                    # return pd.Timestamp(parsed_date, tz="UTC")  # or tz=None for naive
                    # just keep it as a string
                    if element["$date"] == "":
                        return None
                    return element["$date"]
        elif isinstance(element, list):
            return json.dumps(element)

        return element

    def align_to_schema_df(self, df):

        combined_columns = list(self.flattened_schema.keys()) + self.preserve_fields

        insert_df = df.reindex(columns=combined_columns, fill_value=None)
        for column, (dtype, *rest) in self.flattened_schema.items():
            if column in insert_df.columns:
                insert_df[column] = insert_df[column].astype(dtype)

        return insert_df

    def flatten_nested_columns(self, df, column_prefix=""):
        for column in df.columns:
            if any(isinstance(x, dict) for x in df[column]):
                flattened = json_normalize(df[column])
                flattened.columns = [f"{column_prefix}{column}_{subcol}" for subcol in flattened.columns]
                df = df.drop(column, axis=1).join(flattened)
        # df.rename(columns={"_id": "id"}, inplace=True)
        return df

    def get_json_column_type(self, column):
        for x in column:
            if isinstance(x, dict):
                if "$oid" in x:
                    return "oid"
                if "$date" in x:
                    return "date"
                return "dict"
            elif isinstance(x, list):
                return "list"
            # Add any other specific type checks here if needed
        # Default return type if none of the above match
        return "string"

    def render_aggregation_query(self, context, limit, offset):

        new_context = {
            "limit": limit,
            "offset": offset,
        }

        # Merge the new variables with the existing context to keep Airflow variables accessible
        combined_context = {**context, **new_context}

        from pprint import pprint

        pprint(combined_context)
        # Manually render the template with the combined context
        jinja_template = Template(self.aggregation_query)
        output = jinja_template.render(**combined_context)

        return json.loads(output)

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

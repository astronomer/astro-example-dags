import re
import sys
import json
from pprint import pprint  # noqa
from typing import List, Iterable, Optional
from datetime import datetime

import pandas as pd
from bson import ObjectId, json_util
from jinja2 import Template
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from plugins.utils.field_conversions import convert_field
from plugins.utils.json_schema_to_dataframe import json_schema_to_dataframe

pd.set_option("display.max_rows", 10)  # or a large number instead of None
pd.set_option("display.max_columns", None)  # Display any number of columns
pd.set_option("display.max_colwidth", 80)  # Display full width of each column
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
    :param unwind: Field to Unwind
    :type unwind: str
    :param preserve_fields: Fields that you create during the aggregation stage that you want to keep, but don't exist in the json Schema  # noqa
    :type preserve_fields: Optional[List[str]]
    :param discard_fields: Fields that you don't want to keep that despite them existing in the json Schema
    :type discard_fields: Optional[List[str]]
    :param convert_fields: Fields that you want to apply a complex conversion to using a named function
    :type convert_fields: Optional[List[str]]
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
        discard_fields: Optional[List[str]] = [],
        convert_fields: Optional[List[str]] = [],
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
        self.preserve_fields = preserve_fields or []
        self.discard_fields = discard_fields or []
        self.convert_fields = convert_fields or []
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

            self._prepare_schema()
            engine = self.get_postgres_sqlalchemy_engine(destination_hook)
            primary_key = "id"
            print(f"PRIMARY_KEY=={primary_key}")
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    offset = 0
                    limit = 5000  # Set your desired chunk size
                    ds = context["ds"]

                    aggregation_query = self._prepare_aggregation_query()

                    while True:
                        aggregation_query = self._prepare_runtime_aggregation_query(
                            aggregation_query, limit, offset, context
                        )

                        self.log.info("Select SQL: \n%s", aggregation_query)
                        pprint(aggregation_query)

                        collection = mongo_hook.get_collection(self.source_collection)

                        cursor = collection.aggregate(
                            pipeline=aggregation_query,
                        )
                        documents = list(cursor)
                        # documents = []
                        # while True:
                        #     try:
                        #         documents.extend([x for x in decode_all(cursor.next())])
                        #     except StopIteration:
                        #         break

                        print("TOTAL docs after", len(documents))
                        select_df = DataFrame(list(documents))
                        print("TOTAL df", select_df.shape)

                        if select_df.empty:
                            self.log.info("No More Results, Data Selection is empty")
                            break  # Break the loop if no more data is returned

                        print(documents[0])
                        if self.discard_fields:
                            # keep this because if we're dropping any problematic fields
                            # from the top level we might want to do this before Flattenning
                            existing_discard_fields = [col for col in self.discard_fields if col in select_df.columns]
                            select_df.drop(existing_discard_fields, axis=1, inplace=True)

                        print("TOTAL AFTER DISCARD df", select_df.shape)
                        pprint(documents[0])

                        select_df = self.flatten_dataframe_columns_precisely(select_df, separator="__")

                        print("TOTAL AFTER FLATTEN df", select_df.shape)
                        print("FINAL COLUMNS", select_df.columns)
                        self.log.info("Postgres URI: \n %s", destination_hook.get_uri())
                        insert_df = self.align_to_schema_df(select_df)
                        print("TOTAL AFTER ALIGNMENT df", insert_df.shape)
                        print("ALIGNED COLUMNS", insert_df.columns)

                        null_id_records = insert_df[insert_df[primary_key].isnull()]
                        if not null_id_records.empty:
                            print(f"Records with null primary key field '{primary_key}':")
                            print("NULL ID", null_id_records.tolist())

                        # Add the ds value for this run
                        insert_df["airflow_synced_at"] = ds
                        pprint(insert_df.iloc[0])
                        insert_df.to_sql(
                            self.destination_table,
                            conn,
                            if_exists="append",
                            schema=self.destination_schema,
                            index=False,
                        )
                        offset += limit

                    conn.execute(
                        f"ALTER TABLE {self.destination_schema}.{self.destination_table} ADD PRIMARY KEY ({primary_key});"  # noqa
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

    def flatten_dict(self, d, parent_key="", separator="__"):
        """Recursively flatten nested dictionaries."""
        # print("flatten_dict called on ", parent_key, d)
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            if new_key in self.discard_fields:
                continue
            if isinstance(v, list):
                # Convert lists directly to JSON strings
                # print("Handling list", new_key, k, v)
                items[new_key] = json_util.dumps(v)
            elif isinstance(v, ObjectId):
                # print("Handling ObjectId", new_key, k, v)
                items[new_key] = str(v)
            elif isinstance(v, datetime):
                # print("Handling datetime", new_key, k, v)
                items[new_key] = pd.Timestamp(v)
            elif isinstance(v, dict):
                # print("Handling dict", new_key, k, v)
                items.update(
                    self.flatten_dict(
                        v,
                        parent_key=new_key,
                        separator=separator,
                    )
                )
            else:
                # print("Handling preserve", new_key, k, v)
                items[new_key] = v
        # print("items dict", items)
        return items

    def flatten_dataframe_columns_precisely(self, df, separator="__"):
        """Flatten all dictionary columns in a DataFrame and handle non-dict items."""
        flattened_data = pd.DataFrame()
        for column in df.columns:
            # Initialize a container for processed data
            column_data = []
            is_dict_column = df[column].apply(lambda x: isinstance(x, dict)).any()
            is_list_column = df[column].apply(lambda x: isinstance(x, list)).any()
            is_objectid_column = df[column].apply(lambda x: isinstance(x, ObjectId)).any()
            is_date_column = df[column].apply(lambda x: isinstance(x, datetime)).any()

            if is_objectid_column:
                # print("Handling ObjectId Top level column")
                column_df = df[column].apply(str).to_frame(name=column)
            elif is_date_column:
                # print("Handling datetime Top level column")
                column_df = df[column].apply(pd.Timestamp).to_frame(name=column)
            elif is_dict_column:
                for item in df[column]:
                    # Process dictionary items
                    if isinstance(item, dict):
                        # print("COLUMN item dict", column, item)
                        flattened_item = self.flatten_dict(
                            item,
                            separator=separator,
                        )
                        column_data.append(flattened_item)
                    else:
                        # For items that are not dicts (e.g., missing or null values), ensure compatibility
                        column_data.append({})
                # Normalize the processed column data
                column_df = pd.json_normalize(column_data)
                # Rename columns to ensure they are prefixed correctly
                column_df.columns = [
                    f"{column}{separator}{subcol}" if not subcol == "PARENT_COLUMN" else column
                    for subcol in column_df.columns
                ]

            elif is_list_column:
                # print("COLUMN item list", column)
                column_df = df[column].apply(json_util.dumps).to_frame(name=column)
            else:  # Directly append non-dict and non-list items
                # print("COLUMN item preserve", column)
                column_df = df[column].to_frame()

            # Concatenate the new column DataFrame to the flattened_data DataFrame
            flattened_data = pd.concat([flattened_data, column_df], axis=1)

        return flattened_data

    # def convert_and_prepare_mongo_structures(self, element):
    #     # Convert ObjectId and Date
    #     if isinstance(element, dict):
    #         if "$oid" in element:
    #             return str(element["$oid"])
    #         elif "$date" in element:
    #             # Parse the date string; dateutil.parser.parse automatically detects timezone
    #             parsed_date = parser.parse(element["$date"])
    #             # If the parsed date is timezone-aware, return it directly
    #             if parsed_date.tzinfo is not None:
    #                 return pd.Timestamp(parsed_date)
    #             else:
    #                 # If it's naive, assume UTC or any other default timezone if required
    #                 # Alternatively, return without timezone
    #                 # return pd.Timestamp(parsed_date, tz="UTC")  # or tz=None for naive
    #                 # just keep it as a string
    #                 if element["$date"] == "":
    #                     return None
    #                 return element["$date"]
    #     elif isinstance(element, list):
    #         return json.dumps(element)

    #     return element

    def _prepare_schema(self):
        self.log.info("Preparing Schema")
        self._flattened_schema = json_schema_to_dataframe(
            self.jsonschema,
            start_key=self.unwind,
            discard_fields=self.discard_fields,
        )
        if "_id" in self._flattened_schema:
            # We will ignore this field
            del self._flattened_schema["_id"]

        if "id" not in self._flattened_schema:
            # The aggregation query should always produce an "id" field
            # but that field won't exist in the jsonschemas
            # for now we'll assume it'll be a string and see how it goes
            self._flattened_schema["id"] = ("string", None)

        # print("FLATTENED_SCHEMA", self._flattened_schema)
        # Step 1: Combine and preserve columns ensuring no duplicates
        combined_columns = list(self._flattened_schema.keys())
        if self.preserve_fields:
            combined_columns += [field for field in self.preserve_fields if field not in combined_columns]

        # # Step 2: Exclude discard fields
        # if self.discard_fields:
        #     combined_columns = [column for column in combined_columns if column not in self.discard_fields]

        print("COMBINED_COLUMNS", combined_columns)
        self._combined_columns = combined_columns

    def align_to_schema_df(self, df):

        # Step 3: Reindex DataFrame to adjust columns, using 'None' for any new columns' fill value
        insert_df = df.reindex(columns=self._combined_columns, fill_value=None)

        for column, (dtype, *rest) in self._flattened_schema.items():
            if column in insert_df.columns:
                print(f"aligning column {column} as type {dtype}")
                insert_df[column] = insert_df[column].astype(dtype)

        return insert_df

    # def flatten_nested_columns(self, df, column_prefix=""):
    #     for column in df.columns:
    #         if any(isinstance(x, dict) for x in df[column]):
    #             flattened = json_normalize(df[column])
    #             flattened.columns = [
    #                 f"{column_prefix}{column}_{subcol}" for subcol in flattened.columns
    #             ]
    #             df = df.drop(column, axis=1).join(flattened)
    #     # df.rename(columns={"_id": "id"}, inplace=True)
    #     return df

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

    def _prepare_aggregation_query(self):
        aggregation_query = json.loads(self.aggregation_query)
        for function_config in self.convert_fields:
            function_name = function_config["function"]
            fields = function_config["fields"]
            function_arguments = function_config["function_args"]
            aggregation_stage = function_config["aggregation_stage"]
            if "$set" not in aggregation_query[aggregation_stage]:
                aggregation_query.insert(aggregation_stage, {"$set": {}})
            for field_name in fields:
                result = convert_field(function_name, field_name, *function_arguments)
                aggregation_query[aggregation_stage]["$set"].update(result)

        return aggregation_query

    def _prepare_runtime_aggregation_query(self, aggregation_query, limit, offset, context):
        aggregation_query[-1] = {"$limit": limit}
        aggregation_query[-2] = {"$skip": offset}
        aggregation_query[0]["$match"]["updatedAt"]["$lte"] = context["data_interval_end"]
        if context["prev_data_interval_start_success"]:
            aggregation_query[0]["$match"]["updatedAt"]["$gte"] = context["prev_data_interval_start_success"]
        else:
            if "$gte" in aggregation_query[0]["$match"]["updatedAt"]:
                del aggregation_query[0]["$match"]["updatedAt"]["$gte"]

        return aggregation_query

import re
import sys
import json
from pprint import pprint  # noqa
from typing import Dict, List, Iterable, Optional
from datetime import datetime

import pandas as pd
from bson import ObjectId, json_util
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from bson.codec_options import CodecOptions
from airflow.utils.session import provide_session

from plugins.utils.render_template import render_template
from plugins.utils.field_conversions import convert_field
from plugins.utils.detect_duplicate_columns import detect_duplicate_columns
from plugins.utils.json_schema_to_flattened_numpy_datatypes import json_schema_to_flattened_numpy_datatypes

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

    # we manually render the aggregation_query field
    template_fields = "preoperation"
    template_ext = (".json", ".sql")

    template_fields_renderers = {"preoperation": "sql"}
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
        preserve_fields: Optional[Dict] = {},
        discard_fields: Optional[List[str]] = [],
        convert_fields: Optional[List[str]] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising MongoDBToPostgresViaDataframeOperator")
        self.mongo_conn_id = mongo_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.preoperation = preoperation or "DELETE FROM {{ destination_schema }}.{{destination_table}};"
        self.aggregation_query = aggregation_query
        self.source_collection = source_collection
        self.source_database = source_database
        self.jsonschema = jsonschema
        self.destination_table = destination_table
        self.destination_schema = destination_schema
        self.unwind = unwind
        self.preserve_fields = preserve_fields or {}
        self.discard_fields = discard_fields or []
        self.convert_fields = convert_fields or []
        self.output_encoding = sys.getdefaultencoding()
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.separator = "__"
        # We're removing the WHERE in the DELETE function as if we're playing catchup
        # duplicates could exist in older records. We can do this because we only allow 1
        # concurrent task...

        self.delete_template = """DO $$
BEGIN
   IF EXISTS (
    SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
    AND tablename = '{{destination_table}}') THEN
      {{ preoperation }}
   END IF;
END $$;
"""
        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
            "preoperation": preoperation,
        }

        self.log.info("Initialised MongoDBToPostgresViaDataframeOperator")

    def execute(self, context):
        try:
            ds = context["ds"]
            run_id = context["run_id"]
            last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
            extra_context = {
                **context,
                **self.context,
                f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
            }

            self.log.info(
                f"Executing MongoDBToPostgresViaDataframeOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
            )
            mongo_hook = BaseHook.get_hook(self.mongo_conn_id)
            self.log.info("source_hook")
            destination_hook = BaseHook.get_hook(self.postgres_conn_id)
            self.log.info("destination_hook")

            self.log.info("Extracting data from %s", self.mongo_conn_id)
            self.log.info("Executing: \n %s", self.aggregation_query)
            self.delete_sql = render_template(self.delete_template, context=extra_context)

            self._prepare_schema()
            engine = self.get_postgres_sqlalchemy_engine(destination_hook)
            primary_key = "id"
            total_docs_processed = 0
            print(f"PRIMARY_KEY=={primary_key}")
            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    offset = 0
                    limit = 5000  # Set your desired chunk size

                    aggregation_query = self._prepare_aggregation_query()
                    self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
                    conn.execute(self.delete_sql)

                    while True:
                        aggregation_query = self._prepare_runtime_aggregation_query(
                            aggregation_query, limit, offset, extra_context
                        )

                        self.log.info("Select SQL: \n%s", aggregation_query)
                        pprint(aggregation_query)

                        mongo_conn = mongo_hook.get_conn()
                        codec_options = CodecOptions(tz_aware=True)

                        # we do this because the mongo_hook.get_collection doesn't let you pass codec_options
                        collection = mongo_conn.get_database(mongo_hook.connection.schema).get_collection(
                            self.source_collection, codec_options=codec_options
                        )

                        cursor = collection.aggregate(
                            pipeline=aggregation_query,
                        )
                        documents = list(cursor)
                        total_docs_processed += len(documents)

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

                        select_df = self.flatten_dataframe_columns_precisely(select_df)

                        print("TOTAL AFTER FLATTEN df", select_df.shape)
                        print("FINAL COLUMNS", select_df.columns)
                        detect_duplicate_columns(select_df.columns)
                        self.log.info("Postgres URI: \n %s", destination_hook.get_uri())
                        insert_df = self.align_to_schema_df(select_df)
                        print("TOTAL AFTER ALIGNMENT df", insert_df.shape)
                        print("ALIGNED COLUMNS", insert_df.columns)

                        null_id_records = insert_df[insert_df[primary_key].isnull()]
                        if not null_id_records.empty:
                            print(f"Records with null primary key field '{primary_key}':")
                            print("NULL ID", null_id_records.tolist())

                        # Add the ds value for this run
                        insert_df["airflow_sync_ds"] = ds
                        # Make all column names lowercase
                        insert_df.columns = insert_df.columns.str.lower()
                        pprint(insert_df.iloc[0])
                        insert_df.to_sql(
                            self.destination_table,
                            conn,
                            if_exists="append",
                            schema=self.destination_schema,
                            index=False,
                        )
                        offset += limit

                    # Check how many Docs total
                    if total_docs_processed > 0:
                        conn.execute(
                            f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE n.nspname = '{self.destination_schema}'  -- Schema name
        AND c.relname = '{self.destination_table}'  -- Table name
        AND ic.relname = '{self.destination_table}_idx'  -- Index name
    ) THEN
        ALTER TABLE {self.destination_schema}.{self.destination_table}
            ADD CONSTRAINT {self.destination_table}_idx PRIMARY KEY (id);
    END IF;
END $$;
"""  # noqa
                        )  # noqa
                    context["ti"].xcom_push(key=self.last_successful_dagrun_xcom_key, value=context["ts"])
                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
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

    def flatten_dataframe_columns_precisely(self, df):
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
                            separator=self.separator,
                        )
                        column_data.append(flattened_item)
                    else:
                        # For items that are not dicts (e.g., missing or null values), ensure compatibility
                        column_data.append({})
                # Normalize the processed column data
                column_df = pd.json_normalize(column_data)
                # Rename columns to ensure they are prefixed correctly
                column_df.columns = [
                    (f"{column}{self.separator}{subcol}" if not subcol == "PARENT_COLUMN" else column)
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
        self._flattened_schema = json_schema_to_flattened_numpy_datatypes(
            self.jsonschema,
            start_key=self.unwind,
            discard_fields=self.discard_fields,
            preserve_fields=self.preserve_fields,
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
            combined_columns += [field for field, v in self.preserve_fields.items() if field not in combined_columns]

        # # Step 2: Exclude discard fields
        # if self.discard_fields:
        #     combined_columns = [column for column in combined_columns if column not in self.discard_fields]

        print("COMBINED_COLUMNS", combined_columns)
        self._schema_columns = [col.lower() for col in combined_columns]
        self._schema_columns = sorted(self._schema_columns)
        print("SCHEMA COLUMNS LOWERCASE", combined_columns)

    def align_to_schema_df(self, df):

        # ensure existing df columns are lowered to match the schemas lowercase'd columns
        df.columns = df.columns.str.lower()
        print("Align columns duplication detection")
        print("Checking schema columns")
        detect_duplicate_columns(self._schema_columns)
        print("Checking ResultsSet columns", list(df.columns))
        detect_duplicate_columns(df.columns)
        insert_df = df.reindex(columns=self._schema_columns, fill_value=None)
        print(self._flattened_schema.items())
        print(insert_df.columns)

        for column, (dtype, *rest) in self._flattened_schema.items():
            print(f"column name = {column}")
            column = column.lower()
            print(f"column lower_name = {column}")
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

        extra_context = {
            "limit": limit,
            "offset": offset,
        }

        return json.loads(render_template(self.aggregation_query, context=context, extra_context=extra_context))

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
                result, dtype = convert_field(function_name, field_name, *function_arguments)
                aggregation_query[aggregation_stage]["$set"].update(result)
                flattened_name = self._convert_fieldname_to_flattened_name(field_name)
                self._flattened_schema[flattened_name] = dtype

        return aggregation_query

    def _prepare_runtime_aggregation_query(self, aggregation_query, limit, offset, context):
        aggregation_query[-1] = {"$limit": limit}
        aggregation_query[-2] = {"$skip": offset}
        last_successful_dagrun_ts = context.get(self.last_successful_dagrun_xcom_key, None)
        if "updatedAt" in aggregation_query[0]["$match"]:
            if "$lte" in aggregation_query[0]["$match"]["updatedAt"]:
                aggregation_query[0]["$match"]["updatedAt"]["$lte"] = context["data_interval_end"]
            if last_successful_dagrun_ts:
                aggregation_query[0]["$match"]["updatedAt"]["$gt"] = last_successful_dagrun_ts
            else:
                if "$gt" in aggregation_query[0]["$match"]["updatedAt"]:
                    del aggregation_query[0]["$match"]["updatedAt"]["$gt"]

            if (
                "$gt" not in aggregation_query[0]["$match"]["updatedAt"]
                and "$lte" not in aggregation_query[0]["$match"]["updatedAt"]
            ):
                aggregation_query[0]["$match"] = {}

        return aggregation_query

    def _convert_fieldname_to_flattened_name(self, field_name):
        return field_name.replace(".", self.separator)

    @provide_session
    def get_last_successful_dagrun_ts(self, run_id, session=None):
        query = XCom.get_many(
            include_prior_dates=True,
            dag_ids=self.dag_id,
            run_id=run_id,
            task_ids=self.task_id,
            key=self.last_successful_dagrun_xcom_key,
            session=session,
            limit=1,
        )

        xcom = query.first()
        if xcom:
            return datetime.fromisoformat(xcom.value)

        return None

import re
import json
import urllib.parse
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.models.connection import Connection

from plugins.utils.render_template import render_template

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class ZettleToPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        zettle_conn_id,
        destination_schema,
        destination_table,
        *args,
        **kwargs,
    ):
        super(ZettleToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.zettle_conn_id = zettle_conn_id
        self.discard_fields = []
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.separator = "__"

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }
        self.delete_template = """DO $$
BEGIN
   IF EXISTS (
    SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
    AND tablename = '{{destination_table}}') THEN
      DELETE FROM {{ destination_schema }}.{{destination_table}}
        WHERE airflow_sync_ds = '{{ ds }}';
   END IF;
END $$;
"""

    def execute(self, context):

        # Fetch data from the database
        hook = BaseHook.get_hook(self.postgres_conn_id)
        zettle_conn = Connection.get_connection_from_secrets(self.zettle_conn_id)
        api_key = zettle_conn.password
        print(zettle_conn, zettle_conn.extra_dejson)
        grant_type = zettle_conn.extra_dejson.get("grant_type")
        client_id = zettle_conn.extra_dejson.get("client_id")
        token = self.get_zettle_access_token(grant_type, client_id, api_key)
        ds = context["ds"]
        run_id = context["run_id"]
        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)

        extra_context = {
            **context,
            **self.context,
            f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
        }

        self.delete_sql = render_template(self.delete_template, context=extra_context)

        self.log.info(
            f"Executing MongoDBToPostgresViaDataframeOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
        )

        engine = self.get_postgres_sqlalchemy_engine(hook)
        with engine.connect() as conn:

            self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
            conn.execute(self.delete_sql)

            lte = context["data_interval_end"].to_iso8601_string()
            total_docs_processed = 0
            limit = 100
            offset = 0

            # Base URL path
            base_url = "https://finance.izettle.com/v2/accounts/LIQUID/transactions"
            # Determine the 'start' parameter based on 'last_successful_dagrun_ts'
            start_param = last_successful_dagrun_ts if last_successful_dagrun_ts else "2016-08-01T00:00:00.000Z"

            while True:
                # Dictionary of query parameters
                query_params = {
                    "start": start_param,
                    "end": lte,
                    "limit": limit,
                    "offset": offset,
                }
                full_url = f"{base_url}?{urlencode(query_params)}"
                headers = {"Authorization": f"Bearer {token}"}

                response = requests.get(full_url, headers=headers)
                print(response.json())

                if response.status_code == 200:
                    records = response.json()
                else:
                    self.log.error("Error Retreiving Zettle transaction: %s", response)
                    raise AirflowException("Error getting Zettle Transactions")

                print(records)

                total_docs_processed += len(records)

                df = DataFrame(records)

                if df.empty:
                    self.log.info("No More Charges Data to process.")
                    break

                offset += limit

                self.log.info(f"Processing ResultSet {total_docs_processed} from batch {offset}.")
                df["airflow_sync_ds"] = ds

                if self.discard_fields:
                    # keep this because if we're dropping any problematic fields
                    # from the top level we might want to do this before Flattenning
                    existing_discard_fields = [col for col in self.discard_fields if col in df.columns]
                    df.drop(existing_discard_fields, axis=1, inplace=True)

                df = self.flatten_dataframe_columns_precisely(df)
                df.columns = df.columns.str.lower()

                df.to_sql(
                    self.destination_table,
                    conn,
                    if_exists="append",
                    schema=self.destination_schema,
                    index=False,
                )

            # Check how many Docs total
            if total_docs_processed > 0:
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {self.destination_table}_originatingtransactionuuid_idx_idx ON _originatingtransactionuuid_idx (originatingtransactionuuid);"  # noqa
                )

        context["ti"].xcom_push(
            key=self.last_successful_dagrun_xcom_key,
            value=context["data_interval_end"].to_iso8601_string(),
        )
        self.log.info("Zettle Charges written to Datalake successfully.")

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
                items[new_key] = json.dumps(v)
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
            is_date_column = df[column].apply(lambda x: isinstance(x, datetime)).any()

            if is_date_column:
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
                column_df = df[column].apply(json.dumps).to_frame(name=column)
            else:  # Directly append non-dict and non-list items
                # print("COLUMN item preserve", column)
                column_df = df[column].to_frame()

            # Concatenate the new column DataFrame to the flattened_data DataFrame
            flattened_data = pd.concat([flattened_data, column_df], axis=1)

        return flattened_data

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

    def get_zettle_access_token(self, grant_type, client_id, api_key):
        try:
            url = "https://oauth.izettle.com/token"
            data = {
                "grant_type": grant_type,
                "client_id": client_id,
                "assertion": api_key,
            }
            headers = {
                "content-type": "application/x-www-form-urlencoded",
            }
            response = requests.post(url, data=urllib.parse.urlencode(data), headers=headers)

            if response.status_code == 200 and response.json():
                data = response.json()
                return data.get("access_token")
        except Exception as e:
            self.log.error("Error during database operation: %s", e)
            raise AirflowException(f"Error getting Zettle Access Token: {e}")

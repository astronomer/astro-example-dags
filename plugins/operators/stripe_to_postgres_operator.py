import re
import json
from datetime import datetime

import pandas as pd
import stripe
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.models.connection import Connection

from plugins.utils.render_template import render_template

from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class StripeToPostgresOperator(DagRunTaskCommsMixin, BaseOperator):
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        stripe_conn_id,
        destination_schema,
        destination_table,
        *args,
        **kwargs,
    ):
        super(StripeToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.stripe_conn_id = stripe_conn_id
        self.discard_fields = ["payment_method_details", "source"]
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_transaction_key = "last_successful_transaction_id"
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
        stripe_conn = Connection.get_connection_from_secrets(self.stripe_conn_id)
        stripe.api_key = stripe_conn.password
        ds = context["ds"]
        run_id = context["run_id"]
        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)

        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            self.ensure_task_comms_table_exists(conn)
            starting_after = self.get_last_successful_transaction_id(conn, context)
            extra_context = {
                **context,
                **self.context,
                f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
            }

            self.log.info(
                f"Executing StripeToPostgresOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
            )

            if starting_after:
                self.log.info(
                    f"StripeToPostgresOperator Restarting task for this Dagrun from the last successful transaction_id {starting_after} after last successful dagrun {last_successful_dagrun_ts}"  # noqa
                )
            else:
                self.log.info(
                    f"StripeToPostgresOperator Starting Task Fresh for this dagrun from {last_successful_dagrun_ts}"  # noqa
                )
                self.log.info(f"StripeToPostgresOperator Deleting previous Data from this Dagrun")  # noqa
                self.delete_sql = render_template(self.delete_template, context=extra_context)
                self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
                conn.execute(self.delete_sql)

            created = {
                "gt": last_successful_dagrun_ts or 1690898262,
                "lte": context["data_interval_end"].int_timestamp,
            }
            total_docs_processed = 0

            limit = 100
            has_more = True

            while has_more:
                print(created, starting_after, limit)
                result = stripe.BalanceTransaction.list(
                    limit=limit,
                    starting_after=starting_after,
                    created=created,
                    expand=["data.source"],
                )

                records = result.data
                has_more = result.has_more
                total_docs_processed += len(records)

                df = DataFrame(records)

                if df.empty:
                    self.log.info("UNEXPECTED EMPTY Balance Transactions to process.")
                    break

                df["source_id"] = df["source"].apply(lambda x: x.get("id") if x is not None else None)
                df["source_invoice_id"] = df["source"].apply(lambda x: x.get("invoice") if x is not None else None)
                df["source_charge_id"] = df["source"].apply(lambda x: x.get("charge") if x is not None else None)
                df.drop(columns=["source"], inplace=True)

                starting_after = records[-1].id
                self.log.info(f"Processing ResultSet {total_docs_processed} - {starting_after}.")
                df["airflow_sync_ds"] = ds
                print(records[0])

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
                self.set_last_successful_transaction_id(conn, context, starting_after)

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
"""
                )

            self.clear_task_vars(conn, context)
        context["ti"].xcom_push(key="documents_found", value=total_docs_processed)
        self.set_last_successful_dagrun_ts(context, context["data_interval_end"].int_timestamp)
        self.log.info("Stripe Charges written to Datalake successfully.")

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

    def set_last_successful_transaction_id(self, conn, context, starting_after):
        return self.set_task_var(conn, context, self.last_successful_transaction_key, starting_after)

    def get_last_successful_transaction_id(self, conn, context):
        return self.get_task_var(conn, context, self.last_successful_transaction_key)

    def set_last_successful_dagrun_ts(self, context, value):
        context["ti"].xcom_push(key=self.last_successful_dagrun_xcom_key, value=value)

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
            return xcom.value

        return None

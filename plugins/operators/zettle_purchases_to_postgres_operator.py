import re
from datetime import datetime
from urllib.parse import urlencode

import requests
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from dateutil.relativedelta import relativedelta
from airflow.utils.decorators import apply_defaults
from airflow.models.connection import Connection

from plugins.utils.render_template import render_template

from plugins.operators.mixins.zettle import ZettleMixin
from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin
from plugins.operators.mixins.dag_run_task_comms_mixin import DagRunTaskCommsMixin

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class ZettlePurchasesToPostgresOperator(DagRunTaskCommsMixin, FlattenJsonDictMixin, ZettleMixin, BaseOperator):
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
        super(ZettlePurchasesToPostgresOperator, self).__init__(*args, **kwargs)
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.postgres_conn_id = postgres_conn_id
        self.zettle_conn_id = zettle_conn_id
        self.discard_fields = ["references"]
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.last_successful_purchase_key = "last_successful_zettle_purchase"
        self.separator = "__"
        self.preserve_fields = [
            ("gratuityamount", "Float64"),
            ("customamountsale", "bool"),
            ("refundedbypurchaseuuids", "string"),
            ("refundedByPurchaseUUIDs1", "string"),
        ]

        self.context = {
            "destination_schema": destination_schema,
            "destination_table": destination_table,
        }

        # We're removing the WHERE in the DELETE function as if we're playing catchup
        # duplicates could exist in older records. We can do this because we only allow 1
        # concurrent task...
        self.delete_template = """DO $$
BEGIN
   IF EXISTS (
    SELECT FROM pg_tables WHERE schemaname = '{{destination_schema}}'
    AND tablename = '{{destination_table}}') THEN
      DELETE FROM {{ destination_schema }}.{{destination_table}}
        -- WHERE airflow_sync_ds = '{{ ds }}'
        ;
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
        lte = context["data_interval_end"].to_iso8601_string()
        last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)

        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:
            self.ensure_task_comms_table_exists(conn)
            lastPurchaseHash = self.get_last_successful_purchase_id(conn, context)
            extra_context = {
                **context,
                **self.context,
                f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
            }

            self.log.info(
                f"Executing ZettlePurchasesToPostgresOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
            )

            if lastPurchaseHash:
                self.log.info(
                    f"ZettlePurchasesToPostgresOperator Restarting task for this Dagrun from the last successful purchase_id {lastPurchaseHash} after last successful dagrun {last_successful_dagrun_ts}"  # noqa
                )
            else:
                self.log.info(
                    f"ZettlePurchasesToPostgresOperator Starting Task Fresh for this dagrun from {last_successful_dagrun_ts}"  # noqa
                )
                self.log.info("ZettlePurchasesToPostgresOperator Deleting previous Data from this Dagrun")  # noqa
                self.delete_sql = render_template(self.delete_template, context=extra_context)
                self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
                conn.execute(self.delete_sql)

            # Base URL path
            base_url = "https://purchase.izettle.com/purchases/v2"
            # Determine the 'start' parameter based on 'last_successful_dagrun_ts'
            if last_successful_dagrun_ts:
                start_param = last_successful_dagrun_ts
            else:
                three_years_back = datetime.now() - relativedelta(years=2)
                start_param = three_years_back.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            print("start_param", start_param)

            total_docs_processed = 0

            limit = 100
            has_more = True

            while has_more:
                # Dictionary of query parameters
                query_params = {
                    "startDate": start_param,
                    "endDate": lte,
                    "limit": limit,
                }
                if lastPurchaseHash:
                    query_params["lastPurchaseHash"] = lastPurchaseHash

                full_url = f"{base_url}?{urlencode(query_params)}"
                headers = {"Authorization": f"Bearer {token}"}

                print(query_params, limit)
                response = requests.get(full_url, headers=headers)
                if response.status_code == 200:
                    purchases = response.json()
                else:
                    self.log.error("Error Retreiving Zettle transaction: %s", response)
                    raise AirflowException("Error getting Zettle Transactions")

                records = purchases.get("purchases")
                print("TOTAL docs found", len(records))

                normalised_records = self._normalise_records(records)
                print("TOTAL normalized docs found", len(records))
                # print(records)
                total_docs_processed += len(normalised_records)

                # df = DataFrame.from_records(records)
                df = DataFrame(normalised_records)

                print("TOTAL Initial DF docs", df.shape)
                if df.empty:
                    self.log.info("UNEXPECTED EMPTY Balance Transactions to process.")
                    break

                lastPurchaseHash = purchases["lastPurchaseHash"]
                self.log.info(f"Processing ResultSet {total_docs_processed} - {lastPurchaseHash}.")
                df["airflow_sync_ds"] = ds

                if self.discard_fields:
                    # keep this because if we're dropping any problematic fields
                    # from the top level we might want to do this before Flattenning
                    existing_discard_fields = [col for col in self.discard_fields if col in df.columns]
                    df.drop(existing_discard_fields, axis=1, inplace=True)

                df = self.flatten_dataframe_columns_precisely(df)
                df.columns = df.columns.str.lower()
                print("TOTAL flattenned docs found", df.shape)
                df = self.align_to_schema_df(df)
                print("TOTAL Aligned docs found", df.shape)

                df.to_sql(
                    self.destination_table,
                    conn,
                    if_exists="append",
                    schema=self.destination_schema,
                    index=False,
                )
                self.set_last_successful_purchase_id(conn, context, lastPurchaseHash)

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
        self.set_last_successful_dagrun_ts(context, lte)
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

    def set_last_successful_purchase_id(self, conn, context, starting_after):
        return self.set_task_var(conn, context, self.last_successful_purchase_key, starting_after)

    def get_last_successful_purchase_id(self, conn, context):
        return self.get_task_var(conn, context, self.last_successful_purchase_key)

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

    def _normalise_records(self, records):

        normalised_records = []
        for record in records:
            payments = []
            for payment in record.get("payments", []):
                fields = ["uuid", "amount", "type", "gratuityAmount"]
                normalised_payment = {field: payment.get(field) for field in fields}
                payments.append(normalised_payment)
            record["payments"] = payments

            # del record["products"]
            record["id"] = record["purchaseUUID1"]
            normalised_records.append(record)
        return normalised_records

    def align_to_schema_df(self, df):
        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None  # because zettle is rubbish
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        return df

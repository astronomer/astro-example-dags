import re
from urllib.parse import urlencode

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

from plugins.operators.mixins.zettle import ZettleMixin
from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class ZettleFinanceToPostgresOperator(ZettleMixin, FlattenJsonDictMixin, BaseOperator):
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
        super(ZettleFinanceToPostgresOperator, self).__init__(*args, **kwargs)
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
            limit = 1000
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
                self.log.info("Fetching transactions for %s", query_params)
                full_url = f"{base_url}?{urlencode(query_params)}"
                headers = {"Authorization": f"Bearer {token}"}

                response = requests.get(full_url, headers=headers)
                # print(response.json())

                if response.status_code == 200:
                    records = response.json()
                else:
                    self.log.error("Error Retreiving Zettle transaction: %s", response)
                    raise AirflowException("Error getting Zettle Transactions")

                print("TOTAL docs found", len(records))

                total_docs_processed += len(records)
                for record in records:
                    if record["originatingTransactionUuid"] == "772ce5c8-7422-11ee-8257-81c971a9d256":
                        print(
                            "GOT TRANSACTION RECORD FOR 772ce5c8-7422-11ee-8257-81c971a9d256",
                            record,
                        )

                df = DataFrame(records)
                print("TOTAL Initial DF docs", df.shape)

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

                print("TOTAL discarded field docs found", df.shape)
                df = self.flatten_dataframe_columns_precisely(df)
                print("TOTAL flattenned docs found", df.shape)

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
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {self.destination_table}_originatingtransactionuuidtype_idx ON {self.destination_schema}.{self.destination_table} (originatingtransactionuuid, originatortransactiontype, timestamp);"  # noqa
                )

            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)

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

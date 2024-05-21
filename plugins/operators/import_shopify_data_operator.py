import os
import re
import json
import logging
from pprint import pprint  # noqa
from typing import List, Optional
from datetime import timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import DAG, BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin

region_lookup = {"england": "ENG", "wales": "WLS", "scotland": "SCT", "northern ireland": "NIR"}


class ImportShopifyPartnerDataOperator(FlattenJsonDictMixin, BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Schema name
    :type schema: str
    :param destination_schema: Schema name
    :type destination_schema: str
    :param partner_ref: partner reference
    :type partner_ref: str
    """

    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_conn_id",
        schema: str,
        destination_schema: str,
        partner_ref: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising ImportShopifyPartnerDataOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.destination_schema = destination_schema
        self.partner_ref = partner_ref
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"

        self.context = {"schema": schema, "destination_schema": destination_schema, "partner_ref": partner_ref}
        self.get_partner_config_template = f"""
SELECT
    reference,
    partner_platform_api_access_token,
    partner_platform_base_url,
    partner_platform_api_version,
    allowed_region_for_harper
FROM {self.schema}.partner
WHERE partner_platform = 'shopify'
AND reference = '{self.partner_ref}'
"""
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

        self.log.info("Initialised ImportShopifyPartnerDataOperator")

    def execute(self, context):
        try:
            hook = BaseHook.get_hook(self.postgres_conn_id)
            engine = self.get_postgres_sqlalchemy_engine(hook)
            ds = context["ds"]
            run_id = context["run_id"]
            last_successful_dagrun_ts = self.get_last_successful_dagrun_ts(run_id=run_id)
            extra_context = {
                **context,
                **self.context,
                f"{self.last_successful_dagrun_xcom_key}": last_successful_dagrun_ts,
            }

            with engine.connect() as conn:
                transaction = conn.begin()
                try:
                    self._get_partner_config(conn, context)
                    self.fetch_all_orders(limit, since_date)
                    total_docs_process = 0

                    transaction.commit()
                except Exception as e:
                    self.log.error("Error during database operation: %s", e)
                    transaction.rollback()
                    raise AirflowException(f"Database operation failed Rolling Back: {e}")

                context["ti"].xcom_push(key="documents_found", value=total_docs_processed)

            context["ti"].xcom_push(
                key=self.last_successful_dagrun_xcom_key,
                value=context["data_interval_end"].to_iso8601_string(),
            )
            return f"Run SQL for {self.schema}, {self.partner_ref}"
        except Exception as e:
            self.log.error(f"An error occurred: {e}")
            raise AirflowException(e)

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

    def _get_partner_config(self, conn, context):
        self.get_partner_config_sql = render_template(
            self.get_partner_config_template,
            context=context,
            extra_context=self.context,
        )
        self.log.info(f"Executing {self.get_partner_config_sql}")
        partner = conn.execute(self.get_partner_config_sql).fetchone()
        self.api_access_token = partner["partner_platform_api_access_token"]
        self.base_url = partner["partner_platform_base_url"]
        self.api_version = partner["partner_platform_api_version"]
        provinces_json = partner["allowed_region_for_harper"]
        provinces = json.loads(provinces_json)

        self.provinces = []
        for province in provinces:
            # Convert province to lowercase to ensure case-insensitive matching
            province_lower = province.lower()
            if province_lower in region_lookup:
                self.provinces.append(region_lookup[province_lower])
            else:
                raise AirflowException(f"Error: Please add '{province}' to region_lookup.")

    def check_province_code(self, order):
        shipping_address = order.get("shipping_address")
        if shipping_address:
            province_code = shipping_address.get("province_code")
            if province_code in self.provinces:
                return True
        return False

    def fetch_and_process_data(self, **kwargs):
        try:
            all_orders = []
            for store, creds in STORE_CREDENTIALS.items():
                store_name = creds["store_name"]
                access_token = creds["access_token"]
                orders = fetch_all_orders(store_name, access_token, since_date=since_date, until_date=until_date)
                filtered_orders = [order for order in orders if self.check_province_code(order)]
                all_orders.extend(filtered_orders)

            if all_orders:
                fields = list(all_orders[0].keys())
                logging.info("Fields: %s", fields)
            else:
                logging.info("No orders found.")

            all_orders_df = pd.DataFrame(all_orders)

            df = self.flatten_dataframe_columns_precisely(all_orders_df)
            print("TOTAL flattened docs found", df.shape)

            df.columns = df.columns.str.lower()

            hook = BaseHook.get_hook(self.postgres_conn_id)
            engine = self.get_postgres_sqlalchemy_engine(hook)
            with engine.connect() as conn:
                df.to_sql(
                    self.destination_table,
                    conn,
                    if_exists="append",
                    schema=self.destination_schema,
                    index=False,
                )
        except Exception as e:
            logging.error("An error occurred: %s", str(e))
            raise

    def fetch_all_orders(self, limit=250, since_date=None, until_date=None):
        url = f"{self.base_url}/admin/api/2024-04/orders.json?limit={limit}"
        if since_date:
            url += f"&created_at_min={since_date}"
        if until_date:
            url += f"&created_at_max={until_date}"

        headers = {"X-Shopify-Access-Token": self.api_access_token}
        all_orders = []

        while url:
            self.log.info("Fetching orders from URL: %s", url)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                orders = data.get("orders", [])
                filtered_orders = [order for order in orders if check_province_code(order)]
                all_orders.extend(filtered_orders)
                url = get_next_page_url(response)
            else:
                error_message = response.json()
                self.log.error("Error fetching orders: %s", error_message)
                raise Exception(f"Error {response}")

        return all_orders

    def get_next_page_url(response):
        link_header = response.headers.get("Link")
        if link_header:
            links = link_header.split(",")
            for link in links:
                if 'rel="next"' in link:
                    next_url = link.split(";")[0].strip("<>")
                    return next_url
        return None

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

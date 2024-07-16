import re
import json
from pprint import pprint  # noqa

# from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin

region_lookup = {"england": "ENG", "wales": "WLS", "scotland": "SCT", "northern ireland": "NIR"}

required_columns = [
    "partner__name",
    "id",
    "admin_graphql_api_id",
    "app_id",
    "cancel_reason",
    "cancelled_at",
    "cart_token",
    "checkout_id",
    "checkout_token",
    "closed_at",
    "confirmation_number",
    "confirmed",
    "contact_email",
    "created_at",
    "currency",
    "current_subtotal_price",
    "current_subtotal_price_set__shop_money__amount",
    "current_subtotal_price_set__shop_money__currency_code",
    "current_subtotal_price_set__presentment_money__amount",
    "current_subtotal_price_set__presentment_money__currency_code",
    "current_total_additional_fees_set",
    "current_total_discounts",
    "current_total_discounts_set__shop_money__amount",
    "current_total_discounts_set__shop_money__currency_code",
    "current_total_discounts_set__presentment_money__amount",
    "current_total_discounts_set__presentment_money__currency_code",
    "current_total_duties_set",
    "current_total_price",
    "current_total_price_set__shop_money__amount",
    "current_total_price_set__shop_money__currency_code",
    "current_total_price_set__presentment_money__amount",
    "current_total_price_set__presentment_money__currency_code",
    "current_total_tax",
    "current_total_tax_set__shop_money__amount",
    "current_total_tax_set__shop_money__currency_code",
    "current_total_tax_set__presentment_money__amount",
    "current_total_tax_set__presentment_money__currency_code",
    "discount_codes",
    "email",
    "estimated_taxes",
    "financial_status",
    "fulfillment_status",
    "landing_site",
    "landing_site_ref",
    "name",
    "note",
    "note_attributes",
    "number",
    "order_number",
    "order_status_url",
    "original_total_additional_fees_set",
    "original_total_duties_set",
    "payment_gateway_names",
    "po_number",
    "presentment_currency",
    "processed_at",
    "reference",
    "referring_site",
    "source_identifier",
    "source_name",
    "source_url",
    "subtotal_price",
    "subtotal_price_set__shop_money__amount",
    "subtotal_price_set__shop_money__currency_code",
    "subtotal_price_set__presentment_money__amount",
    "subtotal_price_set__presentment_money__currency_code",
    "tags",
    "tax_exempt",
    "tax_lines",
    "taxes_included",
    "test",
    "token",
    "total_discounts",
    "total_discounts_set__shop_money__amount",
    "total_discounts_set__shop_money__currency_code",
    "total_discounts_set__presentment_money__amount",
    "total_discounts_set__presentment_money__currency_code",
    "total_line_items_price",
    "total_line_items_price_set__shop_money__amount",
    "total_line_items_price_set__shop_money__currency_code",
    "total_line_items_price_set__presentment_money__amount",
    "total_line_items_price_set__presentment_money__currency_code",
    "total_outstanding",
    "total_price",
    "total_price_set__shop_money__amount",
    "total_price_set__shop_money__currency_code",
    "total_price_set__presentment_money__amount",
    "total_price_set__presentment_money__currency_code",
    "total_shipping_price_set__shop_money__amount",
    "total_shipping_price_set__shop_money__currency_code",
    "total_shipping_price_set__presentment_money__amount",
    "total_shipping_price_set__presentment_money__currency_code",
    "total_tax",
    "total_tax_set__shop_money__amount",
    "total_tax_set__shop_money__currency_code",
    "total_tax_set__presentment_money__amount",
    "total_tax_set__presentment_money__currency_code",
    "updated_at",
    "user_id",
    "customer__id",
    "customer__email",
    "customer__created_at",
    "customer__updated_at",
    "customer__state",
    "customer__note",
    "customer__verified_email",
    "customer__tags",
    "customer__currency",
    "discount_applications",
    "fulfillments",
    "line_items",
    "payment_terms",
    "refunds",
    "shipping_address__city",
    "shipping_address__province",
    "shipping_address__country",
    "shipping_address__company",
    "shipping_address__country_code",
    "shipping_address__province_code",
    # "shipping_lines",
    "airflow_sync_ds",
    # "customer__email_marketing_consent",
    # "company",
    "partner__reference",
    "order_id",
    # "customer__first_name",
    # "customer__last_name",
]

# Add new required columns for items ordered, fulfilled, returned, and value returned
required_columns.extend(
    [
        # "order_name",
        "items_ordered",
        # "items_fulfilled",
        "items_returned",
        "value_returned",
        # "currency"
    ]
)


class ImportShopifyPartnerDataOperator(FlattenJsonDictMixin, BaseOperator):
    """
    :param postgres_conn_id: postgres connection id
    :type postgres_conn_id: str
    :param schema: Schema name
    :type schema: str
    :param destination_schema: Schema name
    :type destination_schema: str
    :param destination_table: Table name
    :type destination_table: st
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
        destination_table: str,
        partner_ref: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.log.info("Initialising ImportShopifyPartnerDataOperator")
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.partner_ref = partner_ref
        self.separator = "__"
        self.last_successful_dagrun_xcom_key = "last_successful_dagrun_ts"
        self.discard_fields = []
        self.preserve_fields = [
            ("customer__email_marketing_consent", "bool"),
            ("company", "string"),
        ]

        self.context = {
            "schema": schema,
            "destination_schema": destination_schema,
            "destination_table": destination_table,
            "partner_ref": partner_ref,
        }
        self.get_partner_config_template = f"""
SELECT
    reference,
    name,
    partner_platform_api_access_token,
    partner_platform_base_url,
    partner_platform_api_version,
    allowed_region_for_harper,
    partner_shopify_app_type,
    partner_platform_api_key,
    partner_platform_api_secret

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

        self.delete_sql = render_template(self.delete_template, context=extra_context)

        self.log.info(
            f"Executing ImportShopifyPartnerDataOperator since last successful dagrun {last_successful_dagrun_ts}"  # noqa
        )

        engine = self.get_postgres_sqlalchemy_engine(hook)
        with engine.connect() as conn:
            self._get_partner_config(conn, context)
            self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
            conn.execute(self.delete_sql)

            lte = context["data_interval_end"].to_iso8601_string()
            total_docs_processed = 0
            limit = 25
            # max_pages = 100  # Safety limit to prevent infinite loops

            # Base URL path
            headers = {}
            if self.shopify_app_type == "private":
                base_url = f"https://{self.api_key}:{self.api_secret}@{self.base_url}/admin/api/2024-04/orders.json"
            else:
                base_url = f"https://{self.base_url}/admin/api/2024-04/orders.json"
                headers = {"X-Shopify-Access-Token": self.api_access_token}

            # Determine the 'start' parameter based on 'last_successful_dagrun_ts'
            start_param = last_successful_dagrun_ts if last_successful_dagrun_ts else "2016-08-01T00:00:00.000Z"

            # Dictionary of query parameters
            query_params = {
                "created_at_min": start_param,
                "created_at_max": lte,
                "limit": limit,
            }
            self.log.info("Fetching transactions for %s", query_params)

            url = f"{base_url}?{urlencode(query_params)}"
            page_count = 0  # Counter for the number of pages processed
            while url:
                self.log.info("Fetching orders from URL: %s", url)
                try:
                    response = requests.get(url, headers=headers)
                    if response.status_code == 401:
                        self.log.error("Authentication failed for URL: %s", url)
                        break  # Break the while loop to skip to the next iteration of the outer loop
                    if response.status_code != 200:
                        error_message = response.json()
                        self.log.error("Error fetching orders: %s", error_message)
                        raise AirflowException(f"Error {error_message} {response}")
                except requests.exceptions.RequestException as e:
                    self.log.error("Request failed: %s", e)
                    break  # Break the while loop to skip to the next iteration of the outer loop

                url = self._get_next_page_url(response)
                data = response.json()
                orders = data.get("orders", [])
                records = [order for order in orders if self._check_province_code(order)]
                page_count += 1

                self.log.info("Filter total Batch docs found: %d", len(records))
                total_docs_processed += len(records)
                self.log.info("Total pages processed: %d", page_count)

                df = DataFrame(records)
                self.log.info("TOTAL Initial DF docs: %d", df.shape[0])

                if not df.empty:
                    df.insert(0, "partner__name", self.partner_name)  # partner name as the first column
                    self.log.info(f"Processing ResultSet {total_docs_processed} from batch.")
                    df["airflow_sync_ds"] = ds

                    if self.discard_fields:
                        existing_discard_fields = [col for col in self.discard_fields if col in df.columns]
                        if existing_discard_fields:
                            df.drop(existing_discard_fields, axis=1, inplace=True)

                    self.log.info("TOTAL discarded field docs found: %d", df.shape[0])
                    df = self.flatten_dataframe_columns_precisely(df)
                    self.log.info("TOTAL flattened docs found: %d", df.shape[0])
                    print("TOTAL flattenned docs found", df.shape)

                    # Transform fields ending in '_at' to date strings
                    for col in df.columns:
                        if col.endswith("_at"):
                            df[col] = pd.to_datetime(df[col], errors="coerce").apply(
                                lambda x: x.isoformat() if pd.notnull(x) else None
                            )
                    # Create a list to hold new records
                    new_records = []
                    # Break out total item quantity, refunds
                    for order in records:
                        order_node = order
                        items_ordered = sum(item["quantity"] for item in order_node["line_items"])
                        """items_fulfilled = sum(
                            fulfillment["quantity"]
                            for fulfillment in order_node["fulfillments"]
                            for item in fulfillment["line_items"]
                        )"""
                        items_returned = sum(
                            refund_item["quantity"]
                            for refund in order_node["refunds"]
                            for refund_item in refund["refund_line_items"]
                        )
                        value_returned = sum(
                            float(refund_item["subtotal"]["amount"])
                            for refund in order_node["refunds"]
                            for refund_item in refund["refund_line_items"]
                        )

                        self.log.info(f"Order {order_node['name']}:")
                        self.log.info(f"  Items ordered: {items_ordered}")
                        # self.log.info(f"  Items fulfilled: {items_fulfilled}")
                        self.log.info(f"  Items returned: {items_returned}")
                        self.log.info(
                            f"  Value returned: {value_returned:.2f}\n"
                            f"{order_node['total_price_set']['shop_money']['currency_code']}"
                        )

                        # Add the fields to the list of new records
                        new_records.append(
                            {
                                "items_ordered": items_ordered,
                                # "items_fulfilled": items_fulfilled,
                                "items_returned": items_returned,
                                "value_returned": value_returned,
                                # 'currency': order_node['total_price_set']['shop_money']['currency_code']
                            }
                        )

                    # After the loop, create a DataFrame from the new records
                    new_df = pd.DataFrame(new_records)

                    # Concatenate with the existing DataFrame
                    df = pd.concat([df, new_df], ignore_index=True)

                    df.columns = df.columns.str.lower()
                    df = self.align_to_schema_df(df)
                    print("TOTAL Aligned docs found", df.shape)

                    # Print the columns present in the DataFrame
                    # self.log.info("DataFrame columns before filtering: %s", df.columns.tolist())

                    for column in required_columns:
                        if column not in df.columns:
                            df[column] = ""

                    # Filter the DataFrame to only include the required columns
                    df = df[required_columns]

                    try:
                        df.to_sql(
                            self.destination_table,
                            conn,
                            if_exists="append",
                            schema=self.destination_schema,
                            index=False,
                        )
                    except Exception as e:
                        self.log.error(f"Failed to write DataFrame to SQL: {e}")
                        raise
                else:
                    self.log.info("All Records Filtered to zero in this batch.")

            # if page_count >= max_pages:
            # self.log.warning(f"Reached the maximum number of pages
            # ({max_pages}). There might be more data to process.")

            # Check how many Docs total
            if total_docs_processed > 0:
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {self.destination_table}_idx ON {self.destination_schema}.{self.destination_table} (id);"  # noqa
                )

            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)

        context["ti"].xcom_push(
            key=self.last_successful_dagrun_xcom_key,
            value=context["data_interval_end"].to_iso8601_string(),
        )
        self.log.info("Shopify Data Written to Datalake successfully.")

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
        partner_row = conn.execute(self.get_partner_config_sql).fetchone()
        if partner_row:
            self.api_access_token = partner_row["partner_platform_api_access_token"]
            self.base_url = partner_row["partner_platform_base_url"]
            self.api_version = partner_row["partner_platform_api_version"]
            self.shopify_app_type = partner_row["partner_shopify_app_type"]
            self.api_key = partner_row["partner_platform_api_key"]
            self.api_secret = partner_row["partner_platform_api_secret"]
            self.partner_reference = partner_row["reference"]
            self.partner_name = partner_row["name"]
            provinces_json = partner_row["allowed_region_for_harper"]
            provinces = json.loads(provinces_json)
        else:
            self.log.error("No partner details found.")
            raise AirflowException("No partner details found.")

        self.provinces = []
        for province in provinces:
            # Convert province to lowercase to ensure case-insensitive matching
            province_lower = province.lower()
            if province_lower in region_lookup:
                self.provinces.append(region_lookup[province_lower])
            else:
                raise AirflowException(f"Error: Please add '{province}' to region_lookup.")
        print("regions: ", provinces)

    def _check_province_code(self, order):
        shipping_address = order.get("shipping_address", None)
        if shipping_address:
            province_code = shipping_address.get("province_code")
            if province_code in self.provinces:
                return True
        return False

    def _get_next_page_url(self, response):
        link_header = response.headers.get("Link")
        print("_get_next_page_url", link_header)  # Debug print
        if link_header:
            links = link_header.split(",")
            print("_get_next_page_url links", links)  # Debug print
            for link in links:
                if 'rel="next"' in link:
                    next_url = link.split(";")[0].strip()
                    print("_get_next_page_url next_url", next_url)  # Debug print
                    return next_url.strip("<> ")
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

    def align_to_schema_df(self, df):
        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        return df

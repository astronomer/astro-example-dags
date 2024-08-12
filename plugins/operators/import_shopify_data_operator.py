import re
import json
import time
import random

import pandas as pd
import shopify
from pandas import DataFrame
from tenacity import wait_exponential
from sqlalchemy import create_engine
from airflow.models import XCom, BaseOperator
from sqlalchemy.exc import OperationalError
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

# from requests.exceptions import HTTPError
from airflow.utils.session import provide_session

from plugins.utils.render_template import render_template

from plugins.operators.mixins.flatten_json import FlattenJsonDictMixin

required_columns = [
    "partner__name",
    "id",
    "admin_graphql_api_id",
    "app_id",
    "cancel_reason",
    "cancelled_at",
    "closed_at",
    "confirmation_number",
    "confirmed",
    "contact_email",
    "created_at",
    "currency",
    "current_subtotal_price",
    "current_subtotal_price_set__shop_money__currency_code",
    "current_subtotal_price_set__presentment_money__amount",
    "current_subtotal_price_set__presentment_money__currency_code",
    "current_total_discounts",
    "current_total_discounts_set__presentment_money__amount",
    "current_total_discounts_set__presentment_money__currency_code",
    "current_total_duties_set",
    "current_total_price",
    "current_total_price_set__presentment_money__amount",
    "current_total_price_set__presentment_money__currency_code",
    "current_total_tax",
    "current_total_tax_set__presentment_money__amount",
    "current_total_tax_set__presentment_money__currency_code",
    "discount_codes",
    "financial_status",
    "fulfillment_status",
    "harper_product",
    "name",
    "order_number",
    "order_status_url",
    "payment_gateway_names",
    "processed_at",
    "reference",
    "referring_site",
    "source_name",
    # "source_url",
    "subtotal_price",
    "subtotal_price_set__presentment_money__amount",
    "subtotal_price_set__presentment_money__currency_code",
    "tags",
    "taxes_included",
    "test",
    "total_discounts",
    "total_discounts_set__presentment_money__amount",
    "total_discounts_set__presentment_money__currency_code",
    "total_line_items_price",
    "total_line_items_price_set__presentment_money__amount",
    "total_line_items_price_set__presentment_money__currency_code",
    "total_outstanding",
    "total_price",
    "total_price_set__presentment_money__amount",
    "total_price_set__presentment_money__currency_code",
    "total_shipping_price_set__presentment_money__amount",
    "total_shipping_price_set__presentment_money__currency_code",
    "total_tax",
    "total_tax_set__presentment_money__amount",
    "total_tax_set__presentment_money__currency_code",
    "updated_at",
    "user_id",
    "customer__id",
    "customer__created_at",
    "customer__updated_at",
    # "customer__state",
    "customer__tags",
    "customer__currency",
    # "discount_applications",
    "line_items",
    # "payment_terms",
    "refunds",
    "shipping_address__city",
    "shipping_address__province",
    "shipping_address__country",
    "shipping_address__company",
    "shipping_address__country_code",
    "shipping_address__province_code",
    "source_name",
    "airflow_sync_ds",
    "partner__reference",
    "order_id",
    "order_name",
    "items_ordered",
    "items_returned",
    "value_ordered",
    "value_returned",
    "fulfilled_at",
    # "year_month",
]


class ImportShopifyPartnerDataOperator(FlattenJsonDictMixin, BaseOperator):

    region_lookup = {"england": "ENG", "wales": "WLS", "scotland": "SCT", "northern ireland": "NIR"}

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
            ("company", "string"),
            ("user_id", "Int64"),
            ("taxes_included", "bool"),
            ("confirmed", "bool"),
            ("test", "bool"),
            ("order_number", "Int64"),
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

    def custom_wait(self, retry_state):
        exp_wait = wait_exponential(multiplier=1, min=4, max=60).sleep(retry_state)
        rand_wait = random.uniform(0, 5)  # Add up to 5 seconds of random wait
        return exp_wait + rand_wait

    """ @retry(
        stop=stop_after_attempt(10),
        wait=custom_wait,
        retry=retry_if_exception_type(OperationalError),
        reraise=True,
    )"""

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
            try:
                partner_config = self._get_partner_config(conn, context)
                self._setup_shopify_session(partner_config)
                self.log.info(f"Ensuring Transient Data is clean - {self.delete_sql}")
                conn.execute(self.delete_sql)

            except OperationalError as e:
                if "LockNotAvailable" in str(e):
                    self.log.warning("Lock not available. Retrying...")
                    # Add a small random delay before retrying to reduce contention
                    time.sleep(random.uniform(0.1, 0.5))
                    raise  # Re-raise the exception to trigger the retry
                else:
                    raise  # If it's a different OperationalError, re-raise without retry

            lte = context["data_interval_end"].to_iso8601_string()
            total_docs_processed = 0

            # Determine the 'start' parameter based on 'last_successful_dagrun_ts'
            start_param = last_successful_dagrun_ts if last_successful_dagrun_ts else "2024-01-01T00:00:00.000Z"

            self.log.info(f"Fetching orders from {start_param} to {lte}")

            # Fetch orders
            orders = shopify.Order.find(
                created_at_min=start_param, created_at_max=lte, limit=250, status="any"  # Shopify's max limit per page
            )

            page_count = 0
            allowed_regions = partner_config["allowed_region_for_harper"]
            self.log.info(f"Allowed regions for Harper: {allowed_regions}")

            while orders:
                page_count += 1
                self.log.info(f"Processing page {page_count} with {len(orders)} orders")

                records = [
                    order.to_dict() for order in orders if self._check_province_code(order.to_dict(), allowed_regions)
                ]

                self.log.info(f"Filtered orders in this batch: {len(records)}")
                total_docs_processed += len(records)

                if records:
                    # self.process records
                    df = DataFrame(records)
                    df = self._preprocess_dataframe(df, ds)
                    df = self._process_additional_fields(df)
                    self.log.debug("Type of DataFrame: %s", type(df).__name__)
                    df = self.align_to_schema(df)

                    """# Handle NaT values if necessary (e.g., fill with a default date)
                    df["created_at"].fillna(pd.Timestamp("0000-01-01"), inplace=True)

                    # Log the datatype of the 'created_at' column
                    self.log.info(f"created_at datatype = {df['created_at'].dtype}")

                    # Add the new year_month field
                    df["year_month"] = df["created_at"].dt.strftime("%Y-%m")"""

                    try:
                        df.to_sql(
                            self.destination_table,
                            conn,
                            if_exists="append",
                            schema=self.destination_schema,
                            index=False,
                            chunksize=1000,
                        )
                    except Exception as e:
                        self.log.error(f"Failed to write DataFrame to SQL: {e}")
                        raise

                # Get the next page of orders
                if orders.has_next_page():
                    orders = orders.next_page()
                else:
                    break

            self.log.info(f"Total orders processed: {total_docs_processed}")

            if total_docs_processed > 0:
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {self.destination_table}_idx "
                    f"ON {self.destination_schema}.{self.destination_table} (id);"
                )

            context["ti"].xcom_push(key="documents_found", value=total_docs_processed)

        context["ti"].xcom_push(
            key=self.last_successful_dagrun_xcom_key,
            value=context["data_interval_end"].to_iso8601_string(),
        )
        self.log.info("Shopify Data Written to transient table successfully.")

    def parse_json_field(self, field):
        """Parse JSON-like string fields into lists of dictionaries."""
        if isinstance(field, str):
            try:
                field = json.loads(field)
            except (ValueError, TypeError):
                self.log.error("Failed to parse JSON")
                field = []
        return field if isinstance(field, list) else []

    # Assuming df is your DataFrame and it has columns with dict types
    def convert_dict_columns_to_strings(self, df):
        """
        Converts dictionary and list columns in the DataFrame to JSON strings.

        Parameters:
            df (pd.DataFrame): The DataFrame with potential dictionary or list columns.

        Returns:
            pd.DataFrame: The DataFrame with dictionary and list columns converted to JSON strings.
        """
        # Identify columns with object data types that might contain dictionaries or lists
        object_columns = df.select_dtypes(include=["object"]).columns

        for col in object_columns:
            # Try to convert the column entries to JSON strings
            try:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            except Exception as e:
                print(f"Error converting column {col}: {e}")

        return df

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

            return {
                "partner_platform_api_access_token": self.api_access_token,
                "partner_platform_base_url": self.base_url,
                "partner_platform_api_version": self.api_version,
                "partner_shopify_app_type": self.shopify_app_type,
                "partner_platform_api_key": self.api_key,
                "partner_platform_api_secret": self.api_secret,
                "reference": self.partner_reference,
                "name": self.partner_name,
                "allowed_region_for_harper": provinces,
            }
        else:
            self.log.error("No partner details found.")
            raise AirflowException("No partner details found.")

    def _setup_shopify_session(self, partner_config):
        if partner_config is None:
            raise AirflowException("Partner configuration is missing or invalid")

        if partner_config["partner_shopify_app_type"] == "private":
            site_url = (
                f"https://{partner_config['partner_platform_api_key']}:"
                f"{partner_config['partner_platform_api_secret']}@"
                f"{partner_config['partner_platform_base_url']}/admin/api/"
                f"{partner_config['partner_platform_api_version']}"
            )
            shopify.ShopifyResource.set_site(site_url)
        else:
            site_url = (
                f"https://{partner_config['partner_platform_base_url']}/"
                f"admin/api/{partner_config['partner_platform_api_version']}"
            )
            shopify.ShopifyResource.set_site(site_url)
            shopify.ShopifyResource.set_headers(
                {"X-Shopify-Access-Token": partner_config["partner_platform_api_access_token"]}
            )

    def _check_province_code(self, order, allowed_regions):
        shipping_address = order.get("shipping_address", None)
        if shipping_address:
            province_code = shipping_address.get("province_code")
            allowed_province_codes = [
                self.region_lookup[region.lower()]
                for region in allowed_regions
                if region.lower() in self.region_lookup
            ]
            if province_code in allowed_province_codes:
                return True
        return False

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

    def _preprocess_dataframe(self, df: pd.DataFrame, ds: str) -> pd.DataFrame:
        df.insert(0, "partner__name", self.partner_name)
        df["airflow_sync_ds"] = ds
        if self.discard_fields:
            df = df.drop(columns=[col for col in self.discard_fields if col in df.columns])
        return self.flatten_dataframe_columns_precisely(df)

    def _process_additional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure df is not None and is a DataFrame

        df["order_name"] = df["name"]
        for field in ["line_items", "fulfillments", "refunds"]:
            df[field] = df[field].apply(self.parse_json_field)

        df["items_ordered"] = df["line_items"].apply(lambda x: sum(item["quantity"] for item in x))
        df["items_fulfilled"] = df["fulfillments"].apply(
            lambda x: sum(
                sum(item.get("quantity", 0) for item in fulfillment.get("line_items", [])) for fulfillment in x
            )
        )
        df["items_returned"] = df["refunds"].apply(
            lambda x: sum(sum(item["quantity"] for item in refund["refund_line_items"]) for refund in x)
        )
        # Value ordered including discount and tax but not shipping
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
        df["total_shipping_price_set__presentment_money__amount"] = pd.to_numeric(
            df["total_shipping_price_set__presentment_money__amount"], errors="coerce"
        )

        df["value_ordered"] = (
            df["total_price"] - df["total_shipping_price_set__presentment_money__amount"]
        )  # includes discount
        df["value_returned"] = df["refunds"].apply(
            lambda x: sum(sum(float(item["subtotal"]) for item in refund["refund_line_items"]) for refund in x)
        )
        df["fulfilled_at"] = df["fulfillments"].apply(lambda x: x[-1]["created_at"] if x else None)

        # print(df.head)

        # Add the new harper_product field
        df["harper_product"] = df["tags"].apply(
            lambda tags: (
                "harper_try" if "harper_try" in tags else ("harper_concierge" if "harper_concierge" in tags else None)
            )
        )
        return df

    def align_to_schema(self, df):
        # Ensure date fields are stored as datetime
        for col in df.columns:
            if col.endswith("_at"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
                # Convert dict columns to JSON strings

        df = self.convert_dict_columns_to_strings(df)

        # Clean and align columns
        df.columns = df.columns.str.lower()
        # print(df.head)

        for field, dtype in self.preserve_fields:
            if field not in df.columns:
                df[field] = None
            print(f"aligning column {field} as type {dtype}")
            df[field] = df[field].astype(dtype)

        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        df = df[required_columns]
        return df

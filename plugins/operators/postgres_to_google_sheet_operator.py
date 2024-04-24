import re
import datetime

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from googleapiclient.errors import HttpError
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

# YOU MUST CREATE THE DESTINATION SPREADSHEET IN ADVANCE MANUALLY IN ORDER FOR THIS TO WORK


class PostgresToGoogleSheetOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        spreadsheet_id,
        worksheet,
        google_conn_id,
        postgres_conn_id,
        *args,
        **kwargs,
    ):
        super(PostgresToGoogleSheetOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.google_conn_id = google_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        # Construct SQL query
        sql_query = f"SELECT * FROM {self.schema}.{self.table} LIMIT 1000"

        # Fetch data from the database
        hook = BaseHook.get_hook(self.postgres_conn_id)
        engine = self.get_postgres_sqlalchemy_engine(hook)

        with engine.connect() as conn:

            result = conn.execute(sql_query)
            records = result.fetchall()
            df = DataFrame(records)

            if df.empty:
                self.log.info("No data to write to Google Sheet.")
                return

            self.log.info(f"Number of rows in DataFrame Before processing: {len(df)}")

            # Ensure datetime columns are converted to string in ISO format
            for col, dtype in df.dtypes.items():
                if dtype.kind in ("M", "m"):  # 'M' for datetime-like, 'm' for timedelta
                    df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)
                elif isinstance(df[col].iloc[0], list):  # Handle list data
                    df[col] = df[col].apply(lambda x: ", ".join(map(str, x)) if x else None)
                elif dtype.kind == "O":  # Check for 'object' dtype which might include dates
                    try:
                        # Attempt to convert any standard date or datetime objects to string
                        if isinstance(df[col].iloc[0], (datetime.date, datetime.datetime)):
                            df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)
                    except (TypeError, AttributeError):
                        # If conversion fails, it's not a date/datetime, ignore or log if needed
                        self.log.info(f"Column {col} contains non-datetime data that was not converted.")

            self.log.info(f"Number of rows in DataFrame after processing: {len(df)}")

            # Convert DataFrame to a list of lists (Google Sheets format)
            data = [df.columns.tolist()] + df.values.tolist()

            # Initialize Google Sheets hook
            sheets_hook = GSheetsHook(gcp_conn_id=self.google_conn_id)

            try:
                # Try to clear the sheet before writing new data to ensure overwrite
                sheets_hook.clear(spreadsheet_id=self.spreadsheet_id, range_=f"{self.worksheet}")
            except HttpError as e:
                if e.resp.status == 404:
                    # If the sheet does not exist, create it
                    self.log.info(f"Sheet '{self.worksheet}' does not exist. Creating it.")
                    request_body = {
                        "requests": [
                            {
                                "addSheet": {
                                    "properties": {
                                        "title": self.worksheet,
                                    }
                                }
                            }
                        ]
                    }
                    sheets_hook.batch_update(spreadsheet_id=self.spreadsheet_id, body=request_body)
                else:
                    raise

            # Write data to the sheet
            sheets_hook.update_values(
                spreadsheet_id=self.spreadsheet_id,
                range_=f"{self.worksheet}!A1",
                values=data,
            )

        self.log.info("Data written to Google Sheet successfully.")

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

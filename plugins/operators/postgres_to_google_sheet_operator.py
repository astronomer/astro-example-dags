from airflow.models import BaseOperator
from googleapiclient.errors import HttpError
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.sheets.hooks.sheets import GoogleSheetsHook

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
        db_conn_id,
        *args,
        **kwargs,
    ):
        super(PostgresToGoogleSheetOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.spreadsheet_id = spreadsheet_id
        self.worksheet = worksheet
        self.google_conn_id = google_conn_id
        self.db_conn_id = db_conn_id

    def execute(self, context):
        # Construct SQL query
        sql_query = f"SELECT * FROM {self.schema}.{self.table_name} LIMIT 1000"

        # Fetch data from the database
        pg_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        records = pg_hook.get_records(sql_query)
        if not records:
            self.log.info("No data to write to Google Sheet.")
            return

        # Extract column headers
        column_headers = list(records[0]._fields)

        # Prepare data (including headers)
        data = [column_headers] + [list(record) for record in records]

        # Initialize Google Sheets hook
        sheets_hook = GoogleSheetsHook(gcp_conn_id=self.google_conn_id)

        try:
            # Try to clear the sheet before writing new data to ensure overwrite
            sheets_hook.clear(spreadsheet_id=self.spreadsheet_id, range=f"{self.worksheet}!A:Z")
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
            range=f"{self.worksheet}!A1",
            values=data,
        )

        self.log.info("Data written to Google Sheet successfully.")

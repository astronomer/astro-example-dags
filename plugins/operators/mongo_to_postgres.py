import pandas as pd
from pandas import DataFrame, json_normalize
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class MongoToPostgresOperator(BaseOperator):
    def __init__(
        self,
        mongo_conn_id: str,
        postgres_conn_id: str,
        mongo_collection: str,
        mongo_database: str,
        mongo_query: list,
        postgres_table: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.mongo_collection = mongo_collection
        self.mongo_database = mongo_database
        self.mongo_query = mongo_query
        self.postgres_table = postgres_table

    def flatten_nested_columns(self, df, column_prefix=""):
        for column in df.columns:
            if isinstance(df[column].iloc[0], dict):
                flattened = json_normalize(df[column])
                flattened.columns = [f"{column_prefix}{column}_{subcol}" for subcol in flattened.columns]
                df = df.drop(column, axis=1).join(flattened)
            elif isinstance(df[column].iloc[0], list):
                # Convert lists to JSON strings to ensure compatibility with SQL databases
                df[column] = df[column].apply(lambda x: pd.Series(x).to_json())
        return df

    def execute(self, context):
        mongo_hook = MongoHook(self.mongo_conn_id)
        postgres_hook = PostgresHook(self.postgres_conn_id)

        # Connecting to MongoDB and running the aggregation query
        collection = mongo_hook.get_collection(self.mongo_collection, self.mongo_database)
        results = collection.aggregate(self.mongo_query)

        # Load the results into a DataFrame
        df = DataFrame(list(results))

        if not df.empty:
            # Flatten nested objects
            df = self.flatten_nested_columns(df)

            # Use the Postgres Hook to insert the data
            df.to_sql(
                self.postgres_table,
                postgres_hook.get_sqlalchemy_engine(),
                if_exists="append",
                index=False,
            )
        else:
            self.log.info("No data returned from MongoDB query.")

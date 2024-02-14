from pprint import pprint

import pandas as pd

from plugins.utils.json_schema_to_dataframe import json_schema_to_dataframe
from dags.data_migrations.aggregation_loader import load_aggregation_configs

pd.set_option("display.max_rows", None)  # or a large number instead of None
pd.set_option("display.max_columns", None)  # Display any number of columns
pd.set_option("display.max_colwidth", None)  # Display full width of each column
pd.set_option("display.width", None)  # Use maximum width available


# Now load the migrations
migrations = load_aggregation_configs("aggregations")

migration_tasks = []
for config in migrations:
    print(f"Schema for ${config['destination_table']}")
    schema_path = f"./include/generatedSchemas/{config['jsonschema']}"
    flattened_schema = json_schema_to_dataframe(
        schema_path,
        discard_fields=config.get("discard_fields", []),
        start_key=config.get("unwind"),
        unwind_key=config.get("unwind_key"),
    )
    pprint(flattened_schema)

import json
import os

from airflow.exceptions import AirflowException


# Function to load and validate configuration for each migration
def load_migration_configs(migrations_dir):
    migrations = []
    for dir_name in os.listdir(migrations_dir):
        migration_dir = os.path.join(migrations_dir, dir_name)
        if os.path.isdir(migration_dir):
            config_path = os.path.join(migration_dir, "config.json")
            select_sql_path = os.path.join(migration_dir, "select.sql")
            preop_sql_path = os.path.join(migration_dir, "preoperation.sql")

            if os.path.exists(config_path):
                with open(config_path, "r") as file:
                    config = json.load(file)

                # Assign the file paths of SQL files
                config["sql"] = select_sql_path
                config["preoperation"] = preop_sql_path

                # Validate that the 'table' value matches the subdirectory name
                if config.get("table") != dir_name:
                    raise AirflowException(
                        f"Table name in config does not match the subdirectory name for {dir_name}"
                    )

                # Dynamically generate the task ID
                config["task_id"] = f"migrate_{dir_name}_to_postgres"

                migrations.append(config)
    return migrations

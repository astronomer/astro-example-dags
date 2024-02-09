import os
import json

from airflow.exceptions import AirflowException


def load_file(file_path):
    """
    Load json content from a file.
    """
    with open(file_path, "r") as file:
        return file.read()


def load_aggregation_configs(relative_path):
    """
    Load and validate configuration for each migration.

    :param relative_path: The relative path to the migrations directory.
    :return: A list of configurations for each migration.
    """
    base_dir = os.path.dirname(os.path.realpath(__file__))
    migrations_dir = os.path.join(base_dir, relative_path)

    migrations = []
    for dir_name in os.listdir(migrations_dir):
        migration_dir = os.path.join(migrations_dir, dir_name)
        if os.path.isdir(migration_dir):
            config_path = os.path.join(migration_dir, "config.json")
            if os.path.exists(config_path):
                with open(config_path, "r") as file:
                    config = json.load(file)

                select_aggregation_path = os.path.join(migration_dir, "select.json")
                preop_sql_path = os.path.join(migration_dir, "preoperation.sql")
                if os.path.exists(select_aggregation_path):
                    config["aggregation_query"] = load_file(select_aggregation_path)
                else:
                    raise AirflowException(f"'select.json' doesn't exist for migration {dir_name} in {migration_dir}")

                if os.path.exists(preop_sql_path):
                    config["preoperation"] = load_file(preop_sql_path)

                # Validate that the 'table' value matches the subdirectory name
                destination_table = config.get("destination_table", "")
                if f"{destination_table}" != dir_name:
                    raise AirflowException(
                        f"migration subdirectory name {dir_name} doesn't make sense for config supplied {destination_table}"  # noqa
                    )

                # Dynamically generate the task ID
                config["task_id"] = f"migrate_{dir_name}_to_postgres"

                migrations.append(config)
    return migrations

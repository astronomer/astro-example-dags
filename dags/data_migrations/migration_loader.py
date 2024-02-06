import os
import json

from airflow.exceptions import AirflowException


def load_sql(file_path):
    """
    Load SQL content from a file.
    """
    with open(file_path, "r") as file:
        return file.read()


def load_migration_configs(relative_path):
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

                select_sql_path = os.path.join(migration_dir, "select.sql")
                preop_sql_path = os.path.join(migration_dir, "preoperation.sql")
                if os.path.exists(select_sql_path):
                    config["sql"] = load_sql(select_sql_path)
                else:
                    raise AirflowException(f"'select.sql' doesn't exist for migration {dir_name} in {migration_dir}")

                if os.path.exists(preop_sql_path):
                    config["preoperation"] = load_sql(preop_sql_path)

                # Validate that the 'table' value matches the subdirectory name
                if config.get("table") != dir_name:
                    raise AirflowException(f"Table name in config does not match the subdirectory name for {dir_name}")

                # Dynamically generate the task ID
                config["task_id"] = f"migrate_{dir_name}_to_postgres"

                migrations.append(config)
    return migrations

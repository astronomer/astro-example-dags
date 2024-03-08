import os
import json

from airflow.exceptions import AirflowException


def load_file(file_path):
    """
    Load json content from a file.
    """
    with open(file_path, "r") as file:
        return file.read()


def load_sheet_configs(sheets_dir):
    """
    Load and validate configuration for Google Sheets from a specified directory.

    :param sheets_dir: The absolute path to the directory containing sheet configurations.
    :return: A list of configurations for each Google Sheet.
    """

    if not os.path.isdir(sheets_dir):
        raise AirflowException(f"The directory {sheets_dir} does not exist.")

    sheets = []
    for file_name in os.listdir(sheets_dir):
        if file_name.endswith(".json"):  # Ensures only JSON files are processed
            config_path = os.path.join(sheets_dir, file_name)
            with open(config_path, "r") as file:
                config = json.load(file)

            # The name of the sheet is derived from the file name minus '.json'
            dir_name = file_name[:-5]

            # Validate that the 'table' matches the expected naming convention, if necessary
            table = config.get("table", "")
            if table != dir_name:
                raise AirflowException(
                    f"Google Sheet Configuration file {file_name} doesn't match the configured source table name {table}"  # noqa
                )

            # allow custom worksheet names
            worksheet = config.get("worksheet", None)
            if not worksheet:
                config["worksheet"] = config["table"]

            sheets.append(config)

    return sheets

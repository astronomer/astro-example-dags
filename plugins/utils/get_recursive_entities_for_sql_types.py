import os
import re

from airflow.exceptions import AirflowException

pattern = re.compile(
    r"""
CREATE\s+MATERIALIZED\s+VIEW\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+VIEW\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+TABLE\s+{{\s*schema\s*}}\.(\w+)\s*|
CREATE\s+OR\s+REPLACE\s+FUNCTION\s+{{\s*schema\s*}}\.(\w+)
""",
    re.IGNORECASE | re.VERBOSE,
)

sql_dir = "../../dags/sql/"
sql_dir_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_dir)


def get_recursive_entities_for_sql_types(
    sql_types,
    directory=sql_dir_abspath,
    first_call=True,
    add_table_columns_to_context=[],
):
    entity_names = []
    print(f"Current Directory: {directory}")

    current_level_entities = []

    print("directory", directory)
    if first_call or os.path.basename(directory) in sql_types:
        for item in os.listdir(directory):
            full_path = os.path.join(directory, item)
            if os.path.isfile(full_path) and item.endswith(".sql"):
                with open(full_path, "rb") as f:
                    content = f.read()

                filename_without_extension, _ = os.path.splitext(item)
                sql_string = content.decode("utf-8")

                matches = re.findall(pattern, sql_string)
                print(matches)
                if len(matches) < 1:
                    raise AirflowException(
                        f"SQL filename {full_path} doesn't contain a string which matches a doublecheck pattern {pattern}"  # noqa
                    )
                for match in matches:
                    entity_name = next((m for m in match if m), None)
                    if entity_name:
                        print(f"Matched {entity_name}")
                        if f"{filename_without_extension}" != entity_name:
                            raise AirflowException(
                                f"SQL filename {full_path} doesn't match its Entity Name {entity_name}"  # noqa
                            )
                        current_level_entities.append(entity_name)
                    else:
                        print(f"Failed to match for {filename_without_extension}")

    if current_level_entities:
        entity_names.extend(current_level_entities)

    cumulative_entity_names = add_table_columns_to_context + current_level_entities
    print("cumulative_entity_names", cumulative_entity_names)

    for item in os.listdir(directory):
        full_path = os.path.join(directory, item)
        if os.path.isdir(full_path):
            if first_call or item in sql_types:
                subdirectory_entities = get_recursive_entities_for_sql_types(
                    sql_types,
                    directory=full_path,
                    first_call=False,
                    add_table_columns_to_context=cumulative_entity_names,
                )
                if subdirectory_entities:
                    entity_names.extend(subdirectory_entities)

    print("entity_names", entity_names)
    return entity_names

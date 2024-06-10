import os

from plugins.utils.extract_entities_from_sql import extract_entity_name

sql_dir = "../../dags/sql/"
sql_dir_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_dir)


def get_recursive_entities_for_sql_types(
    sql_types,
    directory=sql_dir_abspath,
    first_call=True,
    add_table_columns_to_context=[],
):
    entity_names = []

    current_level_entities = []

    if first_call or os.path.basename(directory) in sql_types:
        for item in os.listdir(directory):
            full_path = os.path.join(directory, item)
            if os.path.isfile(full_path) and item.endswith(".sql"):
                with open(full_path, "rb") as f:
                    content = f.read()

                filename_without_extension, _ = os.path.splitext(item)
                sql_string = content.decode("utf-8")
                entity_name = extract_entity_name(sql_string)

                if entity_name:
                    current_level_entities.append(entity_name)

    if current_level_entities:
        entity_names.extend(current_level_entities)

    cumulative_entity_names = add_table_columns_to_context + current_level_entities

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

    return entity_names

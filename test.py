from plugins.utils.get_recursive_entities_for_sql_types import get_recursive_entities_for_sql_types

sql_types = ["dimensions"]

add_table_columns_to_context = get_recursive_entities_for_sql_types(sql_types)

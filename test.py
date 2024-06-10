import os

from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists
from plugins.utils.get_recursive_entities_for_sql_types import get_recursive_entities_for_sql_types

sql_types = ["dimensions", "cleansers"]

add_table_columns_to_context = get_recursive_entities_for_sql_types(sql_types)
sql_type = "reports"
sql_dir = "dags/sql/reports"
sql_dir_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_dir)

sql_files = get_recursive_sql_file_lists(
    sql_dir_abspath,
    subdir=sql_type,
    add_table_columns_to_context=add_table_columns_to_context,
    check_entity_pattern=True,
)

print(sql_files)

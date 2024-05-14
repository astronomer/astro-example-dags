import os

from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists

cleansers = "./dags/sql/cleansers"
cleansers_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), cleansers)

sql_files = get_recursive_sql_file_lists(
    cleansers_abspath, subdir="cleansers", add_table_columns_to_context=["dim__time"]
)

print(sql_files[-1])

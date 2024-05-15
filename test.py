import os

from plugins.utils.get_recursive_sql_file_lists import get_recursive_sql_file_lists

sql_types = ["cleansers", "indexes", "dimensions", "functions", "reports", "users"]

for sql_type in sql_types:
    dirs = f"./dags/sql/{sql_type}"
    sql_abspath = os.path.join(os.path.dirname(os.path.abspath(__file__)), dirs)

    sql_files = get_recursive_sql_file_lists(sql_abspath, subdir=sql_type, add_table_columns_to_context=["dim__time"])

    # print(sql_files[-1])

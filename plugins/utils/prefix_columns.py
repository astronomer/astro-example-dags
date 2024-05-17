# custom_jinja_filters.py
def prefix_columns(columns, alias, prefix, exclude_columns=[]):
    """
    Constructs a string of column names with a given prefix, excluding specified columns.

    :param columns: A list of column names to prefix.
    :param alias: The table alias used in SQL queries.
    :param prefix: The prefix to append to each column name.
    :param exclude_columns: A list of column names to exclude from prefixing.
    :return: A string of prefixed column names for SQL selection, excluding specified columns.
    """
    # Filter out excluded columns before applying the prefix
    filtered_columns = [col for col in columns if col not in exclude_columns]
    prefixed_columns = ",\n\t".join([f'{alias}."{col}" AS {prefix}_{col}' for col in filtered_columns])
    return prefixed_columns

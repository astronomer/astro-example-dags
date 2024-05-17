from airflow.exceptions import AirflowException


# custom_jinja_filters.py
def unprefix_columns(columns, alias, prefix, exclude_columns=[]):
    """
    use this to directly reference columns that have already been prefixed
    eg.
    select o.prefixed_column_name
    so you can effectively use o.* for previously imported columns using the same columns list

    :param columns: A list of original column names without the prefix.
    :param alias: The table alias used in SQL queries.
    :param prefix: The prefix which has been prepended to each column name already.
    :param exclude_columns: A list of column names to exclude from prefixing.
    :return: A string of prefixed column names for SQL selection, excluding specified columns.
    """
    if len(columns) < 1:
        raise AirflowException(
            f"Column list is empty for {alias} {prefix} given to unprefix_columns. Should it end in _columns?"
        )

    # Filter out excluded columns before applying the prefix
    filtered_columns = [col for col in columns if col not in exclude_columns]
    prefixed_columns = ",\n\t".join([f'{alias}."{prefix}__{col}"' for col in filtered_columns])
    return prefixed_columns

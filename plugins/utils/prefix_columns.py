# custom_jinja_filters.py


def prefix_columns(columns, alias, prefix):
    """
    Constructs a string of column names with a given prefix.

    :param columns: A list of column names to prefix.
    :param prefix: The prefix to append to each column name.
    :return: A string of prefixed column names for SQL selection.
    """
    prefixed_columns = ",\n\t".join([f"{alias}.{col} AS {prefix}_{col}" for col in columns])
    return prefixed_columns

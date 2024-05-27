# custom_jinja_filters.py


def truncate_exception(exception, length):
    """
    Constructs a string of column names with a given prefix, excluding specified columns.

    :param text: The string to truncate
    :param length: The length you wish to truncate the text to
    :return: truncated string
    """
    text = str(exception)
    truncate_length = length - 3
    truncated_text = (text[:truncate_length] + "...") if len(text) > length else text

    return truncated_text

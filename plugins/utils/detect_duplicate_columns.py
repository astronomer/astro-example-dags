from collections import Counter

from airflow.exceptions import AirflowException


def detect_duplicate_columns(input_list):
    # Count occurrences of each item
    counts = Counter(input_list)

    # Find items with more than one occurrence
    duplicates = [item for item, count in counts.items() if count > 1]

    # Report if duplicates are found
    if duplicates:
        raise AirflowException(f"Duplicate columns Detected: {duplicates}")

    else:
        print("No Duplicate Columns Detected")

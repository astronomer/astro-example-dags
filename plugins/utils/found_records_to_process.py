def found_records_to_process(parent_task_id, xcom_key, **kwargs):
    """
    A generic function to decide whether to continue with downstream tasks based on an integer value from XCom.
    0 is interpreted as False, any other value as True.

    :param parent_task_id: The ID of the task from which to pull an XCom value.
    :param xcom_key: The key used to pull the specific XCom value.
    """
    ti = kwargs["ti"]
    task_value = ti.xcom_pull(task_ids=parent_task_id, key=xcom_key)
    # Interpret 0 as False, any other integer as True
    return bool(task_value) and task_value != 0

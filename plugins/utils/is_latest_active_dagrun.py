import logging
from datetime import datetime

from airflow.models import DagRun


def is_latest_dagrun(dag_run):
    latest_run = get_latest_dagrun(dag_run.dag_id)
    print("latest_run", latest_run, latest_run.execution_date)
    print("dag_run", dag_run, dag_run.execution_date)
    return dag_run.execution_date == latest_run.execution_date


def get_latest_dagrun(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None


def get_num_active_dagruns(dag_id, conn_id="airflow_db"):

    dags_running = DagRun.find(dag_id=dag_id, state="running")
    dags_queued = DagRun.find(dag_id=dag_id, state="queued")
    dags_up_for_retry = DagRun.find(dag_id=dag_id, state="queued")
    print("dags_running", dags_running)
    print("dags_queued", dags_queued)
    print("dags_up_for_retry", dags_up_for_retry)
    all_dags = DagRun.find(dag_id=dag_id)

    print("ALL_DAGS", all_dags)

    return len(dags_running) + len(dags_queued) + len(dags_up_for_retry)


def is_latest_active_dagrun(**kwargs):
    """Ensure that there are no runs currently in progress and this is the most recent run."""
    num_active_dagruns = get_num_active_dagruns(kwargs["dag"].dag_id)
    logging.info(num_active_dagruns)
    # first, truncate the date to the schedule_interval granularity, then subtract the schedule_interval
    schedule_interval = kwargs["dag"].schedule_interval
    print("kwargs", kwargs)
    now_epoch = int(datetime.now().strftime("%s"))
    now_epoch_truncated = now_epoch - (now_epoch % schedule_interval.total_seconds())
    expected_run_epoch = now_epoch_truncated - schedule_interval.total_seconds()
    expected_run_execution_datetime = datetime.fromtimestamp(expected_run_epoch)
    is_latest_dagrun = kwargs["execution_date"].date() == expected_run_execution_datetime.date()
    logging.info("schedule_interval: {}".format(schedule_interval))
    logging.info("now_epoch: {}".format(now_epoch))
    logging.info("now_epoch_truncated: {}".format(now_epoch_truncated))
    logging.info("expected_run_epoch: {}".format(expected_run_epoch))
    logging.info("expected_run_execution_datetime: {}".format(expected_run_execution_datetime))
    logging.info("expected_run_execution_date: {}".format(expected_run_execution_datetime.date()))
    logging.info("execution_datetime: {}".format(kwargs["execution_date"]))
    logging.info("execution_date: {}".format(kwargs["execution_date"].date()))
    logging.info("Is latest dagrun: " + str(is_latest_dagrun))
    logging.info("Num active dag runs: " + str(num_active_dagruns))
    # If return value is False, then all downstream tasks will be skipped
    is_latest_active_dagrun = is_latest_dagrun and num_active_dagruns == 1
    return is_latest_active_dagrun

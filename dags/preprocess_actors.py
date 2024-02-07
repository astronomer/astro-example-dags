from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.utils.utils import load_table, normalize_nom, normalize_url, normalize_email, normalize_phone_number, \
    apply_normalization, save_to_database


def normalize_data():

    # Setup the PostgresHook connection
    pg_hook = PostgresHook(postgres_conn_id='your_connection_id')

    # Use the hook to get the engine
    engine = pg_hook.get_sqlalchemy_engine()

    df_act = load_table("qfdmo_acteur", engine)

    columns_to_exclude = ["identifiant_unique", "statut", "cree_le", "modifie_le"]
    normalization_map = {
        "nom": normalize_nom,
        "nom_commercial": normalize_nom,
        "ville": normalize_nom,
        "url": normalize_url,
        "adresse": normalize_nom,
        "adresse_complement": normalize_nom,
        "email": normalize_email,
        "telephone": normalize_phone_number,
    }

    df_act_cleaned = apply_normalization(df_act, normalization_map)

    save_to_database(df_act_cleaned, "lvao_actors_processed",engine)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lvao_data_normalization',
    default_args=default_args,
    description='DAG for normalizing LVAO actors data',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='normalize_and_save_data',
    python_callable=normalize_data,
    dag=dag,
)



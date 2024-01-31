from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago
import os
from pymongo import MongoClient
from pandas import DataFrame
from google.cloud import bigquery
import pandas as pd


#Argumentos por defecto
default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#Conexion a la base de datos de Mongo y Big Query
def get_connect_mongo():
    """
    Conectarse a la base de datos comun para extraer los datos a ser usados en el proceso
    """
    CONNECTION_STRING ="mongodb+srv://atlas:T6.HYX68T8Wr6nT@cluster0.enioytp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)

    return client

def get_connect_mongo_owner():
    """
    Conectarse a la base de datos individual para almacenar los datos
    """
    mongo_connection = 'mongodb+srv://datapath:datapath@datapath.tw1p2hc.mongodb.net/?retryWrites=true'
    client = MongoClient(mongo_connection)
    return client

#Funciones utilitarias para la carga de datos
def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")

def transform_date(text):
    text = str(text)
    d = text[0:10]
    return d

def get_group_status(text):
    text = str(text)
    if text =='CLOSED':
        d='END'
    elif text =='COMPLETE':
        d='END'
    else :
        d='TRANSIT'
    return d

#Obtener el valor del cambio
def get_exchangue_value():
    headers_files = {
        'tipocambio':["fecha","compra","venta","nan"]
        }
    dwn_url_tipcambio='https://www.sunat.gob.pe/a/txt/tipoCambio.txt'
    df = pd.read_csv(dwn_url_tipcambio, names=headers_files['tipocambio'], sep='|')
    list_t=df.values.tolist()
    return list_t[0][1]

#Funciones para la carga de datos
def load_orders():
    print(f" INICIO LOAD ORDERS")
    client = bigquery.Client()
    query_string = """
    drop table if exists `focus-ensign-285500.dep_raw.orders` ;
    """
    query_job = client.query(query_string)
    query_job.result()
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["orders"] 
    orders = collection_name.find({})  
    orders_df = DataFrame(orders)
    dbconnect.close()
    orders_df['_id'] = orders_df['_id'].astype(str)
    orders_df['order_date']  = orders_df['order_date'].map(transform_date)
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], format='%Y-%m-%d').dt.date
    orders_rows=len(orders_df)
    print(f" Se obtuvo  {orders_rows}  Filas de Orders")
    if orders_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.orders"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            orders_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla orders')


def load_order_items():
    print(f" INICIO LOAD ORDER ITEMS")
    client = bigquery.Client()
    query_string = """
    drop table if exists `focus-ensign-285500.dep_raw.order_items` ;
    """
    query_job = client.query(query_string)
    query_job.result()
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["order_items"] 
    order_items = collection_name.find({})  
    order_items_df = DataFrame(order_items)
    dbconnect.close()
    order_items_df['_id'] = order_items_df['_id'].astype(str)
    order_items_df['order_date']  = order_items_df['order_date'].map(transform_date)
    order_items_df['order_date'] = pd.to_datetime(order_items_df['order_date'], format='%Y-%m-%d').dt.date
    order_items_rows=len(order_items_df)
    print(f" Se obtuvo  {order_items_rows}  Filas de Orders Items")
    if order_items_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.order_items"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            order_items_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')


def load_products():
    print(f" INICIO LOAD PRODUCTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["products"] 
    products = collection_name.find({})
    products_df = DataFrame(products)
    dbconnect.close()
    products_df['_id'] = products_df['_id'].astype(str)
    products_df['product_description'] = products_df['product_description'].astype(str)
    products_rows=len(products_df)
    print(f" Se obtuvo  {products_rows}  Filas")
    products_rows=len(products_df)
    if products_rows>0 :
        client = bigquery.Client(project='premium-guide-410714')
        table_id =  "focus-ensign-285500.dep_raw.products"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_description", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("product_image", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            products_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla productos')


def load_customers():
    print(f" INICIO LOAD CUSTOMERS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["customers"] 
    customers = collection_name.find({})
    customers_df = DataFrame(customers)
    dbconnect.close()
    customers_df['_id'] = customers_df['_id'].astype(str)
    customers_rows=len(customers_df)
    print(f" Se obtuvo  {customers_rows}  Filas de Customers")
    if customers_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.customers"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("customer_fname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_lname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_email", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_password", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_street", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_state", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_zipcode", bigquery.enums.SqlTypeNames.INTEGER),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            customers_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla customers')

def load_categories():
    print(f" INICIO LOAD CATEGORIES")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["categories"] 
    categories = collection_name.find({})
    categories_df = DataFrame(categories)
    dbconnect.close()
    categories_df = categories_df.drop(columns=['_id'])
    categories_rows=len(categories_df)
    print(f" Se obtuvo  {categories_rows}  Filas de Categories")
    if categories_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.categories"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            categories_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla categories')



def load_departaments():
    print(f" INICIO LOAD DEPARTAMENTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["departments"] 
    departments = collection_name.find({})
    departments_df = DataFrame(departments)
    dbconnect.close()
    departments_df = departments_df.drop(columns=['_id'])
    departments_rows=len(departments_df)
    print(f" Se obtuvo  {departments_rows}  Filas de Departaments")
    if departments_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.departments"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("department_name", bigquery.enums.SqlTypeNames.STRING)
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            departments_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla departments')


def load_master_layer():
    print(f" INICIO LOAD MASTER LAYER")
    client = bigquery.Client()
    sql = """
        SELECT * FROM `focus-ensign-285500.dep_raw.order_items`
    """
    m_order_items_df = client.query(sql).to_dataframe()
    client = bigquery.Client()
    sql_2 = """
        SELECT *
        FROM `focus-ensign-285500.dep_raw.orders`
    """
    m_orders_df = client.query(sql_2).to_dataframe()
    df_join = m_orders_df.merge(m_order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
    df_master=df_join[[ 'order_id', 'order_date_x', 'order_customer_id',
       'order_status',  'order_item_id',
       'order_item_order_id', 'order_item_product_id', 'order_item_quantity',
       'order_item_subtotal', 'order_item_product_price']]
    df_master=df_master.rename(columns={"order_date_x":"order_date"})
    df_master['order_status_group']  = df_master['order_status'].map(get_group_status)
    df_master['order_date'] = df_master['order_date'].astype(str)
    df_master['order_date'] = pd.to_datetime(df_master['order_date'], format='%Y-%m-%d').dt.date
    valor_cambio = get_exchangue_value()
    df_master['order_item_subtotal_mn'] = df_master['order_item_subtotal']*valor_cambio
    df_master_rows=len(df_master)
    if df_master_rows>0 :
        client = bigquery.Client()

        table_id =  "focus-ensign-285500.dep_raw.master_order"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_subtotal_mn", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_status_group", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')

#Se crea la tabla bi_orders
def create_bi_table_orders():
    print(f" INICIO CREATE OR REPLACE BI TABLE ORDERS")
    client = bigquery.Client()
    query_string = """
    create or replace table `focus-ensign-285500.dep_raw.bi_orders` as
    SELECT 
     order_date,c.category_name ,d.department_name 
     , sum (a.order_item_subtotal) order_item_subtotal
     , sum (a.order_item_quantity) order_item_quantity
    FROM `focus-ensign-285500.dep_raw.master_order` a
    inner join  `focus-ensign-285500.dep_raw.products` b on
    a.order_item_product_id=b.product_id
    inner join `focus-ensign-285500.dep_raw.categories` c on
    b.product_category_id=c.category_id
    inner join `focus-ensign-285500.dep_raw.departments` d on
    c.category_department_id=d.department_id
    group by order_date,c.category_name ,d.department_name
    """
    query_job = client.query(query_string)
    query_job.result()


#Se crea la tabla bi_clientes
def create_bi_table_clientes():
    print(f" INICIO CREATE OR REPLACE BI TABLE CLIENTES")
    lient = bigquery.Client()
    query_string = """
    create or replace table `focus-ensign-285500.dep_raw.bi_clientes` as
    select customer_id,category_name , order_item_subtotal from
    (
      SELECT customer_id,category_name,order_item_subtotal,
      RANK() OVER (PARTITION BY customer_id ORDER BY order_item_subtotal DESC) AS rank_ 
     FROM (
          SELECT 
          d.customer_id ,c.category_name 
          , sum (a.order_item_subtotal) order_item_subtotal
          FROM `focus-ensign-285500.dep_raw.master_order` a
          inner join  `focus-ensign-285500.dep_raw.products` b on
          a.order_item_product_id=b.product_id
          inner join `focus-ensign-285500.dep_raw.categories` c on
          b.product_category_id=c.category_id
          inner join `focus-ensign-285500.dep_raw.customers` d on
          a.order_customer_id=d.customer_id
          group by 1,2
      )
    )
    where rank_=1
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)


#Cargar la tabla de segmento de clientes de bigquery a su  MongoDB personal
def load_table_clientes_mongo():
    print(f" INICIO LOAD TABLE CLIENTES")
    client = bigquery.Client()
    sql_clientes = """
        SELECT *
        FROM `focus-ensign-285500.dep_raw.bi_clientes`
    """
    clientes_df = client.query(sql_clientes).to_dataframe()
    #Borrar la coleccion en caso de que exista
    connection = get_connect_mongo_owner()
    data_deleted = connection['retail_db']["clientes_bq"].delete_many({})
    print(data_deleted.deleted_count)
    connection.close()
    connection = get_connect_mongo_owner()
    data_insert = clientes_df.to_dict(orient='records')
    connection['retail_db']["clientes_bq"].insert_many(data_insert, ordered=False)
    connection.close()



with DAG(
    dag_id="load_project",
    schedule="20 04 * * *", 
    start_date=days_ago(2), 
    default_args=default_args
) as dag:
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )
    step_load_orders = PythonOperator(
        task_id='load_orders_id',
        python_callable=load_orders,
        dag=dag
    )
    step_load_order_items = PythonOperator(
        task_id='load_order_items',
        python_callable=load_order_items,
        dag=dag
    )
    step_load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_products,
        dag=dag
    )
    step_load_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_customers,
        dag=dag
    )
    step_load_categories = PythonOperator(
        task_id='load_categories',
        python_callable=load_categories,
        dag=dag
    )
    step_load_departaments = PythonOperator(
        task_id='load_departaments',
        python_callable=load_departaments,
        dag=dag
    )
    step_load_master_layer = PythonOperator(
        task_id='load_master_layer',
        python_callable=load_master_layer,
        dag=dag
    )
    step_create_bi_table_orders = PythonOperator(
        task_id='create_bi_table_orders',
        python_callable=create_bi_table_orders,
        dag=dag
    )
    step_create_bi_table_clientes = PythonOperator(
        task_id='create_bi_table_clientes',
        python_callable=create_bi_table_clientes,
        dag=dag
    )
    step_load_table_clientes_mongo = PythonOperator(
        task_id='load_table_clientes_mongo',
        python_callable=load_table_clientes_mongo,
        dag=dag
    )
    step_end = PythonOperator(
        task_id='step_end_id',
        python_callable=end_process,
        dag=dag
    )

    step_start>>step_load_orders
    step_start>>step_load_order_items
    step_start>>step_load_products
    step_start>>step_load_customers
    step_start>>step_load_categories
    step_start>>step_load_departaments
    [step_load_orders, step_load_order_items, step_load_products, step_load_customers, step_load_categories, step_load_departaments] >> step_load_master_layer >> step_create_bi_table_orders >> step_create_bi_table_clientes >> step_load_table_clientes_mongo >> step_end
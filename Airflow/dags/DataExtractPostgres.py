from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def extract_TableCaterogies():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT CATEGORY_ID, CATEGORY_NAME, DESCRIPTION FROM CATEGORIES"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.CATEGORIES (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.CATEGORIES', snowflake_data)

def extract_TableCustomers():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax FROM public.customers"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.CUSTOMERS (CUSTOMER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.CUSTOMERS', snowflake_data)

with DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    description='Extract data from PostgreSQL and load into Snowflake',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:
    
    task_TableCaterogies = PythonOperator(
        task_id='TableCaterogies',
        python_callable=extract_TableCaterogies
    )
    
    task_TableCustomers = PythonOperator(
        task_id='TableCustomers',
        python_callable=extract_TableCustomers
    )
    
    # Operador para truncar a tabela no Snowflake
    truncate_Customers = SnowflakeOperator(
        task_id='truncate_customers_table',
        snowflake_conn_id='snowflake_conn_id',  # Nome da conexão configurada no Airflow
        sql="TRUNCATE TABLE DW.BRONZE.CUSTOMERS",
    )

    truncate_Categories = SnowflakeOperator(
        task_id='truncate_categories_table',
        snowflake_conn_id='snowflake_conn_id',  # Nome da conexão configurada no Airflow
        sql="TRUNCATE TABLE DW.BRONZE.CATEGORIES",
    )

    #truncate_Categories>>truncate_Customers>>task_TableCaterogies>>task_TableCustomers
    [truncate_Categories, truncate_Customers] >> task_TableCaterogies >> task_TableCustomers


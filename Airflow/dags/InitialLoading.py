from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

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

def extract_TableEmployees():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT EMPLOYEE_ID, LAST_NAME, FIRST_NAME, TITLE, TITLE_OF_COURTESY, BIRTH_DATE, HIRE_DATE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, HOME_PHONE, EXTENSION, NOTES, REPORTS_TO, PHOTO_PATH FROM EMPLOYEES"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],  row[11], row[12], row[13], row[14], row[15], row[16], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.EMPLOYEES (EMPLOYEE_ID, LAST_NAME, FIRST_NAME, TITLE, TITLE_OF_COURTESY, BIRTH_DATE, HIRE_DATE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, HOME_PHONE, EXTENSION, NOTES, REPORTS_TO, PHOTO_PATH, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.EMPLOYEES', snowflake_data)

def extract_TableOrders():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT ORDER_ID, CUSTOMER_ID, EMPLOYEE_ID, ORDER_DATE, REQUIRED_DATE, SHIPPED_DATE, SHIP_VIA, FREIGHT, SHIP_NAME, SHIP_ADDRESS, SHIP_CITY, SHIP_REGION, SHIP_POSTAL_CODE, SHIP_COUNTRY FROM ORDERS"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],  row[11], row[12], row[13], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.ORDERS (ORDER_ID, CUSTOMER_ID, EMPLOYEE_ID, ORDER_DATE, REQUIRED_DATE, SHIPPED_DATE, SHIP_VIA, FREIGHT, SHIP_NAME, SHIP_ADDRESS, SHIP_CITY, SHIP_REGION, SHIP_POSTAL_CODE, SHIP_COUNTRY, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.ORDERS', snowflake_data)

def extract_TableOrdersDetails():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT ORDER_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY, DISCOUNT FROM ORDER_DETAILS"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4],  datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.ORDER_DETAILS (ORDER_ID, PRODUCT_ID, UNIT_PRICE, QUANTITY, DISCOUNT, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.ORDER_DETAILS', snowflake_data)

def extract_TableProducts():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT PRODUCT_ID, PRODUCT_NAME, SUPPLIER_ID, CATEGORY_ID, QUANTITY_PER_UNIT, UNIT_PRICE, UNITS_IN_STOCK, UNITS_ON_ORDER, REORDER_LEVEL, DISCONTINUED FROM PRODUCTS"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.PRODUCTS (PRODUCT_ID, PRODUCT_NAME, SUPPLIER_ID, CATEGORY_ID, QUANTITY_PER_UNIT, UNIT_PRICE, UNITS_IN_STOCK, UNITS_ON_ORDER, REORDER_LEVEL, DISCONTINUED, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.PRODUCTS', snowflake_data)

def extract_TableRegion():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT region_id, region_description FROM region"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1],  datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.PRODUCTS (REGION_ID, REGION_DESCRIPTION, DATA_EXTRACAO)
        VALUES (%s, %s, %s  )
    """
    snowflake_hook.insert_rows('DW.BRONZE.REGION', snowflake_data)

def extract_TableShippers():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT SHIPPER_ID, COMPANY_NAME, PHONE FROM SHIPPERS"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.SHIPPERS (SHIPPER_ID, COMPANY_NAME, PHONE, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s  )
    """
    snowflake_hook.insert_rows('DW.BRONZE.SHIPPERS', snowflake_data)

def extract_TableSuppliers():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE FROM SUPPLIERS"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.SUPPLIERS (SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s  )
    """
    snowflake_hook.insert_rows('DW.BRONZE.SUPPLIERS', snowflake_data)

def extract_TableTerritories():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT territory_id, territory_description, region_id FROM territories"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], row[2], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.TERRITORIES (TERRITORY_ID, TERRITORY_DESCRIPTION, REGION_ID, DATA_EXTRACAO)
        VALUES (%s, %s, %s, %s )
    """
    snowflake_hook.insert_rows('DW.BRONZE.TERRITORIES', snowflake_data)

def extract_TableEmployeeTerritories():
    # Conexão PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    sql_query = "SELECT EMPLOYEE_ID, TERRITORY_ID FROM EMPLOYEE_TERRITORIES"
    records = postgres_hook.get_records(sql_query)

    # Preparar dados para inserção no Snowflake
    snowflake_data = [
        (row[0], row[1], datetime.now()) for row in records
    ]
    
    # Conexão Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    insert_query = """
        INSERT INTO DW.BRONZE.EMPLOYEE_TERRITORIES (EMPLOYEE_ID, TERRITORY_ID, DATA_EXTRACAO)
        VALUES (%s, %s, %s)
    """
    snowflake_hook.insert_rows('DW.BRONZE.EMPLOYEE_TERRITORIES', snowflake_data)

with DAG(
    'Full_Initial_Load',
    default_args=default_args,
    description='Carga inicial para criar as tabelas no DW e popular com os dados',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:
    
    sql_full_load = """
    create TABLE IF NOT EXISTS DW.BRONZE.CATEGORIES (
	CATEGORY_ID NUMBER(38,0) NOT NULL,
	CATEGORY_NAME VARCHAR(15) NOT NULL,
	DESCRIPTION VARCHAR(16777216),
	DATA_EXTRACAO DATE
    );
    create TABLE IF NOT EXISTS  DW.BRONZE.CUSTOMERS (
	CUSTOMER_ID VARCHAR(5) NOT NULL,
	COMPANY_NAME VARCHAR(40) NOT NULL,
	CONTACT_NAME VARCHAR(30),
	CONTACT_TITLE VARCHAR(30),
	ADDRESS VARCHAR(60),
	CITY VARCHAR(15),
	REGION VARCHAR(15),
	POSTAL_CODE VARCHAR(10),
	COUNTRY VARCHAR(15),
	PHONE VARCHAR(24),
	FAX VARCHAR(24),
	DATA_EXTRACAO DATE
    );    
    create TABLE IF NOT EXISTS  DW.BRONZE.EMPLOYEE_TERRITORIES (
	EMPLOYEE_ID NUMBER(38,0) NOT NULL,
	TERRITORY_ID VARCHAR(20) NOT NULL,
	DATA_EXTRACAO DATE
    );
    create TABLE IF NOT EXISTS  DW.BRONZE.EMPLOYEES (
        EMPLOYEE_ID NUMBER(38,0) NOT NULL,
        LAST_NAME VARCHAR(20) NOT NULL,
        FIRST_NAME VARCHAR(10) NOT NULL,
        TITLE VARCHAR(30),
        TITLE_OF_COURTESY VARCHAR(25),
        BIRTH_DATE DATE,
        HIRE_DATE DATE,
        ADDRESS VARCHAR(60),
        CITY VARCHAR(15),
        REGION VARCHAR(15),
        POSTAL_CODE VARCHAR(10),
        COUNTRY VARCHAR(15),
        HOME_PHONE VARCHAR(24),
        EXTENSION VARCHAR(4),
        NOTES VARCHAR(16777216),
        REPORTS_TO NUMBER(38,0),
        PHOTO_PATH VARCHAR(255),
        DATA_EXTRACAO DATE
    );    
    create TABLE IF NOT EXISTS  DW.BRONZE.ORDER_DETAILS (
        ORDER_ID NUMBER(38,0) NOT NULL,
        PRODUCT_ID NUMBER(38,0) NOT NULL,
        UNIT_PRICE FLOAT NOT NULL,
        QUANTITY NUMBER(38,0) NOT NULL,
        DISCOUNT FLOAT NOT NULL,
        DATA_EXTRACAO DATE
    );    
    create TABLE IF NOT EXISTS  DW.BRONZE.ORDERS (
        ORDER_ID NUMBER(38,0) NOT NULL,
        CUSTOMER_ID VARCHAR(5),
        EMPLOYEE_ID NUMBER(38,0),
        ORDER_DATE DATE,
        REQUIRED_DATE DATE,
        SHIPPED_DATE DATE,
        SHIP_VIA NUMBER(38,0),
        FREIGHT FLOAT,
        SHIP_NAME VARCHAR(40),
        SHIP_ADDRESS VARCHAR(60),
        SHIP_CITY VARCHAR(15),
        SHIP_REGION VARCHAR(15),
        SHIP_POSTAL_CODE VARCHAR(10),
        SHIP_COUNTRY VARCHAR(15),
        DATA_EXTRACAO DATE
    );  
    create TABLE IF NOT EXISTS  DW.BRONZE.PRODUCTS (
        PRODUCT_ID NUMBER(38,0) NOT NULL,
        PRODUCT_NAME VARCHAR(40) NOT NULL,
        SUPPLIER_ID NUMBER(38,0),
        CATEGORY_ID NUMBER(38,0),
        QUANTITY_PER_UNIT VARCHAR(20),
        UNIT_PRICE FLOAT,
        UNITS_IN_STOCK NUMBER(38,0),
        UNITS_ON_ORDER NUMBER(38,0),
        REORDER_LEVEL NUMBER(38,0),
        DISCONTINUED NUMBER(38,0) NOT NULL,
        DATA_EXTRACAO DATE
    );    
    create TABLE IF NOT EXISTS DW.BRONZE.REGION (
	REGION_ID NUMBER(38,0) NOT NULL,
	REGION_DESCRIPTION VARCHAR(60) NOT NULL,
	DATA_EXTRACAO DATE
    );  
    create TABLE IF NOT EXISTS DW.BRONZE.SHIPPERS (
	SHIPPER_ID NUMBER(38,0) NOT NULL,
	COMPANY_NAME VARCHAR(40) NOT NULL,
	PHONE VARCHAR(24),
	DATA_EXTRACAO DATE
    );
    create TABLE IF NOT EXISTS DW.BRONZE.SUPPLIERS (
	SUPPLIER_ID NUMBER(38,0) NOT NULL,
	COMPANY_NAME VARCHAR(40) NOT NULL,
	CONTACT_NAME VARCHAR(30),
	CONTACT_TITLE VARCHAR(30),
	ADDRESS VARCHAR(60),
	CITY VARCHAR(15),
	REGION VARCHAR(15),
	POSTAL_CODE VARCHAR(10),
	COUNTRY VARCHAR(15),
	PHONE VARCHAR(24),
	FAX VARCHAR(24),
	HOMEPAGE VARCHAR(16777216),
	DATA_EXTRACAO DATE
    );
    create TABLE IF NOT EXISTS DW.BRONZE.TERRITORIES (
	TERRITORY_ID VARCHAR(20) NOT NULL,
	TERRITORY_DESCRIPTION VARCHAR(60) NOT NULL,
	REGION_ID NUMBER(38,0) NOT NULL,
	DATA_EXTRACAO DATE
    );
    TRUNCATE TABLE DW.BRONZE.CATEGORIES;
    TRUNCATE TABLE DW.BRONZE.CUSTOMERS;
    TRUNCATE TABLE DW.BRONZE.EMPLOYEE_TERRITORIES;
    TRUNCATE TABLE DW.BRONZE.EMPLOYEES;
    TRUNCATE TABLE DW.BRONZE.ORDER_DETAILS;
    TRUNCATE TABLE DW.BRONZE.ORDERS;
    TRUNCATE TABLE DW.BRONZE.PRODUCTS;
    TRUNCATE TABLE DW.BRONZE.REGION;
    TRUNCATE TABLE DW.BRONZE.SHIPPERS;
    TRUNCATE TABLE DW.BRONZE.SUPPLIERS;
    TRUNCATE TABLE DW.BRONZE.TERRITORIES;
    """

    silver_procedures = """
    CALL DW.SILVER.SILVER_CATEGORIES();
    CALL DW.SILVER.SILVER_CUSTOMERS();
    CALL DW.SILVER.SILVER_EMPLOYEES();
    CALL DW.SILVER.SILVER_EMPLOYEE_TERRITORIES();
    CALL DW.SILVER.SILVER_ORDERDETAILS();
    CALL DW.SILVER.SILVER_ORDERS();
    CALL DW.SILVER.SILVER_PRODUCTS();
    CALL DW.SILVER.SILVER_REGION();
    CALL DW.SILVER.SILVER_SHIPPERS();
    CALL DW.SILVER.SILVER_SUPPLIERS();
    CALL DW.SILVER.SILVER_TERRITORIES();
    """

    Bronze_Full_Load = SnowflakeOperator(
        task_id='Bronze_Full_Load',
        snowflake_conn_id='snowflake_conn_id',  # Conexão configurada no Airflow
        sql=sql_full_load,
    )
    
    Get_Data_Caterogies = PythonOperator(
       task_id='Get_Data_Caterogies',
       python_callable=extract_TableCaterogies
    )
    
    Get_Data_Customers = PythonOperator(
       task_id='Get_Data_Customers',
       python_callable=extract_TableCustomers
    )

    Get_Data_Employees = PythonOperator(
       task_id='Get_Data_Employees',
       python_callable=extract_TableEmployees
    )

    Get_Data_Orders = PythonOperator(
       task_id='Get_Data_Orders',
       python_callable=extract_TableOrders
    )
    
    Get_Data_OrdersDetails = PythonOperator(
       task_id='Get_Data_OrdersDetails',
       python_callable=extract_TableOrdersDetails
    )

    Get_Data_Products = PythonOperator(
        task_id='Get_Data_Products',
        python_callable=extract_TableProducts
    )

    Get_Data_Region = PythonOperator(
        task_id='Get_Data_Region',
        python_callable=extract_TableRegion
    )

    Get_Data_Shippers = PythonOperator(
        task_id='Get_Data_Shippers',
        python_callable=extract_TableShippers
    )

    Get_Data_Territories = PythonOperator(
        task_id='Get_Data_Territories',
        python_callable=extract_TableTerritories
    )

    Get_Data_EmployeeTerritories = PythonOperator(
        task_id='Get_Data_EmployeeTerritories',
        python_callable=extract_TableEmployeeTerritories
    )

    Get_Data_Suppliers = PythonOperator(
        task_id='Get_Data_Suppliers',
        python_callable=extract_TableSuppliers
    )

    Silver_Full_Load = SnowflakeOperator(
        task_id='Silver_Full_Load',
        snowflake_conn_id='snowflake_conn_id',  # Conexão configurada no Airflow
        sql=silver_procedures,
    )

    Gold_Dim_Full_Data = SnowflakeOperator(
        task_id='Gold_Dim_Full_Data',
        snowflake_conn_id='snowflake_conn_id',  # Conexão configurada no Airflow
        sql="CALL DW.GOLD.PROC_DIM_TEMPO('1996-07-01', '1998-05-31');",
    )

    Gold_Dim_Full_Customers = SnowflakeOperator(
        task_id='Gold_Dim_Full_Customers',
        snowflake_conn_id='snowflake_conn_id',  # Conexão configurada no Airflow
        sql='CALL DW.GOLD.PROC_DIM_CUSTOMER();',
    )

    Gold_Dim_Full_Products = SnowflakeOperator(
        task_id='Gold_Dim_Full_Products',
        snowflake_conn_id='snowflake_conn_id',  # Conexão configurada no Airflow
        sql='CALL DW.GOLD.PROC_DIM_PRODUCTS();',
    )

    # Ponto de sincronização (aguardar os 3)
    wait1 = DummyOperator(task_id='wait1', trigger_rule='all_success')
    wait2 = DummyOperator(task_id='wait2', trigger_rule='all_success')
    #wait3 = DummyOperator(task_id='wait3', trigger_rule='all_success')
    #wait4 = DummyOperator(task_id='wait4', trigger_rule='all_success')

    #Bronze_Full_Load >>  [Get_Data_Caterogies,Get_Data_Customers,Get_Data_Employees] >> wait1 >> [Get_Data_Orders,Get_Data_OrdersDetails,Get_Data_Products] >> wait2 >> [Get_Data_Region,Get_Data_Shippers,Get_Data_Suppliers] >> wait3 >> [Get_Data_Territories,Get_Data_EmployeeTerritories] >> Silver_Full_Load >> wait4 >> [Gold_Dim_Data,Gold_Dim_Customers,Gold_Dim_Products]
    Bronze_Full_Load >>  [Get_Data_Caterogies,Get_Data_Customers,Get_Data_Employees,Get_Data_Orders,Get_Data_OrdersDetails,Get_Data_Products] >> wait1 >> [Get_Data_Region,Get_Data_Shippers,Get_Data_Suppliers,Get_Data_Territories,Get_Data_EmployeeTerritories] >> wait2 >> Silver_Full_Load >> [Gold_Dim_Full_Data,Gold_Dim_Full_Customers,Gold_Dim_Full_Products]
    
    
    

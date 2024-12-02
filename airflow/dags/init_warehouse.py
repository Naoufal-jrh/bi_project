from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd
import os
from pathlib import Path
import json

# Get the absolute path to the DAG folder
dag_path = os.path.dirname(os.path.abspath(__file__))

def serialize_dates(obj):
    """Helper function to serialize datetime objects"""
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.strftime('%Y-%m-%d')
    return obj

def extract_data(**context):
    """
    Extract historical sales data from Excel file with enhanced error handling
    """

    try:
        # Get the absolute path to the DAG directory
        dag_path = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(dag_path, 'data', 'test_sales_data.xlsx')

        # Log the file path being accessed
        print(f"Attempting to read Excel file from: {excel_path}")

        # Check if file exists
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found at: {excel_path}")

        # Check file permissions
        if not os.access(excel_path, os.R_OK):
            raise PermissionError(f"No read permission for file: {excel_path}")

        # Read the Excel file
        print("Reading Excel file...")
        df = pd.read_excel(excel_path)

        # Log data shape
        print(f"Successfully read data with shape: {df.shape}")

        # Basic data validation
        if df.empty:
            raise ValueError("Excel file contains no data")

        # Convert DataFrame to records for XCom
        # Convert all datetime columns to date string format
        date_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in date_columns:
            df[col] = df[col].apply(lambda x: serialize_dates(x))

        # Convert to dictionary with date handling
        df_dict = df.to_dict(orient='records')

        # Store in XCom
        context['task_instance'].xcom_push(key='sales_data', value=df_dict)

        # Log success
        print("Data extraction completed successfully")
        return "Data extraction successful"

    except FileNotFoundError as e:
        print(f"File not found error: {str(e)}")
        raise
    except PermissionError as e:
        print(f"Permission error: {str(e)}")
        raise
    except pd.errors.EmptyDataError as e:
        print(f"Empty data error: {str(e)}")
        raise
    except ValueError as e:
        print(f"Value error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error during data extraction: {str(e)}")
        raise






def split_data(**context):
    """
    Split the data into fact and dimension tables and format string fields.
    """
    try:
        # Pull the DataFrame from XCom
        df_dict = context['task_instance'].xcom_pull(task_ids="extract_data", key='sales_data')
        data = pd.DataFrame(df_dict)

        # Function to clean string fields
        def clean_strings(df):
            for col in df.select_dtypes(include=['object']).columns:  # Select string columns
                df[col] = df[col].str.replace("'", " ", regex=False)  # Replace single quotes with a space
                df[col] = df[col].str.replace('"', " ", regex=False)  # Replace single quotes with a space
            return df

        # Create dimension tables and clean their string fields
        dim_customer = clean_strings(
            data[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates().rename(
                columns={'Customer ID': 'customer_id', 'Customer Name': 'customer_name', 'Segment': 'segment'}
            )
        )

        dim_product = clean_strings(
            data[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates().rename(
                columns={'Product ID': 'product_id', 'Product Name': 'product_name', 'Category': 'category', 'Sub-Category': 'sub_category'}
            )
        )

        dim_location = clean_strings(
            data[['City', 'State', 'Country', 'Region', 'Market']].drop_duplicates().rename(
                columns={'City': 'city', 'State': 'state', 'Country': 'country', 'Region': 'region', 'Market': 'market'}
            )
        )
        dim_location = dim_location.reset_index().rename(columns={'index': 'location_id'})

        # Create fact table and clean its string fields
        fact_sales = clean_strings(
            data[['Order ID','Order Date', 'Sales', 'Quantity', 'Profit', 'Customer ID', 'Product ID', 'City', 'State', 'Country', 'Region', 'Market']].rename(
                columns={'Order ID': "order_id",'Order Date': 'order_date', 'Sales': 'sales', 'Quantity': 'quantity', 'Profit': 'profit', 'Customer ID': 'customer_id', 'Product ID': 'product_id', 'City': 'city', 'State': 'state', 'Country': 'country', 'Region': 'region', 'Market': 'market'}
            )
        )

        fact_sales = fact_sales.merge(dim_location[['location_id', 'city', 'state', 'country', 'region', 'market']],
                                      on=['city', 'state', 'country', 'region', 'market'],
                                      how='left')

        fact_sales = fact_sales.drop(columns=['city', 'state', 'country', 'region', 'market'])

        # Convert DataFrames to serializable format
        tables = {
            'dim_customer': json.dumps(dim_customer.to_dict(orient='records')),
            'dim_product': json.dumps(dim_product.to_dict(orient='records')),
            'dim_location': json.dumps(dim_location.to_dict(orient='records')),
            'fact_sales': json.dumps(fact_sales.to_dict(orient='records'))
        }

        for table_name, table_data in tables.items():
            context['task_instance'].xcom_push(key=table_name, value=table_data)

        return "Data splitting completed successfully"

    except Exception as e:
        raise Exception(f"Splitting Data Failed : {str(e)}")


# Create table SQL statements
create_tables_sql = """
CREATE DATABASE IF NOT EXISTS POWERSTOREDB;
USE DATABASE POWERSTOREDB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(100) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100)
);


CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id VARCHAR(100) PRIMARY KEY,
    customer_name VARCHAR(255),
    segment VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    region VARCHAR(100),
    market VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id VARCHAR(100) PRIMARY KEY,
    order_date DATE,
    sales FLOAT,
    quantity INTEGER,
    profit FLOAT,
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    location_id INTEGER
);
"""

def generate_insert_sql(table_name, columns):
    """
    Generates an INSERT SQL statement for a given table and its columns,
    with special handling for escaping JSON data.

    Args:
        table_name (str): Name of the table to insert into.
        columns (list): List of column names to insert into.

    Returns:
        str: SQL INSERT statement.
    """
    # Pull the XCom data for the table (list of dictionaries)
    xcom_data = "{{ task_instance.xcom_pull(task_ids='split_data', key='" + table_name + "') }}"

    # Ensure the XCom data is converted to a JSON string format

    # json_data = xcom_data.replace("'", "\"")

    json_data = f"'{xcom_data}'"

    # print("this is the json passed to the parameters :", xcom_data)


    # Generate SELECT clause mapping JSON fields to table columns with quote escaping
    select_clause = ",\n ".join([
        f"value:{col}::string AS {col}"
        for col in columns
    ])

    # Generate the full SQL statement
    return f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    SELECT {select_clause}
    FROM TABLE(
        FLATTEN(input => parse_json({json_data}))
    );
    """



"""
DAG Definition
"""
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'snowflake_conn_id': "snowflake_conn",  # Set this in Airflow connections
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'init_warehouse_with_historical_data',
    default_args=default_args,
    description='Initialize data warehouse with historical data',
    schedule_interval=None,  # Run manually
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

"""
Tasks Definition
"""
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

split_data_task = PythonOperator(
    task_id='split_data',
    python_callable=split_data,
    provide_context=True,
    dag=dag,
)

create_tables_task = SnowflakeOperator(
    task_id='create_tables',
    sql=create_tables_sql,
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)


# Create separate insert tasks for each table
insert_dim_product = SnowflakeOperator(
    task_id='insert_dim_product',
    sql=generate_insert_sql('dim_product', ['product_id', 'product_name', 'category', 'sub_category']),
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

insert_dim_customer = SnowflakeOperator(
    task_id='insert_dim_customer',
    sql=generate_insert_sql('dim_customer', ['customer_id', 'customer_name', 'segment']),
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

insert_dim_location = SnowflakeOperator(
    task_id='insert_dim_location',
    sql=generate_insert_sql('dim_location', ['location_id', 'city', 'state', 'country', 'region', 'market']),
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

insert_fact_sales = SnowflakeOperator(
    task_id='insert_fact_sales',
    sql=generate_insert_sql('fact_sales', ['order_id', 'order_date', 'sales', 'quantity', 'profit', 'customer_id', 'product_id', 'location_id']),
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)


# Set task dependencies
extract_data_task >> split_data_task >> create_tables_task >> [
    insert_dim_product,
    insert_dim_customer,
    insert_dim_location,
] >> insert_fact_sales


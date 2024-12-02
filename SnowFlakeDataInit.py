import pandas as pd
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

class SnowFlakeDataInit:

    connection = None
    tables = []

    def __init__(self, file_path):
        load_dotenv()
        self.file_path = file_path
        self.snowflake_conn_params = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE")
        }

    def extract_data(self):
        data = pd.read_excel(self.file_path, sheet_name="Orders")
        return data
    

    def split_data(self, data):

        print("Splitting data into fact and dimension tables...")

        dim_customer = data[['Customer ID', 'Customer Name', 'Segment', 'Country']].drop_duplicates().rename(
            columns={'Customer ID': 'customer_id', 'Customer Name': 'customer_name', 'Segment': 'segment'}
        )

        dim_product = data[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates().rename(
            columns={'Product ID': 'product_id', 'Product Name': 'product_name', 'Category': 'category', 'Sub-Category': 'sub_category'}
        )

        dim_location = data[['City', 'State', 'Country', 'Region', 'Market']].drop_duplicates().rename(
            columns={'City': 'city', 'State': 'state', 'Country': 'country', 'Region': 'region', 'Market': 'market'}
        )
        dim_location = dim_location.reset_index().rename(columns={'index': 'location_id'})

        fact_sales = data[['Order Date', 'Sales', 'Quantity', 'Profit', 'Customer ID', 'Product ID', 'City', 'State', 'Country', 'Region', 'Market']].rename(
            columns={'Order Date': 'order_date', 'Sales': 'sales', 'Quantity': 'quantity', 'Profit': 'profit', 'Customer ID': 'customer_id', 'Product ID': 'product_id', 'City': 'city', 'State': 'state', 'Country': 'country', 'Region': 'region', 'Market': 'market'}
        )

        fact_sales = fact_sales.merge(dim_location[['location_id', 'city', 'state', 'country', 'region', 'market']],
                                      on=['city', 'state', 'country', 'region', 'market'],
                                      how='left')

        fact_sales = fact_sales.drop(columns=['city', 'state', 'country', 'region', 'market'])

        return dim_customer, dim_product, dim_location, fact_sales
    

    def create_tables(self):
        self.connect_to_snowflake()
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS dim_customer (customer_id INT, customer_name STRING, segment STRING)")
        cursor.execute("CREATE TABLE IF NOT EXISTS dim_product (product_id INT, product_name STRING, category STRING, sub_category STRING)")
        cursor.execute("CREATE TABLE IF NOT EXISTS dim_location (location_id INT, city STRING, state STRING, country STRING, region STRING, market STRING)")
        cursor.execute("CREATE TABLE IF NOT EXISTS fact_sales (order_date DATE, sales FLOAT, quantity INT, profit FLOAT, customer_id INT, product_id INT, location_id INT)")
        cursor.close()
        self.close_connection()
        print("Tables created successfully")




    
    
    def connect_to_snowflake(self):
        if(self.connection == None):
            print("connecting to snowflake...")
            self.connection = snowflake.connector.connect(
                user=self.snowflake_conn_params["user"],
                password=self.snowflake_conn_params["password"],
                account=self.snowflake_conn_params["account"],
                warehouse=self.snowflake_conn_params["warehouse"],
                database=self.snowflake_conn_params["database"],
                schema=self.snowflake_conn_params["schema"],
                role=self.snowflake_conn_params["role"]
            )
            print("Connected to Snowflake")
        else:
            print("Already connected to Snowflake")
    
    def close_connection(self):
        if(self.connection != None):
            self.connection.close()
            self.connection = None
            print("Connection closed")
        else:
            print("No active connection to close")

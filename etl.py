import os

import pandas as pd
import cryptography

from sqlalchemy import create_engine, text

#connect to the mysql database
engine = create_engine(
    "mysql+pymysql://app:AppPass!1@127.0.0.1:3306/mydb",
    pool_pre_ping=True,
)

os.getcwd()

#load the csvs from the "data_setup/Data CSV" folder with relative paths?
brands = pd.read_csv('data_setup/Data CSV/brands.csv')
categories = pd.read_csv('data_setup/Data CSV/categories.csv')
products = pd.read_csv('data_setup/Data CSV/products.csv')
staffs = pd.read_csv('data_setup/Data CSV/staffs.csv')
stocks = pd.read_csv('data_setup/Data CSV/stocks.csv')
stores = pd.read_csv('data_setup/Data CSV/stores.csv')

#load the csvs from the "csvs_from_api" folder
customers = pd.read_csv('csvs_from_api/customers.csv')
order_items = pd.read_csv('csvs_from_api/order_items.csv')
orders = pd.read_csv('csvs_from_api/orders.csv')

with engine.begin() as conn:
    conn.execute(text("select 1"))

    brands.to_sql('brands', con=conn, if_exists='replace', index=False)
    #show that the data was loaded
    result = conn.execute(text("SELECT COUNT(*) FROM brands"))
    print("Brands count:", result.scalar())

#general function to load dataframes to sql tables
def load_to_sql(df: pd.DataFrame, table_name:str, primary_key:str=None, foreign_keys:list=None):
    if foreign_keys is None:
        foreign_keys = []
    df = df.drop_duplicates()
    df = df.convert_dtypes().infer_objects()


    with engine.begin() as conn:

        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        if primary_key:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"))
        for i in foreign_keys:
            conn.execute(text(f"ALTER TABLE {table_name} ADD FOREIGN KEY ({i[0]}) REFERENCES {i[1]}({i[1]})"))
        print(f"{table_name} count:", result.scalar())

def load_stores_and_customers(df, table_name, primary_key=None):
    df = df.drop_duplicates(subset=[primary_key])
    df = df.convert_dtypes().infer_objects()
    df['zip_code'] = df['zip_code'].astype(str)


    with engine.begin() as conn:

        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        if primary_key:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"))
        print(f"{table_name} count:", result.scalar())

def load_orders(df, table_name, primary_key=None):
    df = df.drop_duplicates(subset=[primary_key])
    df = df.convert_dtypes().infer_objects()
    # order_id is primary key, make sure there are no duplicates
    df = df.drop_duplicates(subset=['order_id'])
    # convert date columns to datetime
    df['order_date'] = pd.to_datetime(df['order_date'], format='%d/%m/%Y')
    df['required_date'] = pd.to_datetime(df['required_date'], format='%d/%m/%Y')
    df['shipped_date'] = pd.to_datetime(df['shipped_date'], format='%d/%m/%Y', errors='coerce')




    with engine.begin() as conn:


        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        if primary_key:
            conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"))
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        print(f"{table_name} count:", result.scalar())


#load the easy tables first
load_to_sql(categories, 'categories', primary_key='category_id')
load_to_sql(brands, 'brands', primary_key='brand_id')
load_to_sql(products, 'products')
load_to_sql(staffs, 'staffs')
load_to_sql(stocks, 'stocks')
load_to_sql(order_items, 'order_items')

#load the stores and customers with special function
load_stores_and_customers(stores, 'stores')
load_stores_and_customers(customers, 'customers')

#load the orders with special function
load_orders(orders, 'orders')

#sql command to get primary keys of all tables
primary_key_query = """
SELECT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE CONSTRAINT_NAME = 'PRIMARY'
AND TABLE_SCHEMA = 'mydb';
"""

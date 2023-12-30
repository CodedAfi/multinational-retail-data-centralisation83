#%%
from database_utils import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning import DataCleaning 
import pandas as pd
from sqlalchemy import create_engine, inspect
# Retrieve data from tables
db = DatabaseConnector()
de = DataExtractor()
dc = DataCleaning()



local_creds = db.read_db_creds("dbcredpostgres.yaml")
local_engine = create_engine(f"postgresql://{local_creds['RDS_USER']}:{local_creds['RDS_PASSWORD']}@{local_creds['RDS_HOST']}:{local_creds['RDS_PORT']}/{local_creds['RDS_DATABASE']}")
public_creds = db.read_db_creds("db_creds.yaml")
public_engine = create_engine(f"postgresql://{public_creds['RDS_USER']}:{public_creds['RDS_PASSWORD']}@{public_creds['RDS_HOST']}:{public_creds['RDS_PORT']}/{public_creds['RDS_DATABASE']}")
def upload_users():   
    #Retrieve Users Data
    df_users = de.read_rds_table("legacy_users",public_engine)
    df_users.to_csv("raw_user_data.csv")
    #New clean Users Data
    df_clean = dc.clean_user_data(df_users)
    df_clean.to_csv("clean_user_data.csv")
    #Upload Data
    db.upload_to_db(df_clean,"dim_users_table",local_engine)

def upload_orders():
    #Retrieve Order Data
    df_orders = de.read_rds_table("orders_table",public_engine)
    df_orders.to_csv("raw_order_data.csv")
    #New clean Users Data
    df_clean = dc.clean_orders_data(df_orders)
    df_clean.to_csv("clean_order_data.csv")
    #Upload Data
    db.upload_to_db(df_clean,"orders_table",local_engine)

def upload_stores():
    #Retrieve Store Data
    df_stores = de.retrieve_stores_data()
    df_stores.to_csv("raw_store_data.csv")
    #New clean Users Data
    df_clean = dc.clean_store_data(df_stores)
    df_clean.to_csv("clean_store_data.csv")
    #Upload Data
    db.upload_to_db(df_clean,"dim_store_details",local_engine)

def upload_products():
    #Retrieve Product Data
    df_products = de.extract_from_s3("s3://data-handling-public/products.csv")
    df_products.to_csv("raw_product_data.csv")
    #New clean Users Data
    df_clean = dc.clean_products_data(df_products)
    df_clean.to_csv("clean_product_data.csv")
    #Upload Data
    db.upload_to_db(df_clean,"dim_products",local_engine)

def upload_cards():
    #Retrieve Card Data
    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    df_card_details = de.retrieve_pdf_data(url)
    df_card_details.to_csv("raw_card_data.csv")
    #%%clean table
    df_clean = dc.clean_card_details(df_card_details)
    df_clean.to_csv("clean_card_details.csv")
    #%%This uploads the cleaned table to my sales database
    db.upload_to_db(df_clean,'dim_card_details',local_engine)

def upload_dates():
    #Retrieve Dates Data
    df_dates = de.retrieve_date_detail()
    df_dates.to_csv("raw_date_data.csv")
    #New clean Dates Data
    df_clean = dc.clean_date_data(df_dates)
    df_clean.to_csv("clean_data_data.csv")
    #Upload Data
    db.upload_to_db(df_clean,"dim_date_times",local_engine)







if __name__ == "__main__":
    upload_users()
    upload_orders()
    upload_stores()
    #upload_products()
    #upload_cards()
    #upload_dates()

# %%

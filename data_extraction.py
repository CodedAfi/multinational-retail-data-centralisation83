#%%
import pandas as pd
from database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
import requests
class DataExtractor:
    db = DatabaseConnector()
    creds = db.read_db_creds("db_creds.yaml")
    ENGINE = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
    
    
        
from database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
import pandas as pd
import requests
import tabula
import boto3

class DataExtractor:

    def __init__(self):
        # Initialise database connection
        self.db_connector = DatabaseConnector()
        creds = self.db_connector.read_db_creds("db_creds.yaml")
        self.ENGINE = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        self.api_headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}


    def read_rds_table(self,tablename,engine):
        engine = self.ENGINE
        with engine.connect():
            return pd.read_sql(tablename,engine)
        # Read a table from RDS into a pandas DataFrame


    def retrieve_pdf_data(self, pdf_path):
        # Extract data from a PDF file and return as DataFrame
        '''
        extract info from a pdf file to pd.dataframe
        '''
        pdf_info = tabula.read_pdf(pdf_path, pages='all')
        # Convert each list table to a DataFrame
        dataframes = []
        for table in pdf_info:
            dataframe = pd.DataFrame(table)
            dataframes.append(dataframe)
        # Concatenate all DataFrames into a single DataFrame
        combined_dataframe = pd.concat(dataframes)
        return combined_dataframe
    
    
    def list_number_of_store(self):
        # Retrieve the number of stores
        response = requests.get(
            'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', 
            headers=self.api_headers
        )
        return response.json().get('number_stores', 0)

    def retrieve_stores_data(self):
        # Collect and store data
        store_count = self.list_number_of_store()
        store_details = []
        for store_id in range(store_count):
            store_url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_id}'
            details = requests.get(store_url, headers=self.api_headers).json()
            store_details.append(details)
        return pd.json_normalize(store_details)

    
    def extract_from_s3(self,file_path):
        # Download a file from S3 and read it into a DataFrame
        s3_service = boto3.client('s3')
        bucket_name = 'data-handling-public'
        s3_object_key = 'products.csv'
        local_filename = 'products.csv'
        s3_service.download_file(bucket_name, s3_object_key, local_filename)
        return pd.read_csv(local_filename)

    def retrieve_date_detail(self):
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
      
        # Send a GET request to the URL
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to fetch data")

        
        data = response.json()

        # Convert the nested JSON into a DataFrame
        df = pd.DataFrame({col: list(values.values()) for col, values in data.items()})

        return df

if __name__ == "__main__":
    data_extractor = DataExtractor()
# %%

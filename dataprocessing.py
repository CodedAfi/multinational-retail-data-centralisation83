#%%
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self):
        creds = {
            'RDS_HOST': 'data-handling-project.cq2e8zno855e.eu-west-1.rds.amazonaws.com',
            'RDS_PASSWORD': 'AiCore2022',
            'RDS_USER': 'aicore_admin',
            'RDS_DATABASE': 'postgres',
            'RDS_PORT': '5432'
        }
        self.engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/postgres")
    def read_db_creds(self):
        with open("db_creds.yaml") as f:
            return yaml.safe_load(f)

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

# Usage
database_connector = DatabaseConnector()
table_names = database_connector.list_db_tables()
print(table_names)
#%%
class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        import pandas as pd
        from sqlalchemy import text

        # Use the passed db_connector's engine to connect and fetch data
        query = text(f"SELECT * FROM {table_name}")  # Use the table_name dynamically
        with db_connector.engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall())
            df.columns = result.keys()  # Set the DataFrame column names
            return df

# Usage
from database_utils import DatabaseConnector
database_connector = DatabaseConnector()  # Assuming this is correctly initialized
extractor = DataExtractor()

# Get the list of table names and select the appropriate one (e.g., the first one)
table_names = "legacy_users"
df = extractor.read_rds_table(database_connector, table_names)
print(df)
df.to_csv('file_name.csv')
#%%
import pandas as pd

class DataCleaning:
    @staticmethod
    def clean_user_data(df):

        
        # Standardize text columns
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].str.strip().str.lower()

        # Replace 'index1' and 'index2' with the actual names of the unwanted index columns.
        df = df.iloc[:, 2:]

        # Handle NULL values - for illustration, fill with a placeholder
        df.fillna('Unknown', inplace=True)

        # Correct date errors - assuming 'date_of_birth' is the date column
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')

        # Remove rows with NaT in 'date_of_birth' if any
        df = df.dropna(subset=['date_of_birth'])
        

        # Remove duplicates
        df = df.drop_duplicates()
        
        print(df)
        return df
#%%
df = extractor.read_rds_table(database_connector, 'legacy_users')
cleaning = DataCleaning()
cleaned_df = cleaning.clean_user_data(df)
database_connector.upload_to_db(cleaned_df, 'dim_users')
# %%


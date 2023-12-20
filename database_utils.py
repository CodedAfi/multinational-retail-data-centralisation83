#%%
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self):
        creds = self.read_db_creds()
        self.engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")

    def read_db_creds(self):
        with open("db_creds.yaml") as f:
            return yaml.safe_load(f)

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)

# Usage
db_connector = DatabaseConnector()
table_names = db_connector.list_db_tables()
print(table_names)


    
# %%
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
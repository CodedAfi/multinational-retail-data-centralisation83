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
# %%

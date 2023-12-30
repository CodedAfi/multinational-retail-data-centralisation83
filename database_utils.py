#%%
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self):
        creds = self.read_db_creds("db_creds.yaml")
        self.engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")

    def read_db_creds(self,file):
        with open(file) as f:
            return yaml.safe_load(f)

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name,engine):
        df.to_sql(table_name, engine, if_exists='replace', index=False)

# Usage
db_connector = DatabaseConnector()
table_names = db_connector.list_db_tables()
print(table_names)
# %%
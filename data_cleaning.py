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
        return 
    
cleaning = DataCleaning()
df = pd.read_csv("file_name.csv")
cleaned_df = cleaning.clean_user_data(df)


# %%

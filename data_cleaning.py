#%%
import pandas as pd

class DataCleaning:
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhances user data by refining dates, phone numbers, and addresses, and omitting rows with missing date information."""
        df = df.drop(['index'], axis=1, errors='ignore')
        df = self._format_dates(df, ['date_of_birth', 'join_date'])
        df = self._refine_phone_numbers(df, 'phone_number')
        df['address'] = df['address'].replace('\n', ', ', regex=True)
        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhances card data by discarding invalid entries, restructuring card numbers, and validating expiry dates."""
        df = self._discard_misrepresented_rows(df)
        df['card_number'] = df['card_number'].replace(r'\D', '', regex=True)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df = df[df['expiry_date'].str.match(r"^(0[1-9]|1[0-2])\/\d{2}$").fillna(False)]
        return df

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Refines store data by dropping specific columns and adjusting data types."""
        drop_columns = ['message', 'lat', 'index']
        df = df.drop(drop_columns, axis=1, errors='ignore')
        df['address'] = df['address'].replace('\n', ' ', regex=False)
        df['continent'] = df['continent'].replace('^ee', '', regex=True)
        df = self._format_dates(df, ['opening_date'])
        df[['longitude', 'latitude', 'staff_numbers']] = df[['longitude', 'latitude', 'staff_numbers']].apply(pd.to_numeric, errors='coerce')
        return df.dropna(subset=['staff_numbers', 'longitude', 'latitude', 'opening_date'], how='all')

    def convert_product_data(self, x):
        """Converts product weight strings to kilograms."""
        if 'kg' in x:
            x = x.replace('kg', '')
            x = float(x)

        elif 'ml' in x:
            x = x.replace('ml', '')
            x = float(x)/1000

        elif 'g' in x:
            x = x.replace('g', '')
            x = float(x)/1000

        elif 'lb' in x:
            x = x.replace('lb', '')
            x = float(x)*0.453591

        elif 'oz' in x:
            x = x.replace('oz', '')
            x = float(x)*0.0283495
            
        return x

    def clean_products_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Refines product data by restructuring the DataFrame and converting weight values."""
        df = df.drop(columns=[0], errors='ignore')
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df['weight'] = df['weight'].apply(self.convert_product_weights)
        return df.dropna(subset=['date_added'])

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Streamlines orders data by removing certain columns."""
        unwanted_columns = ['level_0', 'index', 'first_name', 'last_name', '1']
        return df.drop(unwanted_columns, axis=1, errors='ignore')

    def clean_date_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimizes date data by converting to numeric types and excluding rows with missing values."""
        for col in ['month', 'year', 'day']:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
        return df.dropna()

    # Helper methods
    def _format_dates(self, df, date_columns):
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def _refine_phone_numbers(self, df, phone_col):
        df[phone_col] = df[phone_col].replace(r'[^0-9+\-()\s]', '', regex=True).str.strip().replace(r'\s+', ' ', regex=True)
        return df

    def _discard_misrepresented_rows(self, df):
        return df.drop(df[df.apply(lambda r: all(r == df.columns), axis=1)].index)
# %%

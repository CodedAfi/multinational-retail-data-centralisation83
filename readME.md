# Centralised Data Management Initiative

This initiative involves setting up a PostgreSQL database locally, integrating data from diverse sources, processing this data, establishing a database schema, and executing various SQL queries.

**Core Technologies Utilised:** PostgreSQL, AWS (S3), boto3, REST API, CSV, Python (Pandas).

## Utility Modules of the Project

1. **Data Ingestion**: "data_extraction.py" contains functions for importing data into pandas data frames from various sources.
2. **Data Purification**: "data_cleaning.py" hosts the DataCleaning class, dedicated to purifying data tables imported through "data_extraction.py".
3. **Data Upload Mechanics**: The "database_utils.py" script includes the DatabaseConnector class. This class initialises the database engine using credentials from a ".yml" configuration file.
4. **Integration and Deployment**: "main.py" is equipped with routines for direct data transfer into the local database.

## Data Processing Workflow

Our data is sourced from 6 distinct origins:

1. **AWS Cloud's Remote PostgreSQL**: The "order_table" is crucial as it carries sales data. Key fields like "date_uuid", "user_uuid", "card_number", "store_code", "product_code", and "product_quantity" need cleaning for Nans and missing values. "product_quantity" must be an integer.
2. **User Data from Remote PostgreSQL**: The "dim_users" table is acquired similarly to the first data source. The crucial field here is "user_uuid".
3. **AWS Cloud Public Link**: "dim_card_details" is in a ".pdf" format and accessed via an S3 link. We utilise "tabula" for PDF processing, focusing on cleaning the card number field.
4. **Data from AWS S3 Bucket**: For the "dim_product" table, we employ boto3 for downloading. Key attention is given to the "product code" (primary key), converting "product_price" to floats, and standardising weights.
5. **REST API Source**: The "dim_store_details" data is fetched using GET requests and transformed from JSON to pandas dataframe format. The "store_code" acts as the primary key.
6. **Direct Link JSON Data**: The "dim_date_times" data is procured similarly to the REST API data, focusing on the "date_uuid" as the primary identifier.

#### Data Cleansing Guidelines

1. Cleaning operations are centered around the "primary key" to avoid foreign key mismatches in the "orders_table".
2. We handle various date formats as shown:
   ```python
   df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
   df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
   df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
   df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
### Typical Data Transformation Steps

2. **Data Type Conversions**:
   ```sql
   ALTER TABLE dim_products
   	ALTER COLUMN product_price TYPE float USING product_price::double precision,
   	ALTER COLUMN weight TYPE float USING weight::double precision,
   	ALTER COLUMN product_code TYPE VARCHAR(255),
   	ALTER COLUMN uuid TYPE uuid using uuid::uuid,
   	ALTER COLUMN still_available Type Bool using still_available::boolean,
   	ALTER COLUMN weight_class Type varchar(50),
   	ALTER COLUMN "EAN" Type varchar(255);

3. **Key Constraints Establishment**:
    ```sql
    ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
    ALTER TABLE orders_table
        ADD FOREIGN KEY(product_code) REFERENCES dim_products(product_code);

3. **Adding Conditional Columns for Data Segmentation**:
    ```sql
    LTER TABLE dim_products
	ADD weight_class VARCHAR(30);
    UPDATE dim_products
        SET weight_class = CASE 
            when weight/1000 < 2 then 'Light'
            when weight/1000 between 2 and 40 then 'Mid_Sized'
            when weight/1000 between 41 and 140 then 'Heavy'
            when weight/1000 > 140 then 'Truck_Required'  
            else 'Invalid' 
        END;

    ALTER TABLE dim_products
        RENAME COLUMN removed TO still_available;

    UPDATE dim_products
        SET still_available = CASE 
            when still_available = 'Still_available' then True
            when still_available = 'Removed' then False
        END;

Through this initiative, we have demonstrated the practical application of tools like Python, Pandas, and PostgreSQL in solving real-world data management challenges. The project underscores the importance of clean, organised, and easily accessible data in driving business intelligence and strategy.

### Significance

- **Data Integrity and Reliability**: Ensuring data is clean, consistent, and well-structured enhances the reliability of business insights derived from it.
- **Efficiency**: Automating the extraction, cleaning, and loading processes reduces manual effort and minimises the risk of errors, leading to increased operational efficiency.
- **Scalability**: The architecture designed in this project is scalable, capable of handling increased data loads and accommodating new data sources with minimal adjustments.
- **Insights and Decision Making**: With centralised data, stakeholders can extract valuable insights more efficiently, leading to more informed and timely business decisions.

## Conclusion 

This project demonstrates a comprehensive approach to data centralisation, leveraging the power of PostgreSQL and Python. It offers a scalable and efficient solution for managing diverse data sets, providing a foundation for advanced data analysis and decision-making.


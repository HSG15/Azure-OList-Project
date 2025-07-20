-- Master key: Used to protect credentials and other secrets.
-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = '#hsg321';

-- Credential hsgadmin: Meant to access your Azure Data Lake Storage using Managed Identity (secure, passwordless access).
-- CREATE DATABASE SCOPED CREDENTIAL hsgadmin WITH IDENTITY = 'Managed Identity';

-- This would list any credentials (like hsgadmin) created in the database.
-- select * from sys.database_credentials;

-- Data Migration to Gold Layer (https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-cetas)



-- You defined a reusable format named extfileformat. It says: the files are in Parquet format and compressed using Snappy.
CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


--This sets up a link to your Azure Data Lake path (/olist-data/gold/). It uses the hsgadmin credential to access it.
CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://hsgolistecommstorage.dfs.core.windows.net/olist-data/gold/',
    CREDENTIAL = hsgadmin
);

/*
This creates a new external table called gold.final.
It:
    Stores the data at the Gold layer location you defined.
    Reads data from a view or table called final_delivered_orders.
    Saves it in Parquet format using the settings above.
*/

CREATE EXTERNAL TABLE gold.finaltable WITH (
    LOCATION = 'finalServing',
    DATA_SOURCE = goldlayer,        
    FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final_delivered_orders;



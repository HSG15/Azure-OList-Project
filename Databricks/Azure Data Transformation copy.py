# Databricks notebook source
print('Hello')

# COMMAND ----------

# Define variables
storage_account = "hsgolistecommstorage"
application_id = "c725c5e1-fc40-4fe3-939b-1388d6da7a82"
directory_id = "26f26383-daa5-4b20-a189-0b91e7bf473d"
client_secret = "wse8Q~V.gsIIubyyBB.bFJA5kEYBItwTYRCchbvn"

# Spark Azure Data Lake Gen2 OAuth Configuration
try:
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "{client_secret}")
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
    print("OAuth configuration set successfully")
except Exception as e:
    print(f'Error while setting up OAuth : {e}')

#Before running make sure your newly created cluster is selected else you might get the error : [CONFIG_NOT_AVAILABLE] Configuration fs.azure.account.auth.type.hsgolistecommstorage.dfs.core.windows.net is not available. SQLSTATE: 42K0I


# COMMAND ----------

df.size

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using ADLS Access key directly - Read Data from ADLS Gen2

# COMMAND ----------

# Set up your credentials and file paths
storage_account_name = "hsgolistecommstorage"
access_key = "Inl11rJUtSEzU1jmqQcylva37Veg660TiKMxqH97p1s4qQ4VKI1FU5Cd+tU5PEcmHr+YgyAe+I8B+AStUJxzXQ=="
container_name = "olist-data"
file_path = "bronze/customers_dataset.csv"

# Configure Spark to access ADLS using the access key
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    access_key
)

# Build the full ABFSS path
abfss_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"

df = spark.read.csv(abfss_path, header=True, inferSchema=True)
#df.display()

# COMMAND ----------

storage_account_name = "hsgolistecommstorage"
container_name = "olist-data"

customers_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/customers_dataset.csv", header=True, inferSchema=True)
geolocation_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/geolocation_dataset.csv", header=True, inferSchema=True)
payments_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/order_items_dataset.csv", header=True, inferSchema=True)
order_reviews_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/order_reviews_dataset.csv", header=True, inferSchema=True)
orders_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/orders_dataset.csv", header=True, inferSchema=True)
products_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/products_dataset.csv", header=True, inferSchema=True)
sellers_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/sellers_dataset.csv", header=True, inferSchema=True)

sellers_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### retrieve the data from MongoDB that has been sent from VS Code
# MAGIC

# COMMAND ----------

#retrieve the data from MongoDB that has been sent from VS Code

#connect to the database
# importing module
!pip install pymongo
from pymongo import MongoClient

hostname = "l41g19.h.filess.io"
database = "olistDatabaseNoSQL_alsoaskjoy"
port = "61004"
username = "olistDatabaseNoSQL_alsoaskjoy"
password = "3427ce2af376e3fd824ce6e1990f9c7a73d9c07e"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]

mydatabase

# COMMAND ----------

import pandas as pd

collection = mydatabase['product_category_translations']
mongo_data = pd.DataFrame(list(collection.find()))
mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning the Data

# COMMAND ----------

from pyspark.sql.functions import *
orders_df.printSchema()

# COMMAND ----------

orders_df.display()

# COMMAND ----------

#Convert Data Columns

orders_df = orders_df.withColumn("order_purchase_timestamp", date_format(to_timestamp(col('order_purchase_timestamp')), 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn('order_delivered_customer_date', date_format(to_timestamp(col('order_delivered_customer_date')), 'yyyy-MM-dd HH:mm:ss'))\
            .withColumn('order_estimated_delivery_date', date_format(to_timestamp(col('order_estimated_delivery_date')), 'yyyy-MM-dd HH:mm:ss'))
orders_df.display()

# COMMAND ----------

#Calculate delivery 
delivery_time_df = orders_df.withColumn('time_taken_to_deliver', date_diff(col('order_delivered_customer_date'), col('order_purchase_timestamp')))
delivery_time_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining some datasets

# COMMAND ----------

orders_customers_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, 'left')
orders_payments_df  = orders_customers_df.join(payments_df, orders_customers_df.order_id == payments_df.order_id, 'left')
orders_items_df     = orders_payments_df.join(order_items_df, 'order_id', 'left')
orders_items_products_df = orders_items_df.join(products_df, orders_items_df.product_id == products_df.product_id, 'left')
final_df = orders_items_products_df.join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id, 'left')

#joining mongo data -> convert pandas_mongo_df(mongo_data) to spark_mongo_df
try:
    mongo_data.drop("_id", axis=1, inplace=True)
except:
    pass

spark_mongo_df = spark.createDataFrame(mongo_data)
final_df = final_df.join(spark_mongo_df, 'product_category_name', 'left')
final_df.count()

# COMMAND ----------

final_df.display()

# COMMAND ----------

# remove duplicate columns in final_df
print(f'brfore deleting duplicates, column count is {len(final_df.columns)}')

def remove_duplicates(df):
    cols = df.columns
    for col in cols:
        if cols.count(col) > 1:
            df = df.drop(col)
    return df
final_df = remove_duplicates(final_df)
print(f'after deleting duplicates, column count is {len(final_df.columns)}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the final data to Silver folder of ADLS Gen2

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/olist_dataset")

# COMMAND ----------


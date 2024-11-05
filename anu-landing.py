# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format
from datetime import datetime

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("IncrementalLoad").getOrCreate()

# COMMAND ----------

# Configuration for Azure Key Vault
kv_scope = "anu-access-scope"
key_vault_name = "de106kv50215"
key_vault_secret_name = "anu-storage-access"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net/secrets/{key_vault_secret_name}"

# COMMAND ----------

# Fetch the secret from Azure Key Vault
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

# Configuration for ADLS Gen2
storage_account = "de10692367dl"
container_name = "anukansha"
source_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/e-mob1/source-data/"
landing_data_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/e-mob1/landing_data/"
print(source_data_path)

# COMMAND ----------

# Set the Spark configuration to use the storage account key
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Read the master and transactional data with proper delimiter and quote handling
master_df = spark.read.csv(f"{source_data_path}Charger_Master_Data_LU_Masked.csv", header=True, inferSchema=True)
ocpp_data_df = spark.read.csv(f"{source_data_path}OCPP_Data_LU - Masked.csv", header=True, inferSchema=True, sep=",", quote='"', escape='"', multiLine=True)

# COMMAND ----------

display(master_df)

# COMMAND ----------


display(ocpp_data_df)

# COMMAND ----------

# Write the master data to the landing folder (non-incremental)
master_df.write.csv(f"{landing_data_path}/master_data", header=True, mode="overwrite")

# COMMAND ----------

# Read the last load time from metadata
last_load_time = spark.sql("SELECT max(last_load_time) FROM anu_metadata_table").collect()[0][0]

# COMMAND ----------

print(last_load_time)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, col, max as spark_max

# COMMAND ----------

# Read new or modified data from the source
incremental_df = ocpp_data_df.filter(col("request_date") > last_load_time)

# COMMAND ----------

display(incremental_df)

# COMMAND ----------

# Define folder structure and file name
current_time = datetime.utcnow().strftime('%Y%m%d %H%M%S')
folder_path = datetime.utcnow().strftime('%Y/%m/%d')
file_name = f"{current_time}.csv"
full_path = f"{landing_data_path}/{folder_path}/{file_name}"
temp_path = f"{landing_data_path}/temp/{current_time}"
metadata_path = f"{landing_data_path}/{folder_path}/metadata/{current_time}"

# COMMAND ----------

print(full_path)

# COMMAND ----------

# Write the incremental data to a temporary directory
if not incremental_df.rdd.isEmpty():
    incremental_df.coalesce(1).write.csv(temp_path, header=True, mode='overwrite', quote='"', escape='"')

    # Rename the file
    files = dbutils.fs.ls(temp_path)
    for file in files:
        if file.name.endswith(".csv"):
            dbutils.fs.mv(file.path, f"{landing_data_path}/{folder_path}/{file_name}")
        else:
            dbutils.fs.mv(file.path, f"{metadata_path}/{file.name}")

    # Remove the temporary directory
    dbutils.fs.rm(temp_path, recurse=True)
    # Update metadata with the latest request_date
    latest_request_date = incremental_df.agg(spark_max("request_date")).collect()[0][0]
    if latest_request_date is not None:
        spark.sql(f"INSERT INTO anu_metadata_table VALUES (TIMESTAMP '{latest_request_date}')")
else:
    print("No new records to process.")

# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp, col
from pyspark.sql import SparkSession

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("MasterDataUpload").getOrCreate()

# COMMAND ----------

# Define the schema for the master Delta table
master_schema = """
OCPP_CHARGE_STATION_ID STRING,
OCPP_CONNECTOR_ID STRING,
CONNECTOR_ID STRING,
SOURCE_EVSE_ID STRING,
COUNTRY_CODE STRING,
SITE_ID STRING,
SITE_NAME STRING,
LOCATION_TYPE STRING,
LOCATION_SUB_TYPE STRING,
TIMEZONE STRING,
LOCATION_STATUS STRING,
OPERATOR STRING,
OWNING_COMPANY STRING,
OCPI_CONNECTOR_ID STRING,
CONNECTOR_TYPE STRING,
POWER_TYPE STRING,
POWER_KW STRING,
VOLTAGE STRING,
AMPERAGE STRING,
MANUFACTURER STRING,
MODEL STRING,
_rescued_data STRING
"""

# COMMAND ----------

# Create the master Delta table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS anu_master_table (
    {master_schema}
) USING DELTA
""")

# COMMAND ----------

kv_scope = "anu-access-scope"
key_vault_secret_name = "anu-storage-access"
storage_account_key = dbutils.secrets.get(scope=kv_scope, key=key_vault_secret_name)

# COMMAND ----------

storage_account = "de10692367dl"
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

# Define the source and target paths for master data
landing_data_path_master = "abfss://anukansha@de10692367dl.dfs.core.windows.net/e-mob1/landing_data/master_data/"
master_table_path = "dbfs:/user/hive/warehouse/anu_master_table"
master_checkpoint_path = f"{master_table_path}/_checkpoints"

# COMMAND ----------

# Read the master data from the landing layer using Auto Loader
df_master = (spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.includeExistingFiles", "true")
             .option("cloudFiles.schemaLocation", f"{master_table_path}/_schema")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(landing_data_path_master)
            )

# COMMAND ----------

display(df_master)

# COMMAND ----------

# Write the master data to the Delta table
query_master = (df_master.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", master_checkpoint_path)
                .start(master_table_path)
               )

# Start the stream with a timeout for testing
query_master.awaitTermination() 

# COMMAND ----------

# Check the schema of the existing Delta table
bronze_df = spark.read.format("delta").load(master_table_path)
bronze_df.printSchema()

# COMMAND ----------

# Check the schema of the incoming data
df_master.printSchema()

# COMMAND ----------

# Verify data in the Delta table
master_bronze_df = spark.read.format("delta").load(master_table_path)
master_bronze_df.show(5)

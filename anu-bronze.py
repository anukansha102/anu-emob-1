# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("CreateBronzeTable").getOrCreate()

# Define the schema for the bronze table
schema = """
id BIGINT,
request_uuid STRING,
request_date TIMESTAMP,
entity_id BIGINT,
service_provider_id BIGINT,
raw_log STRING,
serial_number STRING,
identity_key STRING,
source_file STRING,
ingest_load_timestamp TIMESTAMP,
_rescued_data STRING
"""

# COMMAND ----------

# Create the Delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS anu_bronze_table (
    {schema}
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

# Define custom schema
custom_schema = StructType([
    StructField("id", LongType(), True),
    StructField("request_uuid", StringType(), True),
    StructField("request_date", TimestampType(), True),
    StructField("entity_id", LongType(), True),
    StructField("service_provider_id", LongType(), True),
    StructField("raw_log", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("identity_key", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

# COMMAND ----------

# Define the source and target paths
landing_data_path = "abfss://anukansha@de10692367dl.dfs.core.windows.net/e-mob1/landing_data/"
bronze_adls_path = "abfss://anukansha@de10692367dl.dfs.core.windows.net/e-mob1/bronze_data/"
bronze_table_path = "dbfs:/user/hive/warehouse/anu_bronze_table"
checkpoint_path = f"{bronze_table_path}/_checkpoints"

# COMMAND ----------

# Read the data from the landing layer using Auto Loader with custom schema
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.includeExistingFiles", "true")
      .option("cloudFiles.schemaLocation", f"{bronze_table_path}/_schema")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(custom_schema)
      .option("multiLine", "true")
      .option("quote", '"')
      .option("escape", '"')
      .load(landing_data_path)
      .withColumn("ingest_load_timestamp", current_timestamp())
      .withColumn("source_file", input_file_name())
     )

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.select(
    col("id").cast("long").alias("id"),
    col("request_uuid").cast("string").alias("request_uuid"),
    to_timestamp(col("request_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("request_date"),
    col("entity_id").cast("long").alias("entity_id"),
    col("service_provider_id").cast("long").alias("service_provider_id"),
    col("raw_log").cast("string").alias("raw_log"),
    col("identity_key").cast("string").alias("identity_key"),
    col("serial_number").cast("string").alias("serial_number"),
    col("source_file").cast("string").alias("source_file"),
    col("ingest_load_timestamp").cast("timestamp").alias("ingest_load_timestamp"),
    col("_rescued_data").cast("string").alias("_rescued_data")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# Simplified write operation for debugging
query = (df.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_path)
         .start(bronze_table_path)
        )

# Start the stream with a timeout for testing
query.awaitTermination()

# COMMAND ----------

# Verify data in the Delta table
bronze_df = spark.read.format("delta").table("anu_bronze_table")
bronze_df.show(5)


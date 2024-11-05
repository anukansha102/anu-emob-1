# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("CreateSilverTable").getOrCreate()

# COMMAND ----------

schema = """
SOURCE string,
SOURCE_ID string,
SOURCE_MESSAGE_ID string,
CHARGE_STATION_ID string,
MANUFACTURER string,
MODEL string,
SERIAL_NUMBER string,
FIRMWARE_VERSION string,
COUNTRY_CODE string,
INGEST_LOAD_TIMESTAMP timestamp,
STAGE_LOAD_TIMESTAMP timestamp,
SOURCE_MESSAGE_TIMESTAMP_CREATED timestamp,
SOURCE_MESSAGE_TIMESTAMP_UPDATED timestamp,
CORE_MESSAGE_UNIQUE_ID string,
UNIFIED_TIMESTAMP timestamp,
UNIFIED_TIMESTAMP_SOURCE string,
OCPP_MESSAGE_UNIQUE_ID string,
OCPP_ACTION string,
OCPP_REQUEST_MESSAGE string,
OCPP_RESPONSE_MESSAGE string,
SOURCE_CONNECTOR_ID string,
OCPP_ERROR_CODE string,
VENDOR_ERROR_CODE string,
STATUS string,
PAYLOAD_TIMESTAMP timestamp,
ID_TAG string,
ID_TAG_STATUS string,
TRANSACTION_ID int,
STOP_REASON string,
METER_START float,
METER_STOP float,
CHANGE_AVAILABILITY_TYPE string,
CHANGE_AVAILABILITY_STATUS string,
HEARBEAT_INTERVAL int,
METERVALUE_SOC float,
METERVALUE_CURRENT_OFFERED float,
METERVALUE_CURRENT_IMPORT float,
METERVALUE_POWER_KW float,
METERVALUE_ENERGY_KWH float,
METERVALUE_VOLTAGE float,
METERVALUE_TEMP float,
METERVALUE_RPM float,
SIM_ICCID string,
STATUS_INFO string
"""

# COMMAND ----------

# Path to master and bronze tables
master_table_path = "dbfs:/user/hive/warehouse/anu_master_table"
bronze_table_path = "dbfs:/user/hive/warehouse/anu_bronze_table"

# COMMAND ----------

# Create the Delta table without primary key constraints
spark.sql(f"""
CREATE TABLE IF NOT EXISTS anu_silver_table (
    {schema}
) USING DELTA
""")

# COMMAND ----------

# Load the Delta tables into DataFrames
master_df = spark.read.format("delta").load(master_table_path)
bronze_df = spark.read.format("delta").load(bronze_table_path)

# COMMAND ----------

display(master_df)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat_ws, current_timestamp, when, regexp_extract, from_json, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, IntegerType

# COMMAND ----------

# Define schemas for parsing JSON fields
schema_StatNotif_req = StructType() \
    .add('timestamp', TimestampType()) \
    .add('connectorId', StringType()) \
    .add('vendorId', StringType()) \
    .add('info', StringType()) \
    .add('errorCode', StringType()) \
    .add('status', StringType()) \
    .add('vendorErrorCode', StringType())

# schema_Heartbeat_res = StructType() \
#     .add('currentTime', TimestampType())

schema_StartTrans_req = StructType() \
    .add('connectorId', StringType()) \
    .add('idTag', StringType()) \
    .add('meterStart', StringType()) \
    .add('timestamp', TimestampType())

schema_StartTrans_res = StructType() \
    .add('idTagInfo', StructType().add('status', StringType())) \
    .add('transactionId', StringType())

schema_StopTrans_req = StructType() \
    .add('idTag', StringType()) \
    .add('meterStop', StringType()) \
    .add('timestamp', TimestampType()) \
    .add('transactionId', StringType()) \
    .add('reason', StringType()) \
    .add('transactionData', ArrayType(StringType()))

schema_StopTrans_TransData = StructType() \
    .add('timestamp', TimestampType()) \
    .add('sampledValue', ArrayType(StringType()))

schema_StopTrans_res = StructType() \
    .add('idTagInfo', StructType().add('status', StringType()))

# COMMAND ----------

bronze_df = bronze_df.withColumn("json_str", regexp_extract(col("raw_log"), r'\[(.*)\]@@EOM@@', 1))
# Split the json_str column into its components
bronze_df = bronze_df.withColumn("json_components", split(col("json_str"), ',\"'))


# COMMAND ----------

bronze_df = bronze_df.withColumn("SOURCE_MESSAGE_TIMESTAMP_CREATED", regexp_extract(col("raw_log"), r'(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3})', 1)) \
    .withColumn("SOURCE_MESSAGE_TIMESTAMP_UPDATED", regexp_extract(col("raw_log"), r'(\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3})', 1)) \
    .withColumn("OCPP_MESSAGE_UNIQUE_ID", regexp_extract(col("json_components").getItem(1), r'(\d+)', 1)) \
        .withColumn("OCPP_ACTION", regexp_extract(col("raw_log"), r'\[2,"\d+","(\w+)"', 1)) \
        .withColumn("OCPP_REQUEST_MESSAGE", when(col("json_components").getItem(0) == "2", col("json_str")).otherwise(lit(None))) \
            .withColumn("OCPP_RESPONSE_MESSAGE", when(col("json_components").getItem(0) == "3", col("json_str")).otherwise(lit(None))) \
    .withColumn("SOURCE_CONNECTOR_ID", regexp_extract(col("raw_log"), r'"connectorId":(\d+)', 1))

bronze_df = bronze_df.drop("json_components" , "json_str")

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

schema_Heartbeat_res = StructType() \
    .add('currentTime', TimestampType())

schema_Heartbeat_req = StructType() \
    .add('timestamp', TimestampType())

# COMMAND ----------

# Extract the relevant fields from the JSON columns for request messages
df_Heartbeat_req = bronze_df.filter(col("OCPP_ACTION") == "Heartbeat") \
    .withColumn("OCPP_REQUEST_MESSAGE_JSON", from_json(col("OCPP_REQUEST_MESSAGE"), schema_Heartbeat_req)) \
    .select("*", "OCPP_REQUEST_MESSAGE_JSON.*") \
    .drop("OCPP_REQUEST_MESSAGE_JSON" , "OCPP_RESPONSE_MESSAGE")

# COMMAND ----------

display(df_Heartbeat_req)

# COMMAND ----------

# Extract the currentTime from the OCPP_RESPONSE_MESSAGE column
bronze_df = bronze_df.withColumn("currentTime", regexp_extract(col("OCPP_RESPONSE_MESSAGE"), r'"currentTime":"([^"]+)"', 1))

# Extract the relevant fields from the JSON columns for response messages
df_Heartbeat_res = bronze_df.filter(col("OCPP_RESPONSE_MESSAGE").isNotNull()) \
    .select("OCPP_MESSAGE_UNIQUE_ID", "currentTime" , "OCPP_RESPONSE_MESSAGE")

# COMMAND ----------

display(df_Heartbeat_res)

# COMMAND ----------

# Join the request and response DataFrames to get the correct UNIFIED_TIMESTAMP_SOURCE
df_Heartbeat = df_Heartbeat_req.join(df_Heartbeat_res, "OCPP_MESSAGE_UNIQUE_ID", "left") \
    .withColumn("PAYLOAD_TIMESTAMP", col("currentTime")) \
    .withColumn("UNIFIED_TIMESTAMP_SOURCE", when(col("PAYLOAD_TIMESTAMP").isNotNull(), "payload").otherwise("created")) \
    .drop("currentTime" , "timestamp")

# COMMAND ----------

display(df_Heartbeat)

# COMMAND ----------

# Extract the JSON string from raw_log
bronze_df = bronze_df.withColumn("json_str", regexp_extract(col("raw_log"), r'\[2,"[^"]+","[^"]+",(\{.*\})\]@@EOM@@', 1))

# Parse the JSON string
df_StartTrans = bronze_df \
    .filter(col("OCPP_ACTION") == "StartTransaction") \
    .withColumn("reqjsonData", from_json(col("json_str"), schema_StartTrans_req)) \
    .withColumn("resjsonData", from_json(col("OCPP_RESPONSE_MESSAGE"), schema_StartTrans_res)) \
    .select("*", "reqjsonData.*", "resjsonData.*", "resjsonData.idTagInfo.*") \
    .withColumnRenamed("timestamp", "PAYLOAD_TIMESTAMP") \
    .withColumn("UNIFIED_TIMESTAMP_SOURCE", when(col("PAYLOAD_TIMESTAMP").isNotNull(), "payload").otherwise("created")) \
    .withColumnRenamed("connectorId", "CONNECTOR_ID") \
    .withColumnRenamed("idTag", "ID_TAG") \
    .withColumnRenamed("status", "ID_TAG_STATUS") \
    .withColumnRenamed("meterStart", "METER_START") \
    .drop("reqjsonData", "resjsonData", "OCPP_REQUEST_MESSAGE", "OCPP_RESPONSE_MESSAGE", "idTagInfo", "json_str" , "transactionId" ,"currentTime")


# COMMAND ----------

display(df_StartTrans)

# COMMAND ----------

# Extract the JSON string from raw_log
bronze_df = bronze_df.withColumn("json_str", regexp_extract(col("raw_log"), r'\[2,"[^"]+","[^"]+",(\{.*\})\]@@EOM@@', 1))

# Parse the JSON string
df_StopTrans = bronze_df \
    .filter(col("OCPP_ACTION") == "StopTransaction") \
    .withColumn("reqjsonData", from_json(col("json_str"), schema_StopTrans_req)) \
    .withColumn("resjsonData", from_json(col("OCPP_RESPONSE_MESSAGE"), schema_StopTrans_res)) \
    .select("*", "reqjsonData.*", "resjsonData.*", "resjsonData.idTagInfo.*") \
    .withColumnRenamed("timestamp", "PAYLOAD_TIMESTAMP") \
    .withColumn("UNIFIED_TIMESTAMP_SOURCE", when(col("PAYLOAD_TIMESTAMP").isNotNull(), "payload").otherwise("created")) \
    .withColumnRenamed("idTag", "ID_TAG") \
    .withColumnRenamed("status", "ID_TAG_STATUS") \
    .withColumnRenamed("transactionId", "TRANSACTION_ID") \
    .withColumnRenamed("meterStop", "METER_STOP") \
    .withColumnRenamed("reason", "STOP_REASON") \
    .drop("reqjsonData", "resjsonData", "OCPP_REQUEST_MESSAGE", "OCPP_RESPONSE_MESSAGE", "idTagInfo", "json_str" , "transactionData" , "currentTime")


# COMMAND ----------

display(df_StopTrans)

# COMMAND ----------

# Extract the JSON string from raw_log
bronze_df = bronze_df.withColumn("json_str", regexp_extract(col("raw_log"), r'\[2,"[^"]+","[^"]+",(\{.*\})\]@@EOM@@', 1))

# Parse the JSON string
df_StatusNotif = bronze_df \
    .filter(col("OCPP_ACTION") == "StatusNotification") \
    .withColumn("reqjsonData", from_json(col("json_str"), schema_StatNotif_req)) \
    .select("*", "reqjsonData.*") \
    .withColumnRenamed("timestamp", "PAYLOAD_TIMESTAMP") \
    .withColumn("UNIFIED_TIMESTAMP_SOURCE", when(col("PAYLOAD_TIMESTAMP").isNotNull(), "payload").otherwise("created")) \
    .withColumnRenamed("connectorId", "CONNECTOR_ID") \
    .withColumnRenamed("errorCode", "OCPP_ERROR_CODE") \
    .withColumnRenamed("status", "STATUS") \
    .withColumnRenamed("vendorErrorCode", "VENDOR_ERROR_CODE") \
    .withColumnRenamed("info", "STATUS_INFO") \
    .drop("reqjsonData", "OCPP_REQUEST_MESSAGE", "OCPP_RESPONSE_MESSAGE", "json_str" , "currentTime")


# COMMAND ----------

display(df_StatusNotif)

# COMMAND ----------

filtered_df = df_StatusNotif.filter(df_StatusNotif.VENDOR_ERROR_CODE != "0")
display(filtered_df)

# COMMAND ----------

unique_columns = set(df_Heartbeat.columns).union(set(df_StartTrans.columns), set(df_StopTrans.columns), set(df_StatusNotif.columns))

# COMMAND ----------

print(unique_columns)

# COMMAND ----------

# Function to add missing columns
def add_missing_columns(df, unique_columns):
    for column in unique_columns:
        if column not in df.columns:
            df = df.withColumn(column, lit(None))
    return df

# COMMAND ----------

# Apply the function to each DataFrame
df_Heartbeat = add_missing_columns(df_Heartbeat, unique_columns)
df_StartTrans = add_missing_columns(df_StartTrans, unique_columns)
df_StopTrans = add_missing_columns(df_StopTrans, unique_columns)
df_StatusNotif = add_missing_columns(df_StatusNotif, unique_columns)

# COMMAND ----------

# Combine the DataFrames
combined_df = df_Heartbeat.unionByName(df_StartTrans).unionByName(df_StopTrans).unionByName(df_StatusNotif)

# COMMAND ----------

display(combined_df)

# COMMAND ----------

# It was observed in the data that the timestamp values in columns SOURCE_MESSAGE_TIMESTAMP_CREATED, SOURCE_MESSAGE_TIMESTAMP_UPDATED and PAYLOAD_TIMESTAMP for e.g. 2019-07-01T19:92:33Z , 2019-07-01T21:92:37Z

# 


# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# Assuming your DataFrame is named df
combined_df = combined_df.withColumn("SOURCE_MESSAGE_TIMESTAMP_CREATED", to_timestamp("SOURCE_MESSAGE_TIMESTAMP_CREATED", "dd-MMM-yyyy HH:mm:ss.SSS"))
combined_df = combined_df.withColumn("SOURCE_MESSAGE_TIMESTAMP_UPDATED", to_timestamp("SOURCE_MESSAGE_TIMESTAMP_UPDATED", "dd-MMM-yyyy HH:mm:ss.SSS"))

# COMMAND ----------

silver_df = combined_df.join(master_df, combined_df.identity_key == master_df.OCPP_CHARGE_STATION_ID, "inner") \
    .select(
        lit("OCPP_DRIIVZ").alias("SOURCE"),
        col("id").alias("SOURCE_ID").cast(StringType()),
        concat_ws("_", col("entity_id"), col("request_uuid")).alias("SOURCE_MESSAGE_ID"),
        col("identity_key").alias("CHARGE_STATION_ID"),
        col("MANUFACTURER"),
        col("MODEL"),
        col("serial_number").alias("SERIAL_NUMBER"),
        lit("csb.21.1.3").alias("FIRMWARE_VERSION"),
        lit("99920200000000").alias("SIM_ICCID"),
        col("COUNTRY_CODE"),
        col("SOURCE_MESSAGE_TIMESTAMP_CREATED"),
        col("SOURCE_MESSAGE_TIMESTAMP_UPDATED"),
        concat_ws("_", col("identity_key"), col("SOURCE_CONNECTOR_ID"), col("OCPP_ACTION"), col("SOURCE_MESSAGE_TIMESTAMP_CREATED"), col("OCPP_MESSAGE_UNIQUE_ID")).alias("CORE_MESSAGE_UNIQUE_ID"),
        when(col("PAYLOAD_TIMESTAMP").isNotNull(), col("PAYLOAD_TIMESTAMP")).otherwise(col("SOURCE_MESSAGE_TIMESTAMP_CREATED")).alias("UNIFIED_TIMESTAMP").cast(TimestampType()),
        col("UNIFIED_TIMESTAMP_SOURCE"),
        col("OCPP_MESSAGE_UNIQUE_ID"),
        col("OCPP_ACTION"),
        col("OCPP_REQUEST_MESSAGE"),
        col("OCPP_RESPONSE_MESSAGE"),
        col("SOURCE_CONNECTOR_ID"),
        col("OCPP_ERROR_CODE"),
        col("VENDOR_ERROR_CODE"),
        col("STATUS"),
        col("STATUS_INFO"),
        col("PAYLOAD_TIMESTAMP").cast(TimestampType()),
        col("ID_TAG"),
        col("ID_TAG_STATUS"),
        col("TRANSACTION_ID").cast(IntegerType()),
        col("STOP_REASON"),
        col("METER_START").cast(FloatType()),
        col("METER_STOP").cast(FloatType()),
        lit("Operative").alias("CHANGE_AVAILABILITY_TYPE"),
        lit("Accepted").alias("CHANGE_AVAILABILITY_STATUS"),
        lit(15).alias("HEARBEAT_INTERVAL"),
        lit(81).alias("METERVALUE_SOC").cast(FloatType()),
        lit(200).alias("METERVALUE_CURRENT_OFFERED").cast(FloatType()),
        lit(29.9).alias("METERVALUE_CURRENT_IMPORT").cast(FloatType()),
        lit(11459.2).alias("METERVALUE_POWER_KW").cast(FloatType()),
        lit(43099).alias("METERVALUE_ENERGY_KWH").cast(FloatType()),
        lit(444).alias("METERVALUE_VOLTAGE").cast(FloatType()),
        lit(300.1).alias("METERVALUE_TEMP").cast(FloatType()),
        lit(1410.8).alias("METERVALUE_RPM").cast(FloatType()),
        col("ingest_load_timestamp").alias("INGEST_LOAD_TIMESTAMP"),
        current_timestamp().alias("STAGE_LOAD_TIMESTAMP")
    )

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# Write the transformed data to the silver Delta table in append mode
silver_df.write.format("delta").mode("append").save("dbfs:/user/hive/warehouse/anu_silver_table")

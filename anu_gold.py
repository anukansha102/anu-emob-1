# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("createGoldTable").getOrCreate()

# COMMAND ----------

# Define the paths for the silver and gold tables
silver_table_path = "dbfs:/user/hive/warehouse/anu_silver_table"
gold_table_path = "dbfs:/user/hive/warehouse/anu_gold_table"

# COMMAND ----------

# Load the silver table
silver_df = spark.read.format("delta").load(silver_table_path)

# COMMAND ----------

# Create the gold table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS anu_gold_table (
    SOURCE STRING,
    SOURCE_ID STRING,
    SOURCE_MESSAGE_ID STRING,
    CHARGE_STATION_ID STRING,
    MANUFACTURER STRING,
    MODEL STRING,
    SERIAL_NUMBER STRING,
    FIRMWARE_VERSION STRING,
    COUNTRY_CODE STRING,
    SOURCE_MESSAGE_TIMESTAMP_CREATED TIMESTAMP,
    SOURCE_MESSAGE_TIMESTAMP_UPDATED TIMESTAMP,
    OCPP_MESSAGE_UNIQUE_ID STRING,
    OCPP_ACTION STRING,
    OCPP_REQUEST_MESSAGE STRING,
    OCPP_RESPONSE_MESSAGE STRING,
    SOURCE_CONNECTOR_ID STRING,
    OCPP_ERROR_CODE STRING,
    VENDOR_ERROR_CODE STRING,
    STATUS STRING,
    PAYLOAD_TIMESTAMP TIMESTAMP,
    CORE_MESSAGE_UNIQUE_ID STRING,
    UNIFIED_TIMESTAMP TIMESTAMP,
    UNIFIED_TIMESTAMP_SOURCE STRING,
    ID_TAG STRING,
    ID_TAG_STATUS STRING,
    TRANSACTION_ID INT,
    STOP_REASON STRING,
    METER_START FLOAT,
    METER_STOP FLOAT,
    CHANGE_AVAILABILITY_TYPE STRING,
    CHANGE_AVAILABILITY_STATUS STRING,
    HEARBEAT_INTERVAL INT,
    METERVALUE_SOC FLOAT,
    METERVALUE_CURRENT_OFFERED FLOAT,
    METERVALUE_CURRENT_IMPORT FLOAT,
    METERVALUE_POWER_KW FLOAT,
    METERVALUE_ENERGY_KWH FLOAT,
    METERVALUE_VOLTAGE FLOAT,
    METERVALUE_TEMP FLOAT,
    METERVALUE_RPM FLOAT,
    INGEST_LOAD_TIMESTAMP TIMESTAMP,
    STAGE_LOAD_TIMESTAMP TIMESTAMP,
    STAGE_PROCESSED_TIMESTAMP TIMESTAMP,
    SIM_ICCID STRING,
    STATUS_INFO STRING
) USING DELTA LOCATION '{gold_table_path}'
""")

# COMMAND ----------

# Perform the upsert (merge) operation
spark.sql(f"""
MERGE INTO anu_gold_table AS gold
USING anu_silver_table AS silver
ON gold.SOURCE = silver.SOURCE AND gold.SOURCE_ID = silver.SOURCE_ID
WHEN MATCHED THEN
  UPDATE SET
    gold.SOURCE_MESSAGE_ID = silver.SOURCE_MESSAGE_ID,
    gold.CHARGE_STATION_ID = silver.CHARGE_STATION_ID,
    gold.MANUFACTURER = silver.MANUFACTURER,
    gold.MODEL = silver.MODEL,
    gold.SERIAL_NUMBER = silver.SERIAL_NUMBER,
    gold.FIRMWARE_VERSION = silver.FIRMWARE_VERSION,
    gold.COUNTRY_CODE = silver.COUNTRY_CODE,
    gold.SOURCE_MESSAGE_TIMESTAMP_CREATED = silver.SOURCE_MESSAGE_TIMESTAMP_CREATED,
    gold.SOURCE_MESSAGE_TIMESTAMP_UPDATED = silver.SOURCE_MESSAGE_TIMESTAMP_UPDATED,
    gold.OCPP_MESSAGE_UNIQUE_ID = silver.OCPP_MESSAGE_UNIQUE_ID,
    gold.OCPP_ACTION = silver.OCPP_ACTION,
    gold.OCPP_REQUEST_MESSAGE = silver.OCPP_REQUEST_MESSAGE,
    gold.OCPP_RESPONSE_MESSAGE = silver.OCPP_RESPONSE_MESSAGE,
    gold.SOURCE_CONNECTOR_ID = silver.SOURCE_CONNECTOR_ID,
    gold.OCPP_ERROR_CODE = silver.OCPP_ERROR_CODE,
    gold.VENDOR_ERROR_CODE = silver.VENDOR_ERROR_CODE,
    gold.STATUS = silver.STATUS,
    gold.PAYLOAD_TIMESTAMP = silver.PAYLOAD_TIMESTAMP,
    gold.CORE_MESSAGE_UNIQUE_ID = silver.CORE_MESSAGE_UNIQUE_ID,
    gold.UNIFIED_TIMESTAMP = silver.UNIFIED_TIMESTAMP,
    gold.UNIFIED_TIMESTAMP_SOURCE = silver.UNIFIED_TIMESTAMP_SOURCE,
    gold.ID_TAG = silver.ID_TAG,
    gold.ID_TAG_STATUS = silver.ID_TAG_STATUS,
    gold.TRANSACTION_ID = silver.TRANSACTION_ID,
    gold.STOP_REASON = silver.STOP_REASON,
    gold.METER_START = silver.METER_START,
    gold.METER_STOP = silver.METER_STOP,
    gold.CHANGE_AVAILABILITY_TYPE = silver.CHANGE_AVAILABILITY_TYPE,
    gold.CHANGE_AVAILABILITY_STATUS = silver.CHANGE_AVAILABILITY_STATUS,
    gold.HEARBEAT_INTERVAL = silver.HEARBEAT_INTERVAL,
    gold.METERVALUE_SOC = silver.METERVALUE_SOC,
    gold.METERVALUE_CURRENT_OFFERED = silver.METERVALUE_CURRENT_OFFERED,
    gold.METERVALUE_CURRENT_IMPORT = silver.METERVALUE_CURRENT_IMPORT,
    gold.METERVALUE_POWER_KW = silver.METERVALUE_POWER_KW,
    gold.METERVALUE_ENERGY_KWH = silver.METERVALUE_ENERGY_KWH,
    gold.METERVALUE_VOLTAGE = silver.METERVALUE_VOLTAGE,
    gold.METERVALUE_TEMP = silver.METERVALUE_TEMP,
    gold.METERVALUE_RPM = silver.METERVALUE_RPM,
    gold.INGEST_LOAD_TIMESTAMP = silver.INGEST_LOAD_TIMESTAMP,
    gold.STAGE_LOAD_TIMESTAMP = silver.STAGE_LOAD_TIMESTAMP,
    gold.STAGE_PROCESSED_TIMESTAMP = current_timestamp(), 
    gold.SIM_ICCID = silver.SIM_ICCID,
    gold.STATUS_INFO = silver.STATUS_INFO
WHEN NOT MATCHED THEN
  INSERT (
    SOURCE, SOURCE_ID, SOURCE_MESSAGE_ID, CHARGE_STATION_ID, MANUFACTURER, MODEL, SERIAL_NUMBER, FIRMWARE_VERSION, COUNTRY_CODE, 
    SOURCE_MESSAGE_TIMESTAMP_CREATED, SOURCE_MESSAGE_TIMESTAMP_UPDATED, OCPP_MESSAGE_UNIQUE_ID, OCPP_ACTION, OCPP_REQUEST_MESSAGE, 
    OCPP_RESPONSE_MESSAGE, SOURCE_CONNECTOR_ID, OCPP_ERROR_CODE, VENDOR_ERROR_CODE, STATUS, PAYLOAD_TIMESTAMP, CORE_MESSAGE_UNIQUE_ID, 
    UNIFIED_TIMESTAMP, UNIFIED_TIMESTAMP_SOURCE, ID_TAG, ID_TAG_STATUS, TRANSACTION_ID, STOP_REASON, METER_START, METER_STOP, 
    CHANGE_AVAILABILITY_TYPE, CHANGE_AVAILABILITY_STATUS, HEARBEAT_INTERVAL, METERVALUE_SOC, METERVALUE_CURRENT_OFFERED, 
    METERVALUE_CURRENT_IMPORT, METERVALUE_POWER_KW, METERVALUE_ENERGY_KWH, METERVALUE_VOLTAGE, METERVALUE_TEMP, METERVALUE_RPM, 
    INGEST_LOAD_TIMESTAMP, STAGE_LOAD_TIMESTAMP, STAGE_PROCESSED_TIMESTAMP, SIM_ICCID, STATUS_INFO
  ) VALUES (
    silver.SOURCE, silver.SOURCE_ID, silver.SOURCE_MESSAGE_ID, silver.CHARGE_STATION_ID, silver.MANUFACTURER, silver.MODEL, 
    silver.SERIAL_NUMBER, silver.FIRMWARE_VERSION, silver.COUNTRY_CODE, silver.SOURCE_MESSAGE_TIMESTAMP_CREATED, 
    silver.SOURCE_MESSAGE_TIMESTAMP_UPDATED, silver.OCPP_MESSAGE_UNIQUE_ID, silver.OCPP_ACTION, silver.OCPP_REQUEST_MESSAGE, 
    silver.OCPP_RESPONSE_MESSAGE, silver.SOURCE_CONNECTOR_ID, silver.OCPP_ERROR_CODE, silver.VENDOR_ERROR_CODE, silver.STATUS, 
    silver.PAYLOAD_TIMESTAMP, silver.CORE_MESSAGE_UNIQUE_ID, silver.UNIFIED_TIMESTAMP, silver.UNIFIED_TIMESTAMP_SOURCE, silver.ID_TAG, 
    silver.ID_TAG_STATUS, silver.TRANSACTION_ID, silver.STOP_REASON, silver.METER_START, silver.METER_STOP, silver.CHANGE_AVAILABILITY_TYPE, 
    silver.CHANGE_AVAILABILITY_STATUS, silver.HEARBEAT_INTERVAL, silver.METERVALUE_SOC, silver.METERVALUE_CURRENT_OFFERED, 
    silver.METERVALUE_CURRENT_IMPORT, silver.METERVALUE_POWER_KW, silver.METERVALUE_ENERGY_KWH, silver.METERVALUE_VOLTAGE, 
    silver.METERVALUE_TEMP, silver.METERVALUE_RPM, silver.INGEST_LOAD_TIMESTAMP, silver.STAGE_LOAD_TIMESTAMP, current_timestamp(), 
    silver.SIM_ICCID, silver.STATUS_INFO
  )
""")
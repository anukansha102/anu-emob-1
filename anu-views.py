# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("createViews").getOrCreate()

# COMMAND ----------

gold_table_path = "dbfs:/user/hive/warehouse/anu_gold_table"

# COMMAND ----------

# 1. Heartbeats of Chargers on a Given Date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW charger_heartbeats AS
# MAGIC SELECT
# MAGIC     CHARGE_STATION_ID,
# MAGIC     DATE(UNIFIED_TIMESTAMP) AS date,
# MAGIC     COUNT(*) AS heartbeat_count,
# MAGIC     MAX(CASE WHEN STATUS = 'Down' THEN 1 ELSE 0 END) AS was_down
# MAGIC FROM anu_gold_table
# MAGIC WHERE OCPP_ACTION = 'Heartbeat'
# MAGIC GROUP BY CHARGE_STATION_ID, DATE(UNIFIED_TIMESTAMP);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM charger_heartbeats;

# COMMAND ----------

# 2. Errors in Charging Sessions for Various Vendors

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW charging_session_errors AS
# MAGIC SELECT
# MAGIC     MANUFACTURER,
# MAGIC     COUNT(*) AS error_count,
# MAGIC     OCPP_ERROR_CODE,
# MAGIC     VENDOR_ERROR_CODE
# MAGIC FROM anu_gold_table
# MAGIC WHERE WHERE OCPP_ACTION = 'StatusNotification' AND (OCPP_ERROR_CODE = 'Error' OR VENDOR_ERROR_CODE != '0')
# MAGIC GROUP BY MANUFACTURER, OCPP_ERROR_CODE, VENDOR_ERROR_CODE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM charging_session_errors;

# COMMAND ----------

# 3. Meter Start and End Reading with Units Consumed for Each EV Transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ev_transaction_units AS
# MAGIC WITH start_trans AS (
# MAGIC     SELECT
# MAGIC         ID_TAG,
# MAGIC         CHARGE_STATION_ID,
# MAGIC         METER_START,
# MAGIC         UNIFIED_TIMESTAMP AS start_time
# MAGIC     FROM anu_gold_table
# MAGIC     WHERE OCPP_ACTION = 'StartTransaction'
# MAGIC ),
# MAGIC stop_trans AS (
# MAGIC     SELECT
# MAGIC         ID_TAG,
# MAGIC         CHARGE_STATION_ID,
# MAGIC         METER_STOP,
# MAGIC         UNIFIED_TIMESTAMP AS stop_time
# MAGIC     FROM anu_gold_table
# MAGIC     WHERE OCPP_ACTION = 'StopTransaction'
# MAGIC )
# MAGIC SELECT
# MAGIC     start_trans.ID_TAG,
# MAGIC     start_trans.CHARGE_STATION_ID,
# MAGIC     start_trans.METER_START,
# MAGIC     stop_trans.METER_STOP,
# MAGIC     (stop_trans.METER_STOP - start_trans.METER_START) AS units_consumed,
# MAGIC     start_trans.start_time,
# MAGIC     stop_trans.stop_time
# MAGIC FROM start_trans
# MAGIC JOIN stop_trans
# MAGIC ON start_trans.ID_TAG = stop_trans.ID_TAG
# MAGIC AND start_trans.CHARGE_STATION_ID = stop_trans.CHARGE_STATION_ID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ev_transaction_units;

# COMMAND ----------

# 4. Charging Sessions Duration

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW charging_session_duration AS
# MAGIC SELECT
# MAGIC     TRANSACTION_ID,
# MAGIC     CHARGE_STATION_ID,
# MAGIC     MIN(UNIFIED_TIMESTAMP) AS session_start,
# MAGIC     MAX(UNIFIED_TIMESTAMP) AS session_end,
# MAGIC     (MAX(UNIFIED_TIMESTAMP) - MIN(UNIFIED_TIMESTAMP)) AS session_duration
# MAGIC FROM anu_gold_table
# MAGIC WHERE TRANSACTION_ID IS NOT NULL
# MAGIC GROUP BY TRANSACTION_ID, CHARGE_STATION_ID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM charging_session_duration;

# COMMAND ----------

#  4. Energy Consumption by Charger

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW energy_consumption_by_charger AS
# MAGIC SELECT
# MAGIC     CHARGE_STATION_ID,
# MAGIC     SUM(METERVALUE_ENERGY_KWH) AS total_energy_consumed
# MAGIC FROM anu_gold_table
# MAGIC GROUP BY CHARGE_STATION_ID;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM energy_consumption_by_charger;

# COMMAND ----------

#5. daily_energy_consumption

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW daily_energy_consumption AS
# MAGIC SELECT
# MAGIC     CHARGE_STATION_ID,
# MAGIC     DATE(UNIFIED_TIMESTAMP) AS date,
# MAGIC     SUM(METERVALUE_ENERGY_KWH) AS total_energy_consumed
# MAGIC FROM anu_gold_table
# MAGIC GROUP BY CHARGE_STATION_ID, DATE(UNIFIED_TIMESTAMP);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_energy_consumption;

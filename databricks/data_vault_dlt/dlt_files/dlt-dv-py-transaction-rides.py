# Databricks notebook source
# MAGIC %md
# MAGIC # Link & Satellite [Rides]
# MAGIC
# MAGIC
# MAGIC ### Link
# MAGIC * lnk_rides_str

# COMMAND ----------

# DBTITLE 1,dlt
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,lnk_rides_str
@dlt.table(
    name = "lnk_rides_str",
    table_properties={"quality": "silver"}
)
def fnc_lnk_rides():
    
    # rides = dlt.readStream("raw_vw_py_mongodb_rides")
    # hub_users = dlt.readStream("hub_users")
    # hub_vehicles = dlt.readStream("hub_vehicles")
    # hub_payments = dlt.readStream("hub_payments")

    rides = spark.readStream.option("skipChangeCommits", "true").table("LIVE.raw_vw_py_mongodb_rides")
    hub_users = spark.readStream.option("skipChangeCommits", "true").table("LIVE.hub_users")
    hub_vehicles = spark.readStream.option("skipChangeCommits", "true").table("LIVE.hub_vehicles")
    hub_payments = spark.readStream.option("skipChangeCommits", "true").table("LIVE.hub_payments")

    df_join_rides = rides.join(hub_users, rides["cpf"] == hub_users["cpf"], "inner") \
    .join(hub_vehicles, rides["vehicle_id"] == hub_vehicles["id"], "inner") \
    .join(hub_payments, rides["txn_id"] == hub_payments["txn_id"], "inner")

    lnk_rides = df_join_rides.select(
        sha1(concat(col("hub_users.hk_cpf"), col("hub_vehicles.hk_vehicle_id"), col("hub_payments.hk_txn_id"))).alias("lnk_rides_id"),
        col("hub_users.hk_cpf").alias("hk_cpf"),
        col("hub_vehicles.hk_vehicle_id").alias("hk_vehicle_id"),
        col("hub_payments.hk_txn_id").alias("hk_txn_id"),
        col("raw_vw_py_mongodb_rides.id").alias("id"),
        col("hub_users.cpf").alias("cpf"),
        col("hub_vehicles.id").alias("vehicle_id"),
        col("hub_payments.txn_id").alias("txn_id"),
        lit(current_timestamp()).alias("load_ts"),
        lit("mongodb").alias("source")
    )
    
    return lnk_rides
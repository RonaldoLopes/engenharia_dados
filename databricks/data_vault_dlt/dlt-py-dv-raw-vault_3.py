# Databricks notebook source
# MAGIC %md
# MAGIC ## Raw vault Python<VIEWS>
# MAGIC * raw_vw_py_mssql_users
# MAGIC * raw_vw_py_mongodb_users
# MAGIC * raw_vw_py_postgres_vehicle
# MAGIC * raw_vw_py_postgres_payments
# MAGIC * raw_vw_py_postgres_subscription
# MAGIC * raw_vw_py_mongodb_ride

# COMMAND ----------

# DBTITLE 1,dlt
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,raw_vw_py_mssql_user
@dlt.view()
def raw_vw_py_mssql_user():
    return(
        dlt.readStream("bronze_mssql_users").select(
            sha1(upper(trim(col("cpf")))).alias("hk_cpf"),
            lit(current_timestamp()).alias("load_ts"),
            lit("mssql").alias("source"),
            col("user_id").alias("user_id"),
            col("uuid").alias("uuid"),
            col("cpf").alias("cpf"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("name"),
            col("date_birth").cast(DateType()).alias("date_birth"),
            col("phone_number").alias("phone_number"),
            col("company_name").alias("company_name"),
            col("job").alias("job"),
            col("city").alias("city"),
            col("country").alias("country")
        )
    )

# COMMAND ----------

# DBTITLE 1,raw_vw_py_mongodb_users
@dlt.view()
def raw_vw_py_mongodb_users():
    return(
        dlt.readStream("bronze_mongodb_users").select(
            sha1(upper(trim(col("cpf")))).alias("hk_cpf"),
            lit(current_timestamp()).alias("load_ts"),
            lit("mongodb").alias("source"),
            col("user_id").alias("user_id"),
            col("uid").alias("uid"),
            col("cpf").alias("cpf"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("name"),
            col("date_of_birth").cast(DateType()).alias("date_birth"),
            col("email").alias("email"),
            col("employment.title").alias("title"),
            col("employment.key_skill").alias("key_skill"),
            col("address.city").alias("city"),
            col("address.country").alias("country"),
            col("address.state").alias("state"),
            col("address.zip_code").alias("zip_code"),
            col("phone_number").alias("phone_number"),
            col("social_insurance_number").alias("social_insurance_number"),
            col("credit_card.cc_number").alias("cc_number"),
            col("subscription.payment_method").alias("payment_method"),
        )
    )

# COMMAND ----------

# DBTITLE 1,raw_vw_py_postgres_vehicle
@dlt.view()
def raw_vw_py_postgres_vehicle():
    return(
        dlt.readStream("bronze_postgres_vehicle").select(
            sha1(upper(trim(col("id")))).alias("hk_vehicle_id"),
            lit(current_timestamp()).alias("load_ts"),
            lit("postgres").alias("source"),
            col("id").alias("id"),
            col("name").alias("name"),
            col("engine").alias("engine"),
            col("max_power").alias("max_power"),
            col("torque").alias("torque"),
            col("mileage").alias("mileage"),
            col("fuel").alias("fuel"),
            col("km_driven").alias("km_driven"),
            col("transmission").alias("transmission"),
            col("seats").alias("seats"),
            col("seller_type").alias("seller_type"),
            col("year").alias("year"),
            expr(
                "CASE WHEN seats BETWEEN 1 AND 4 THEN 'Compact' " + "WHEN seats BETWEEN 5 AND 6 THEN 'Standard' WHEN seats BETWEEN 7 AND 9 THEN 'Multi-Purpose' WHEN seats BETWEEN 10 AND 14 THEN 'Large Group'" + "ELSE 'Not-Classified' END"
            ).alias("drive_type_desc"),
            expr(
                "CASE WHEN year IN ('2020', '2021', '2022') THEN 'Current' " + "WHEN year IN('2016', '2017', '2018', '2019') THEN 'Recent' WHEN year IN('2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015') THEN 'Mid-Age' WHEN year < 2002 THEN 'Old'" + "ELSE 'Not-Classified' END"
            ).alias("car_classification")
        )
    )

# COMMAND ----------

# DBTITLE 1,raw_vw_py_postgres_payments
@dt.view()
def raw_vw_py_postgres_payments():
    return (
        dt.readStream("bronze_postgres_payments").select(
            sha1(upper(trim(col("txn_id")))).alias("hk_txn_id"),
            lit(current_timestamp()).alias("load_ts"),
            lit("postgres").alias("source"),
            col("txn_id").alias("txn_id"),
            col("time").alias("txn_time"),
            col("user_id").alias("user_id"),
            col("subscription_id").alias("subscription_id"),
            when(col("gender") == "F", "Female").when(col("gender") == "M", "Male").otherwise("Unknown").alias("gender"),
            col("job_title").alias("job_title"),
            col("credit_card_type").alias("credit_card_type"),
            col("currency").alias("currency"),
            col("currency_mode").alias("currency_mode"),
            col("language").alias("language"),
            regexp_replace(col("price"), "[\\$,]", "").cast("float").alias("price"),
            col("city").alias("city"),
            col("country").alias("country")
        )
    )

# COMMAND ----------

# DBTITLE 1,raw_vw_py_postgres_subscription
@dt.view()
def raw_vw_py_postgres_subscription():
    return (
        dt.readStream("bronze_postgres_subscription").select(
            sha1(upper(trim(col("id")))).alias("hk_subscription_id"),
            lit(current_timestamp()).alias("load_ts"),
            lit("postgres").alias("source"),
            col("id").alias("id"),
            col("uid").alias("uid"),
            col("user_id").alias("user_id"),
            col("payment_method").alias("method"),
            col("payment_term").alias("term"),
            col("plan").alias("plan"),
            col("status").alias("status"),
            col("subscription_term").alias("frequency"),
            when(col("plan").isin("Business", "Diamond", "Gold", "Platinum", "Premium", "High")).when(col("plan").isin("Bronze", "Essential", "Professional", "Silver", "Standard"), "Normal").otherwise("Low").alias("importance"),
            when(col("plan") == "Basic", 6.00)
            .when(col("plan") == "Bronze", 8.00)
            .when(col("plan") == "Business", 10.00)
            .when(col("plan") == "Diamond", 14.00)
            .when(col("plan") == "Essential", 9.00)
            .when(col("plan") == "Free Trial", 0.00)
            .when(col("plan") == "Gold", 25.00)
            .when(col("plan") == "Platinum", 9.00)
            .when(col("plan") == "Premium", 13.00)
            .when(col("plan") == "Professional", 17.00)
            .when(col("plan") == "Silver", 11.00)
            .when(col("plan") == "Standard", 13.00)
            .when(col("plan") == "Starter", 5.00)
            .when(col("plan") == "Student", 2.00)
            .otherwise(0.00).alias("price")
        )
    )

# COMMAND ----------

# DBTITLE 1,raw_vw_py_mongodb_rides
@dt.view()
def raw_vw_py_mongodb_rides():
    mls_kms = 1.60934
    usd_brl_rate = 5.0
    
    return (
        dt.readStream("bronze_mongodb_rides").select(
            lit(current_timestamp()).alias("load_ts"),
            lit("mongodb").alias("source"),
            col("id").alias("id"),
            col("user_id").alias("user_id"),
            col("cpf").alias("cpf"),
            col("vehicle_id").alias("vehicle_id"),
            col("txn_id").alias("txn_id"),
            col("subscription_id").alias("subscription_id"),
            col("product_id").alias("product_id"),
            col("source").alias("loc_source"),
            col("destination").alias("loc_destination"),
            col("distance").alias("distance_miles"),
            (col("distance") * mls_kms).alias("distance_km"),
            col("name").alias("name"),
            col("cab_type").alias("cab_type"),
            col("price").alias("price_usd"),
            (col("price") * usd_brl_rate).alias("price_brl"),
            col("surge_multiplier").alias("dynamic_fare"),
            col("time_stamp").alias("time_stamp")
        )
    )

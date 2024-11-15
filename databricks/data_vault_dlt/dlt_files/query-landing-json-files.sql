-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query Landing JSON Files

-- COMMAND ----------

-- DBTITLE 1,mssql_users
select * 
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/mssql/users/2024_04_25_14_06_07.json', format => 'json') 
limit 10

-- COMMAND ----------

-- DBTITLE 1,mongodb_users
select * 
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/users/2024_04_25_14_06_07.json', format => 'json') 
limit 10

-- COMMAND ----------

-- DBTITLE 1,postgres_vehicle
select * 
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/vehicle/2024_04_25_14_06_07.json', format => 'json') 
limit 10

-- COMMAND ----------

-- DBTITLE 1,postgres_payments
select * 
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/payments/2024_04_25_14_06_07.json', format => 'json') 
limit 10

-- COMMAND ----------

-- DBTITLE 1,postgres_subscription
select * 
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/subscription/2024_04_25_14_06_07.json', format => 'json') 
limit 10

-- COMMAND ----------

-- DBTITLE 1,mongodb_rides
select *
from read_files('dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/rides/2024_04_25_14_06_07.json', format => 'json')
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Validate DataSets
-- MAGIC
-- MAGIC * mssql_users
-- MAGIC * mongodb_users
-- MAGIC * mongodb_rides
-- MAGIC * postgres_payments
-- MAGIC * postgres_subscription
-- MAGIC * postgres_vehicle

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC mssql_users = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/mssql/users/*.json")
-- MAGIC mongodb_users = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/users/*.json")
-- MAGIC mongodb_rides = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/rides/*.json")
-- MAGIC postgres_payments = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/payments/*.json")
-- MAGIC postgres_subscription = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/subscription/*.json")
-- MAGIC postgres_vehicle = spark.read.json("dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/vehicle/*.json")
-- MAGIC
-- MAGIC mssql_users.createOrReplaceTempView("mssql_users")
-- MAGIC mongodb_users.createOrReplaceTempView("mongodb_users")
-- MAGIC mongodb_rides.createOrReplaceTempView("mongodb_rides")
-- MAGIC postgres_payments.createOrReplaceTempView("postgres_payments")
-- MAGIC postgres_subscription.createOrReplaceTempView("postgres_subscription")
-- MAGIC postgres_vehicle.createOrReplaceTempView("postgres_vehicle")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DLT Pipeline

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dbutils.fs.ls("dbfs:/pipelines/"))
-- MAGIC display(dbutils.fs.ls("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/tables"))
-- MAGIC display(dbutils.fs.ls("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/autoloader"))
-- MAGIC display(dbutils.fs.ls("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/checkpoints"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.rm("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/tables", True))
-- MAGIC display(dbutils.fs.rm("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/autoloader", True))
-- MAGIC display(dbutils.fs.rm("dbfs:/pipelines/db964f6c-2ec7-41fa-b370-a1817abee7fa/checkpoints", True))

-- COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Blob Storage [ADLS2]

# COMMAND ----------

# DBTITLE 1,owshq-stg-files
dbutils.fs.mount(
  source = "wasbs://owshq-stg-files@owshqblobstg.blob.core.windows.net",
  mount_point = "/mnt/owshq-stg-files",
  extra_configs = {"fs.azure.account.key.owshqblobstg.blob.core.windows.net":dbutils.secrets.get(scope = "az-blob-storage-owshqblobstg", key = "key-owshqblobstg")}
)

# COMMAND ----------

# DBTITLE 1,com.owshq.data
display(dbutils.fs.ls("/mnt/owshq-stg-files/"))
display(dbutils.fs.ls("/mnt/owshq-stg-files/com.owshq.data/"))
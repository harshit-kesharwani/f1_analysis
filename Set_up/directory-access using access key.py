# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('try-2')

# COMMAND ----------

access_key=dbutils.secrets.get(scope="try-2",key="aacount-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.udemylearningdb.dfs.core.windows.net",access_key)

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv'))


# COMMAND ----------

display(spark.read.csv("abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



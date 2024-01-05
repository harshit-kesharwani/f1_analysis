# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.udemylearningdb.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.udemylearningdb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.udemylearningdb.dfs.core.windows.net","sp=rl&st=2023-08-01T09:05:43Z&se=2023-08-01T17:05:43Z&spr=https&sv=2022-11-02&sr=c&sig=SbDdXda3Tey6VSDE1tJq%2BE%2Bb1SM8VXv%2F0UcBIChFGuI%3D")


# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv'))


# COMMAND ----------

display(spark.read.csv("abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



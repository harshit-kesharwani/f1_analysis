# Databricks notebook source
display(dbutils.fs.ls('abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv'))


# COMMAND ----------

display(spark.read.csv("abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



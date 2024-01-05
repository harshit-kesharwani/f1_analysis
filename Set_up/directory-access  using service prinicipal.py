# Databricks notebook source
client_id = "4b50da71-4cc6-4a3a-b3b5-3564836b6529"
tenant_id = "89459345-88e6-434f-a103-3498a85ef976"
client_secret = "8Bx8Q~OPPHXjTe4_L3Ch_QST8wwMEd1Imt4O4a6F"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.udemylearningdb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.udemylearningdb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.udemylearningdb.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.udemylearningdb.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.udemylearningdb.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv'))


# COMMAND ----------

display(spark.read.csv("abfss://demo@udemylearningdb.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



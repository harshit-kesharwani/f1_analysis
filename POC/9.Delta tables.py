# Databricks notebook source
df=spark.read.format('parquet').load('dbfs:/mnt/finaldatabricks/presentation/race_results')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy('race_year').saveAsTable("demp_delta.race_results")

# COMMAND ----------


spark.sql("update  demp_delta.race_results set points =20- position")

# COMMAND ----------

# MAGIC %sql 
# MAGIC use demp_delta;
# MAGIC select * from race_results

# COMMAND ----------

spark.sql("delete from demp_delta.race_results where position>10 or position is  null ")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demp_delta.race_results where position = null 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS demp_delta.race_results

# COMMAND ----------



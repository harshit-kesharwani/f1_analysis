# Databricks notebook source
df=spark.read.format('parquet').load('dbfs:/mnt/finaldatabricks/presentation/race_results')

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema if exists dempdelta cascade;
# MAGIC create schema dempdelta 
# MAGIC location 'dbfs:/mnt/finaldatabricks/dempdelta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE dempdelta;

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.select("race_year","race_date","circuit_location","team","grid","race_id").filter("race_id==862")

# COMMAND ----------

df1.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dempdelta.t1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dempdelta.t1

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

df2=df.select("race_year","race_date",upper("circuit_location").alias("circuit_location"),"team","grid", "race_id").filter("race_id==862")

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.mode("overwrite").format("delta").option("overwriteschema",True).saveAsTable("dempdelta.t2")

# COMMAND ----------

spark.sql(""" 
merge into dempdelta.t1 a using dempdelta.t2  b on a.grid= b.grid and a.race_id=b.race_id
when MATCHED THEN 
update set a.circuit_location =b.circuit_location
when not matched then
insert(race_year,race_date, circuit_location, team,grid, race_id)values(race_year,race_date, circuit_location, team,grid, race_id)""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dempdelta.t1 

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy('race_year').saveAsTable("dempdelta.race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC Delta file format is also same as the parquet format, the only difference the two is that delta format stores the log files which allowss us to visit the history of the table.  In backend delta stores the file in the parquet along with logs in the json format. The snapshots of previous is stored. although logs can checked like when and what type of modificaiton has been performed can be checked for lifetime.

# COMMAND ----------


spark.sql("update  dempdelta.race_results set points =20- position")

# COMMAND ----------

# MAGIC %sql 
# MAGIC use dempdelta;
# MAGIC select * from race_results

# COMMAND ----------

spark.sql("delete from dempdelta.race_results where position>10 or position is  null ")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dempdelta.race_results where position is not  null 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --using the below sql statement we can check how the table has been partitioned 
# MAGIC SHOW PARTITIONS dempdelta.race_results

# COMMAND ----------

# MAGIC %sql 
# MAGIC --this command is used to check the historical logs of the delta tables when the table was modified.
# MAGIC desc History dempdelta.t1

# COMMAND ----------

# MAGIC %sql 
# MAGIC --we can check the data present in any of the version of the table using the below command.
# MAGIC select * from  dempdelta.t1 version as of 0

# COMMAND ----------

#we can also create the dataframe from the previous version of the table.
df2=spark.sql("""select * from demp_delta.t1 version as of 0""")

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  VACUUM demp_delta.t1
# MAGIC  --This command is use to delete the history of the table greater than 7 days

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  VACUUM demp_delta.t1 RETAIN 0 HOURS

# COMMAND ----------

spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------



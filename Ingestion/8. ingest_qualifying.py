# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,lit,to_timestamp

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name file_date which will take the value of the file_date paramter as a variable. 

# COMMAND ----------

df_schema="qualifyId int, raceId int, driverId int, constructorId int, number int, q1 string, q2 string, q3 string"

# COMMAND ----------

#Here we are reading multiple multiline json files which are split, so we have given the folder path of mutline json and given the paramter multiline as true
df_qualify =spark.read.schema(df_schema).json(f"/mnt/finaldatabricks/raw/{file_date}/qualifying", multiLine=True)

# COMMAND ----------

display(df_qualify)

# COMMAND ----------

final_df=df_qualify.withColumnsRenamed({"qualifyId":'qaulify_id','raceId':'race_id','driverId':'driver_id','constructorId':'constructor_id'}).withColumn("ingestion_date",current_timestamp()).withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Without incrimental load 
#final_df.write.parquet("/mnt/finaldatabricks/processed/qualifying",'overwrite')
#With hive meta store table 
#final_df.write.format("parquet").mode("overwrite").saveAsTable("processed.qualifying")


# COMMAND ----------

#Full load + inrimental writting
write_data(final_df,"processed","qualifying",'race_id')

# COMMAND ----------

display(spark.read.parquet("/mnt/finaldatabricks/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")

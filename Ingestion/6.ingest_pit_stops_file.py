# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,lit,to_timestamp

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name file_date which will take the value of the file_date paramter as a variable. 

# COMMAND ----------

df_schema="driverId int,duration string,lap int,milliseconds int, stop string, time string,raceId int"

# COMMAND ----------

#df_pit =spark.read.schema(df_schema).option("multiLine",True).json("/mnt/finaldatabricks/raw/pit_stops.json") This is same as the perfomed below, When we are reading the multiline json, we need to specify that it is a multiline json.
df_pit =spark.read.schema(df_schema).json(f"/mnt/finaldatabricks/raw/{file_date}/pit_stops.json", multiLine=True)

# COMMAND ----------

display(df_pit)

# COMMAND ----------

final_df=df_pit.withColumnsRenamed({"driverId": 'driver_id',"raceId":"race_id"}).withColumn("ingestion_date",current_timestamp()).withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

#This is the writting methodology, without incrimental
# #final_df.write.parquet("/mnt/finaldatabricks/processed/pit_stops",'overwrite')
# final_df.write.format("parquet").mode("overwrite").saveAsTable("processed.pit_stops")

# COMMAND ----------

#Full load + inrimental writting
#merge_condition="tgt.result_id=src.result_id AND tgt.race_id=src.race_id"
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
write_data(final_df,"processed","pit_stops",merge_condition, "race_id")

# COMMAND ----------



# COMMAND ----------

display(spark.read.format('delta').load("/mnt/finaldatabricks/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

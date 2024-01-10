# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import *
from  pyspark.sql.functions import current_timestamp, lit,to_timestamp

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name file_date which will take the value of the file_date paramter as a variable. 

# COMMAND ----------

main_schema="resultId int, driverId int, raceId int, constructorId int, number int, grid int, position int, positionText string, positionOrder int, points float, laps int, time string, milliseconds int, fastestLap int, rank int, fastestLapTime string, fastestLapSpeed string, statusId int"

# COMMAND ----------

ingest_df=spark.read.schema(main_schema).json(f'/mnt/finaldatabricks/raw/{file_date}/results.json')

# COMMAND ----------

display(ingest_df)

# COMMAND ----------

final_df=ingest_df.withColumnsRenamed({'resultID':'result_id','raceId':'race_id','driverId':'driver_id','constructorId':'constrcutor_id','positionText':'postion_text','positionOrder':'postion_order',"fastestLap":"fastest_lap","fastestLapTime":"fastest_lap_time","fastestLapSpeed":"fastest_lap_speed"}).withColumn("ingestion_date",current_timestamp()).drop('url').withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------

#Without incrimental writting mehtod
# #Here we have given partionby which will write over date over seprate folder over the unique column values of the race_id
# #final_df.write.partitionBy("race_id").mode("overwrite").parquet("/mnt/finaldatabricks/processed/results")
# final_df.write.partitionBy("race_id").mode("overwrite").format("parquet").saveAsTable("processed.results")
# display(spark.read.parquet("/mnt/finaldatabricks/processed/results"))

# COMMAND ----------

# Method 1 for inrcimental load 
#This method has beeen retarded as thr drop partition and writtting again  will take more time comparison to method 2  
# race_ids=[]
# for race_id in final_df.select("race_id").distinct().collect(): #This will create a list of key value pairs 
#     if spark._jsparkSession.catalog().tableExists("processed.results"):
#         spark.sql(f"alter table processed.results drop if exists partition (race_id={race_id.race_id})")
# final_df.write.partitionBy("race_id").mode("append").format("parquet").saveAsTable("processed.results")
#Method 2 refer to common used function notebook

# COMMAND ----------

# This is  pyspark version of data writing
# merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
# merge_delta_data1(final_df, 'processed', 'results',"/mnt/finaldatabricks/processed/",merge_condition, 'race_id')

# COMMAND ----------

#This method uses the sql function to write the temp views.
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
write_data(final_df, 'processed', 'results',merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  processed.results

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe processed.results

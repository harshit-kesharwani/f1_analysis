# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,lit,to_timestamp

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name file_date which will take the value of the file_date paramter as a variable. 

# COMMAND ----------

#Here it is import to define the schema as the CSV file does not contains header so we have define the schmea fast, without defining the schema, the file will not be readed.
df_schema="race_id int,driver_id int,lap int,position int,  time string, milliseconds int"

# COMMAND ----------

#Here we are reading multiple csv files, the csv files are split into mutliple files,so we have given the folder path rest, it will read by its own.
df_lap =spark.read.schema(df_schema).csv(f"/mnt/finaldatabricks/raw/{file_date}/lap_times")

# COMMAND ----------

display(df_lap)
df_lap.count() #checking that total number of expected records are here or not.

# COMMAND ----------

final_df=df_lap.withColumn("ingestion_date",current_timestamp()).withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Withtout incrimental load
#final_df.write.parquet("/mnt/finaldatabricks/processed/lap_times",'overwrite')
#final_df.write.format("parquet").mode("overwrite").saveAsTable("processed.lap_times")

# COMMAND ----------

#Full load + inrimental writting
#The connection logics are calculated with the table from ergast.com/docs/f1db_user_guide.txt
merge_condition="src.race_id=tgt.race_id and src.driver_id=tgt.driver_id and tgt.lap=src.lap"
write_data(final_df,"processed", "lap_times",merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.format("delta").load("/mnt/finaldatabricks/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from  pyspark.sql.functions import current_timestamp, lit,to_timestamp, concat, col

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/finaldatabricks/raw/

# COMMAND ----------

spark.read.json(f"/mnt/finaldatabricks/raw/{file_date}/constructors.json").show()

# COMMAND ----------

#while reading the json we don't need to specifcy the header
constructors_df=spark.read.schema("constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING").json(f"/mnt/finaldatabricks/raw/{file_date}/constructors.json")
display(constructors_df)

# COMMAND ----------

#Here we are dropping one column of the URL
droped_column_df=constructors_df.drop("url")
display(droped_column_df)

# COMMAND ----------

#pysparkallows you to run multiple api calls from the single line,so here we are doing the same running multiple API calls on the single line
final_df=droped_column_df.withColumnsRenamed({"constructorID":'constrcutor_id',"constructorRef":"constrcutor_ref"}).withColumn("ingestion_date",current_timestamp())
final_df=final_df.withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------


final_df.write.mode("overwrite").format("delta").saveAsTable("processed.constructors") #("/mnt/finaldatabricks/processed/constructors")

# COMMAND ----------

df=spark.read.format('delta').load("/mnt/finaldatabricks/processed/constructors").show()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



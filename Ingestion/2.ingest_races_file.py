# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import *
from  pyspark.sql.functions import current_timestamp, lit,to_timestamp, concat, col

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

#This will read and create a dataframe , consider the header using options(header=True) and inferning schem, by schema(vaules)
#races_df=spark.read.options(header=True, inferSchema=True).csv('dbfs:/mnt/finaldatabricks/raw/races.csv') 
# #This reads the schema automatically but may take time in the case of big data sets)
races_df=spark.read.options(header=True).schema("raceID INT,year INT, round INT, circuitID INT,name STRING, date DATE,time STRING, url STRING").csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/races.csv')
races_df.printSchema()
display(races_df)


# COMMAND ----------

#Here we have added two new columns and  specifically we have given the race_timestamp where we have contactenated two columns into one and format into a specific timestamp format 
added_column=races_df.withColumns({'ingestion_date':current_timestamp(),"race_timestamp": to_timestamp(concat(col('date'),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss')})
display(added_column)

# COMMAND ----------

#Here we have renamed some columns, rest of the columns will come asits
renamed_column_df=added_column.withColumnsRenamed({"raceID":'race_id',"circuitID":'circuit_id','year':'race_year'})
renamed_column_df=renamed_column_df.withColumn("file_date",lit(file_date))
display(renamed_column_df)

# COMMAND ----------

#Selecting some specific columns 
final_df=renamed_column_df.select("race_id","race_year","round","circuit_id","name","race_timestamp","ingestion_date","file_date")
final_df.printSchema()
display(final_df)

# COMMAND ----------

#Here we are combing selecting and renaming in a siingle statement =, here we are selecting and renaming both using alias
final_df2=added_column.select(col('raceID').alias('race_id'),col('year').alias('race_year'),col("round"),col("circuitID").alias("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"))
display(final_df2)

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("processed.races")

# COMMAND ----------

df=spark.read.parquet("/mnt/finaldatabricks/processed/races")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

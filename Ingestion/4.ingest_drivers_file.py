# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,lit, concat,col

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

#Here we have a nested json object the name filed contains forename and surname, so we have created a seprate schema for it and will pass this in the main schema.
name_schema=StructType(fields=[StructField('forename',StringType(),True),StructField('surname',StringType(),True)])

# COMMAND ----------

#Here we have uded the name_schnma inside the main schema
main_schema =StructType(fields=[StructField('code',StringType(),True,),StructField('dob',DateType(),True),StructField('driverId',IntegerType(),True),StructField("driverRef",StringType(),True),StructField("name",name_schema,True),StructField("nationality",StringType(),True),StructField("number",IntegerType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

ingest_dftest=spark.read.schema(main_schema).json(f'/mnt/finaldatabricks/raw/{file_date}/drivers.json')
ingest_dftest.printSchema()
display(ingest_dftest)

# COMMAND ----------

#Here we have  defined the schema and given the data type for all the field, majorly if we see the name  we have struct where we have defined the internal structure of the json, along with internal objects data type.
ingest_df=spark.read.schema("code string, dob date, driverId int, driverRef string,name struct<forename:string, surname:string>, nationality string, number int,url string").json(f'/mnt/finaldatabricks/raw/{file_date}/drivers.json')
ingest_df.printSchema()

# COMMAND ----------

display(ingest_df)

# COMMAND ----------

#making multiple api calls on the single line
final_df=ingest_df.withColumnsRenamed({'driverId':'driver_id','driverRed':'driver_ref'}).withColumns({'name': concat(col("name.forename"),lit(" "),col("name.surname")),'timestamp':current_timestamp()}).drop('url')
final_df=final_df.withColumn("file_date",lit(file_date))
display(final_df)

# COMMAND ----------

#final_df.write.parquet("/mnt/finaldatabricks/processed/drivers","overwrite")
final_df.write.mode("overwrite").format("parquet").saveAsTable("processed.drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/finaldatabricks/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# Databricks notebook source
# MAGIC %md
# MAGIC ### Here we are going to read the circuit_file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the CSV file from spark Dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
v_data_scource=dbutils.widgets.get("p_data_source")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name file_date which will take the value of the file_date paramter as a variable. 

# COMMAND ----------

#Here we have included the another notebook which contains all the global variables

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

#Here we are importing the notebook which contains the common function of ingestion date

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC The below code is use to the check the storage mounts

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import *
from  pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

##The below code is use to the list of file available in the raw conatainer

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/finaldatabricks/raw

# COMMAND ----------

#The code is to read CSV and create a dataframe, we have imported the raw path variable from another notebook
circuit_df=spark.read.csv(f'{raw_path}/{file_date}/circuits.csv')

# COMMAND ----------

#The spark method to display the data
circuit_df.show()

# COMMAND ----------

#DataBricks method to display dataframe which is a better method to display the data
new_df=add_current_date(circuit_df)
display(new_df)

# COMMAND ----------

#Check what is the type of the circuit_df 
type(circuit_df)

# COMMAND ----------

#The header is not detected in the dataframe so to detect the header using option("header",True)
circuit_df=spark.read.option('header',True).csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/circuits.csv')
display(circuit_df)

# COMMAND ----------

#using the option while reading CSV to infern the schema automatically, Please note that the header need to be detected before inferschema
circuit_df_1=spark.read.option('header',True).option('inferSchema',True).csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/circuits.csv')
circuit_df_1.printSchema()

# COMMAND ----------

#Here we are going to use the options rather than option so that we can give multiple input instead of giving single input per option tag.
#Please note that while using the options we are not going to give the option name in inverted commas we are going to give the name directly.
circuit_df_2=spark.read.options(header=True ,inferSchema=True).csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/circuits.csv')
circuit_df_2.printSchema()

# COMMAND ----------

#Here we are going to understand the data to give the schema because inferschema not always gives the correct result as per the requirement #and if the dataset is large then it will take so much of time to infer the schema.
display(circuit_df_2.describe())

# COMMAND ----------

#Here we are going to give the a schema described by us and inforce it to the dataframe. here the structField contains (column name, datatype(),isNull boolean value)
circuit_df_schema=StructType(fields=[StructField("circuitId",IntegerType(),False), 
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True)])
circuit_df_3=spark.read.\
    options(header=True).schema(circuit_df_schema).csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/circuits.csv')
circuit_df_3.printSchema()
display(circuit_df_3)

# COMMAND ----------

circuit_df_4=spark.read.options(header=True ).schema("circuitId INT,circuitREF STRING,name STRING,location STRING,lat DOUBLE,lng DOUBLE,alt INT").csv(f'dbfs:/mnt/finaldatabricks/raw/{file_date}/circuits.csv')
circuit_df_4.printSchema()

# COMMAND ----------

#This is the easiest method of selecting the schema, here the coloumn name are not case sensitive
circuits_selected_df=circuit_df_3.select("circuitID","circuitREF","name","location","lat","lng","alt")

# COMMAND ----------

#The below methods are different from the above as they support coloumn operations which are not supported by the method stated above.
circuits_selected_df=circuit_df_3.select(circuit_df_3.circuitId,circuit_df_3.circuitRef,circuit_df_3.name,circuit_df_3.location,circuit_df_3.lat,circuit_df_3.lng,circuit_df_3.alt)

# COMMAND ----------

#The coloumn names in this statement are also not case sensitive
circuits_selected_df=circuit_df_3.select(circuit_df_3["circuitID"],circuit_df_3["circuitRef"],circuit_df_3["name"],circuit_df_3["country"],circuit_df_3["location"],circuit_df_3["lat"],circuit_df_3["lng"],circuit_df_3["alt"])

# COMMAND ----------

from pyspark.sql.functions import col
display(circuit_df_3)

# COMMAND ----------

#The coloumn names in this statement are also not case sensitive
circuits_selected_df=circuit_df_3.select(col("circuitid"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(circuits_selected_df)

# COMMAND ----------

#Here the column names are getting changed as per the requirement, the non renamaed columns will appear same without any change, please note we can use any of the date frame of above cmd 22 or cmd 20. there is a also a method called as withcolumnrenamed which is use for single column renaming purpose.
cirucuits_renamed_df=circuits_selected_df.withColumnsRenamed({'circuitid':'circuit_id',"circuitRef":"circuit_ref",'lat':'latitude','lng':'longitude','alt':'altitude'})
display(cirucuits_renamed_df)

# COMMAND ----------

test_df_parameter=cirucuits_renamed_df.withColumn("ingestion_date", current_timestamp()).withColumn("environment",lit(v_data_scource))
display(test_df_parameter)

# COMMAND ----------

#Here we are including the ingestion date by adding a new column to it using with column, the currenttime stamp need to be imported.
final_df=cirucuits_renamed_df.withColumn("ingestion_date", current_timestamp())
final_df =final_df.withColumn("file_date", lit(file_date))
display(final_df)

# COMMAND ----------

#Here we are giving explicit value as a column value so here we are using lit function in which we have wrapped the normal text, so that it can be converted into column, please note that we also need to import the lit function from pyspark.sql.functions.
test_df =cirucuits_renamed_df.withColumn("env", lit('prod'))
test_df =test_df.withColumn("file_date", lit(file_date))
display(test_df)

# COMMAND ----------

#Here we are writting thr content to the proccessed container in delta file format, here we have given the parameter as overwrite because it may give error when we already have the file created, so it will overwrite we run the notebook again. Please note here circuits is a folder, the file is circuited with multiple files inside this folder.
#final_df.write.delta("/mnt/finaldatabricks/processed/circuits","overwrite")
#The below method is use wo write the date as well as infer the schema to hivemeta store
final_df.write.mode("overwrite").format("delta").saveAsTable("processed.circuits")

# COMMAND ----------

display(spark.read.delta(f"{processed_path}/circuits"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/finaldatabricks/processed/

# COMMAND ----------

dbutils.notebook.exit("Success")

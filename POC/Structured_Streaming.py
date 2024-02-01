# Databricks notebook source
# Reading streaming tables
streamDF=spark.readStream.table("input_table")

# COMMAND ----------

streamDF.writeStream.tirgger(processingTime="2 minutes").ouputMode("append").option("checkpointLocations"/path").table("output_table")
#Trigger undefined then default processing time i.e 500 ms
#Fixed internal :   .trigger(processingTime="2minutes")      # Process data in microbatches at user specified interval
#Triggered Batch .trigger(once=True)                         # Process all available data in single batch then stop
#Triggered microbatches .trigger(availbleNow=True)           # Process all availble data   in multiple micro-batches, then stop
#Output Modes: append this is default,  outputMode("Complete") #This will overwrite the with each batch mostly used in gold layers.
#Checkpoint is to track the progress 
#spark streaming does not support sorting and deduplication.
                                    

# COMMAND ----------

# MAGIC %sql 
# MAGIC --COPY COMMAND
# MAGIC COPY INTO my_table
# MAGIC FROM '/path/to/files’
# MAGIC FILEFORMAT = <format>
# MAGIC FORMAT_OPTIONS (<format options>)
# MAGIC COPY_OPTIONS (<copy options>);
# MAGIC --If we run this command it will write incirmental data which is newly added to S3.
# MAGIC COPY INTO my_table
# MAGIC FROM '/path/to/files’
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('delimiter' = '|’,
# MAGIC 'header' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true’)
# MAGIC Derar Alhussein

# COMMAND ----------

# MAGIC %PYTHON 
# MAGIC #Auto loader: This should be used only when there are billions of file expected to be ingested.
# MAGIC spark.readStream
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", <source_format>)
# MAGIC .load('/path/to/files’)
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", <checkpoint_directory>)
# MAGIC .table(<table_name>)
# MAGIC #The below method is to add with also specifiying the schema in the case of file formats which do not have specified formats.
# MAGIC spark.readStream
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", <source_format>)
# MAGIC .option("cloudFiles.schemaLocation", <schema_directory>)
# MAGIC .load('/path/to/files’)
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", <checkpoint_directory>)
# MAGIC .option("mergeSchema", “true”)
# MAGIC .table(<table_name>)

# COMMAND ----------



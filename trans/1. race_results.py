# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_df=spark.read.format('delta').load(f"{processed_path}/circuits").withColumnsRenamed({"location":"circuit_location"})

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/races").withColumnsRenamed({"name":"race_name","race_timestamp":"race_date"})

# COMMAND ----------

display(races_df)

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_path}/drivers").withColumnsRenamed({"name":"driver_name","nationality":"driver_nationality","number":"driver_number"})

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_path}/constructors").withColumnRenamed("name","team")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_path}/results").filter(f"file_date='{file_date}'").withColumnsRenamed({"time":"race_time","race_id":"result_race_id","file_date":"result_file_date"})

# COMMAND ----------

results_df.display()

# COMMAND ----------

race_circuit_joined_df=races_df.join(circuits_df,races_df["circuit_id"]==circuits_df["circuit_id"],"inner")

# COMMAND ----------

combined_df=results_df.join(constructors_df,constructors_df.constrcutor_id==results_df.constrcutor_id,"inner")\
    .join(drivers_df,drivers_df.driver_id==results_df.driver_id,"inner")\
    .join(race_circuit_joined_df,race_circuit_joined_df.race_id==results_df.result_race_id,)

# COMMAND ----------

final_df=combined_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_nationality","driver_number","team","grid","fastest_lap","race_time","points","position", "result_race_id","result_file_date").withColumn("created_date",current_timestamp()).withColumnRenamed("result_file_date",'file_date').withColumnRenamed("result_race_id","race_id")

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Here we have used the orderBy along with the dataframe name and tht option desc
display(final_df.filter("race_year=2020 and race_name='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.parquet(f"{presentation_path}/race_results","overwrite")
#final_df.write.format("parquet").mode("overwrite").saveAsTable("presentation.race_Results")

# COMMAND ----------

#Full_load + incrimental writting
write_data(final_df,"presentation","race_results",'race_id')

# COMMAND ----------

display(spark.read.parquet(f"{presentation_path}/race_results"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1) from presentation.race_results group by race_id

# COMMAND ----------

test=spark.read.parquet(f"{presentation_path}/race_results")

# COMMAND ----------

test.printSchema()

# COMMAND ----------

dbutils.notebook.exit("The presentation layer data is completed")

# COMMAND ----------



# COMMAND ----------

print("Hello world")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls '/mnt/finaldatabricks/processed/'

# COMMAND ----------



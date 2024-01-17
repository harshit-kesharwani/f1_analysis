# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_path}/race_results").filter(f"file_date='{file_date}'")

# COMMAND ----------

race_results_df.display()

# COMMAND ----------

race_year=df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_path}/race_results").filter(col('race_year').isin(race_year))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
"""Here we are fetching the race_year on the particular file of specific date and then going to make a list and then create a dataframe having year value same, because we may have same year data in two days file. so file calculating the standings it is important to consider all the files i.e consider all the recods of a year beyond the file of a specific date"""
driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# #final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.driver_standings")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
write_data(final_df, 'presentation', 'driver_standings',merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from presentation.driver_standings where race_year=2020

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_year, file_date from processed.races where file_date= '2021-03-21'

# COMMAND ----------



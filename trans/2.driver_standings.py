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

race_results_df = spark.read.parquet(f"{presentation_path}/race_results").filter(f"file_date='{file_date}'")

# COMMAND ----------

race_results_df.display()

# COMMAND ----------

race_year=df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_path}/race_results").filter(col('race_year').isin(race_year))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
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

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# #final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.driver_standings")

# COMMAND ----------

write_data(final_df,"presentation","driver_standings",'race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from presentation.driver_standings

# COMMAND ----------



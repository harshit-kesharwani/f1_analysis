# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct,sum,

# COMMAND ----------

race_result_df=spark.read.parquet(f"{presentation_path}/race_results")

# COMMAND ----------

race_result_df.printSchema()

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

df_demo=race_result_df.filter("race_year in (2019,2020)")

# COMMAND ----------

#Count total number of rows 
#df_demo.select(count("*")).show()

# COMMAND ----------

#Counting distinct number of "circuit+location"
df_demo.select(countDistinct("circuit_location")).show()

# COMMAND ----------

#Here we are filtering a s driver then we are selecting the sum of points of the particular driver
display(df_demo.filter("driver_name='Kimi Räikkönen'").select(sum("points")))

# COMMAND ----------

#Here we have have taken multiple agregate value under 1 select statement with the renamed column name 
display(df_demo.filter("driver_name='Kimi Räikkönen'").select(sum("points").alias("points total") ,countDistinct("race_name")).withColumnsRenamed({"count(DISTINCT race_name)":"an"}))

# COMMAND ----------

#Here we have have taken multiple agregate value under 1 select statement with the renamed column name 
display(df_demo.filter("driver_name='Kimi Räikkönen'").select(sum("points").alias("points total") ,countDistinct("race_name")).withColumnRenamed("count(DISTINCT race_name)","an"))

# COMMAND ----------

display(df_demo)

# COMMAND ----------

#Here we  have group by the driver name and sum there resepective points
df_demo.groupBy("driver_name").sum("points").display()

# COMMAND ----------

df_demo.groupBy("driver_name").agg(sum("points"),countDistinct("race_name")).show() #Here we are using the multiple aggregate functions so we have used the agg function. Here we have recived the groupBy data object, whenever we apply the group by tranfformation the dataframe is converted into 

# COMMAND ----------

#Here we are going to demonstrate the widnow function, here we are going to create a widnow for 2020 and 2019

# COMMAND ----------

demo_grouped_df=df_demo.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("Number of races")) #Here we have created a dataframe which stores the aggregate information with there alias.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,desc, rank
driverrankspec=Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank",rank().over(driverrankspec)).display()
#here we have first created a widnow with a partion, like we have split the date based on yeaar 1 section for 2020, another for 2019,and then we have ordered it based on total points in desrcending order and then we have uded the rank function to rank it with refrence to the window we have created.

# COMMAND ----------



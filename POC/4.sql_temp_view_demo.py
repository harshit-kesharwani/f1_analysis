# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_Result_df=spark.read.parquet(f"{presentation_path}/race_results")

# COMMAND ----------

display(race_Result_df)

# COMMAND ----------

race_Result_df.createTempView("test_view")
#Here we have created a temporary SQL view, which can help use to perform sql operation treating it as real db or view
#Please note that the views are only valid inside the spark session and once a view is created, it will not be created again

# COMMAND ----------

#In order to resolve the conflict that view might have been already created. we can use the below command
race_Result_df.createOrReplaceTempView("test_view")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from test_view

# COMMAND ----------

race_year=2019

# COMMAND ----------

spark.sql(f"select * from test_view where race_year={race_year}").show()
#Here we have run the sql statement inside the python this is helpful when you wan to write some dynamic sql query

# COMMAND ----------

race_Result_df.createGlobalTempView("g_test_view")
#The difference between the global and the normal is that, global can be accessed from the other notebook as well.

# COMMAND ----------

race_Result_df.createOrReplaceGlobalTempView("g_test_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.g_test_view
# MAGIC --Here we have to give a prefix global_temp. else we will not able view the view, reason is global view are created in a different table.

# COMMAND ----------

spark.sql("select * from global_temp.g_test_view").display()

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------



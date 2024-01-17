# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_result_df=spark.read.format('delta').load(f"{presentation_path}/race_results").filter(f"file_date='{file_date}'")

# COMMAND ----------

race_year=df_column_to_list(race_result_df,"race_year")

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_path}/race_results").filter(col("race_year").isin(race_year))

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count,desc,rank
from pyspark.sql.window import  Window 

# COMMAND ----------

constructor_standing_df=race_result_df.groupBy("race_year","team").agg(sum(col("points")).alias("total_points"),count(when(col("position")==1,True)).alias("wins"))
#Here we have defined a dataframe which is group by the above fields and contain the aggregate value of the above  specifically points and the poition, counting the wins we have used  when col(position)==1, true, it will count all the occurance of the fields where position is   1.

# COMMAND ----------

 display(constructor_standing_df.filter("race_year=2020").orderBy(desc("wins")))

# COMMAND ----------

constrcutor_rank_spec=Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df=constructor_standing_df.withColumn("rank",rank().over(constrcutor_rank_spec))
#Here we have created a window which is orderd by total points first and if it is same, then it go according to the wins in descending order. then we haved used the rank function to rank it acording to the window specified.

# COMMAND ----------

final_df.display()

# COMMAND ----------

# #final_df.write.parquet(f"{presentation_path}/constructor_standings","overwrite")
# final_df.write.format("parquet").mode("overwrite").saveAsTable("presentation.constructor_standings")

# COMMAND ----------

#Full load + incriemental writting
#write_data(final_df,'presentation',"constructor_standings",'race_year')
merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
write_data(final_df, 'presentation', 'constructor_standings',merge_condition, 'race_year')

# COMMAND ----------

spark.read.format('delta').load(f"{presentation_path}/constructor_standings").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from presentation.constructor_standings where race_year=2020

# COMMAND ----------



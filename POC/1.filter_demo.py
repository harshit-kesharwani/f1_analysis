# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

filtered_df=races_df.where("race_year=2021 and round>5") #Here we can use filter in place of where, this is the sql method of applying filter, the condition is written inside the double inverted commas.

# COMMAND ----------

display(filtered_df)

# COMMAND ----------

#filtered_df2=races_df.filter(( races_df.race_id > 1) & (races_df.race_year == 2021 )) 
#Here the commented and the below written method both are same,please note that square brackets are important else it will through datatype error.
filtered_df2=races_df.filter(( races_df['race_id'] > 1) & (races_df['race_year'] == 2021 )) 

# COMMAND ----------

display(filtered_df2)

# COMMAND ----------



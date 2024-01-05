# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_path}/races").filter('race_year=2021').withColumnRenamed("name","race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuit_df=spark.read.parquet(f"{processed_path}/circuits").withColumnRenamed("name","circuit_name").filter("circuit_id<10")

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

joined_df_inner=races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'inner').select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(joined_df_inner)

# COMMAND ----------

races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'right').select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.race_name,races_df.round).show()

# COMMAND ----------

races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'left').select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.race_name,races_df.round).show()

# COMMAND ----------

races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'outer').select(circuit_df.circuit_name,circuit_df.location,circuit_df.country,races_df.race_name,races_df.round).show()

# COMMAND ----------

display(f"Count={joined_df_inner.count()}")

# COMMAND ----------

#semi joined is the join in which we get only get the data of the left dataframe, which are corresponding the right dataframe.Please note no any field from right side is allowed to be shown
races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'semi').select(races_df.race_name,races_df.round).show()

# COMMAND ----------

#anti joined is the join in which we get only get the data of the left dataframe, which are **NOT**  corresponding the right dataframe.Please note no any field from right side is allowed to be shown
races_df.join(circuit_df,races_df.circuit_id==circuit_df.circuit_id,'anti').select(races_df.race_name,races_df.round).show()

# COMMAND ----------

#Cross join is cartisian product of right and left
#The METHOD TO PERFORM CROSS JOIN IS DIFFERENT FROM OTHER JOINS, IT IS crossJoin(field_name)
races_df.crossJoin(circuit_df).count()


# COMMAND ----------



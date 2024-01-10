# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_current_date(df):
    output_df=df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def order_partition(df,partition_column):
    column_list=df.schema.names
    column_list.remove(partition_column)
    column_list.append(partition_column)
    new_df=df.select(column_list)
    return new_df

# COMMAND ----------

# """Here we have set the spark conf because in the case of static partion it will overwrite all the data,in the case of dynamic partition it will search for the partition and then only over write the data, the order_partition is used here because the partition column should be in the end of dataframe """
# def write_data(df,database_name, table_name, partition_column):
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#     ordered_df= order_partition(df,partition_column) 
#     if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
#         #Here we have used insertInto as it will search for the partion and then overwrite, if not found then will add the data
#         ordered_df.write.mode("overwrite").insertInto(f"{database_name}.{table_name}")
#     else:
#         ordered_df.write.partitionBy(partition_column).mode("overwrite").format("parquet").saveAsTable(f"{database_name}.{table_name}")
#Here we have the code whcih we will work with parquet as well.
    

# COMMAND ----------

"""Here we have set the spark conf because in the case of static partion it will overwrite all the data,in the case of dynamic partition it will search for the partition and then only over write the data, the order_partition is used here because the partition column should be in the end of dataframe """
def write_data(df,database_name, table_name,merge_condition, partition_column):
    if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
        df.createOrReplaceTempView("input_table")
        spark.sql(f""" merge into {database_name}.{table_name} tgt  using input_table src on {merge_condition} when matched then update set * when not matched then insert * """)
    else:
        df.write.partitionBy(partition_column).mode("overwrite").format("delta").saveAsTable(f"{database_name}.{table_name}")
    
    

# COMMAND ----------

def merge_delta_data1(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list    

# COMMAND ----------

#This function can be run by importing this notebook using the %run command please note that only a single command can be run on a cell while running the %run command. .. is used to go back to the basic folder structure, like in ingestion files we are doing like %run "../includes/common_functions"

# COMMAND ----------



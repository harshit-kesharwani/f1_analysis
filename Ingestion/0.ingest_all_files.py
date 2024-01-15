# Databricks notebook source
v_result=dbutils.notebook.run("9.create_processed_database",0)

# COMMAND ----------

v_result=dbutils.notebook.run("1.ingest_circuits_file",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("2.ingest_races_file",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("3.ingest_constructors _file",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("4.ingest_drivers_file",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("5.Ingest_results",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------


v_result

# COMMAND ----------

v_result=dbutils.notebook.run("6.ingest_pit_stops_file",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------


v_result=dbutils.notebook.run("7.ingest_lap_times",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("8. ingest_qualifying",0,{"p_date_source":"Ergast API","file_date":'2021-03-28'})

# COMMAND ----------

v_result

# COMMAND ----------

#This is to run the the all ingestion notebooks from the job scheduler which will help to run all the motebooks at a speified time.
#We can use the job cluster, as well as all purpose cluster for job running but, job clusters are cheaper so we should use the job clusters as much as possible, we can also give parameters and confirmation email for failure or succes.

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from processed.races

# COMMAND ----------



# COMMAND ----------



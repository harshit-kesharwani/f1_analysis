# Databricks notebook source
# MAGIC %sql
# MAGIC use processed

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21") #here we have created a parameter which will ask you for the paramter and if no parameter is given it will check for the second argument as default parameter, which is blank in this case.
file_date=dbutils.widgets.get("file_date")#Here we have difned a variable name v_data_source which will take the value of the p_data_source paramter as a variable. 

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM processed.results 
                JOIN processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN processed.constructors ON (results.constrcutor_id = constructors.constrcutor_id)
                JOIN processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                AND results.file_date = '{file_date}'
""")

# COMMAND ----------

 spark.sql(f""" merge into presentation.calculated_race_results tgt  using race_result_updated src ON (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id) when matched then UPDATE SET tgt.position = src.position,
                           tgt.points = src.points,
                           tgt.calculated_points = src.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp) """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from presentation.calculated_race_results

# COMMAND ----------

#This notebook is written with sql just for learning purpose some audit columns has been included

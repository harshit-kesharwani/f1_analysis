-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

create database demo;
-- This statements create the database

-- COMMAND ----------

create database IF not exists demo;  
-- This statement creates a database when the database does not exists.

-- COMMAND ----------

show databases

-- COMMAND ----------

desc database  default
-- Desc command is use to describe the database,  we can aslo use describe command, instead of desc

-- COMMAND ----------

describe database  default

-- COMMAND ----------

describe database extended default
-- we can use describe or desc with extended keyword to get a detailed information about a the database

-- COMMAND ----------

select current_database()
-- This is just to check which database are we in

-- COMMAND ----------

use demo
--We can change the default database using the use keyword.

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in default
-- we can check the tables in the a particular database using the in keyword.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df=spark.read.parquet(f"{presentation_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")
-- MAGIC #This is managed table
-- MAGIC #This is the correct approach to use the overwrite method, this written method does not works: race_result_df.write.saveAsTable("demo.race_results_pythons", "overwrite")

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc extended race_results_python;

-- COMMAND ----------

use default

-- COMMAND ----------

desc extended demo.race_results_python ;

-- COMMAND ----------

select * from demo.race_results_python;

-- COMMAND ----------

select * from demo.race_results_python where race_year=2020

-- COMMAND ----------

create table race_result_sql as
select * from demo.race_results_python where race_year=2020;
--This is managed table because here we are not giving the path

-- COMMAND ----------

select * from race_result_sql

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

use demo

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table race_results_pythons

-- COMMAND ----------

--There is a difference between the external m table in the hive  store and the managed table, while creating an managed table we do not need to specify the location and once we drop the table the data is also deleted but this is not the case with external tables, there will be data in the table even if we drop the table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").mode("overwrite").option("path",f"{presentation_path}/race_result_ext_py").saveAsTable("demo.race_results_ext_py")
-- MAGIC #External Table 

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql (
  race_year int,
  race_name string,
  race_date timestamp,
  circuit_location string,
  driver_name string,
  driver_nationality string,
  driver_number int,
  team string,
  grid int,
  fastest_lap int,
  race_time string,
  points float,
  position int,
  created_date TimeStamp
)
using parquet
Location "/mnt/udemylearninghk/presentation/race_results_ext_sql"

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------



-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

desc extended demo.race_results_ext_sql

-- COMMAND ----------

insert into demo.race_results_ext_sql 
select * from demo.race_results_ext_py where race_year=2020

-- COMMAND ----------

-- MAGIC %py
-- MAGIC race_result_df.display()

-- COMMAND ----------

print("hello")

-- COMMAND ----------

drop table  demo.race_results_ext_sql 

-- COMMAND ----------

 create temp view v_race_results
 as 
 select * from demo.race_results_python
 where race_year=2020

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create global temp view gv_race_results
as select * from demo.race_results_python


-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create view pv_race_results
as 
select * from demo.race_results_python

-- COMMAND ----------

select * from pv_race_results

-- COMMAND ----------



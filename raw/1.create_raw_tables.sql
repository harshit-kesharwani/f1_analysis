-- Databricks notebook source

create schema  if not exists processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create tables for csv files

-- COMMAND ----------

drop table if exists raw.circuits;
create table if not exists raw.circuits( circuitId int, circuitRef String, name String, location string, country string, lat double, lng double, alt int, url string)
using csv
options(path "/mnt/finaldatabricks/raw/circuits.csv", header true)
--Here we are creating the table with the existing file but we are specifing the location so it is external table.

-- COMMAND ----------

select * from raw.circuits;

-- COMMAND ----------

desc extended raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create race tables

-- COMMAND ----------

drop table if exists raw.races;
create table if not exists raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date date,
time string,
url string
)
using csv
options(path "/mnt/finaldatabricks/raw/races.csv", header true)

-- COMMAND ----------

select * from raw.races;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####create tables with Json files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####create Constructor table
-- MAGIC * Simple Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

drop table if exists raw.constructors;
create table if not exists raw.constructors(
  constructorId int,
  constructorRef string,
  name string,
  nationality  string,
  url string)
  using json 
  options(path "/mnt/finaldatabricks/raw/constructors.json")

-- COMMAND ----------

select * from raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create driver table
-- MAGIC * Single line Json
-- MAGIC * Complex structure 

-- COMMAND ----------

drop table  if exists raw.drivers;
create table if not exists raw.drivers(
  driverId int,
  driverRef String,
  number int,
  code string,
  name struct <forename:String, surname: String>,
  dob date,
  nationality string,
  url string)
  using json
  options(path "/mnt/finaldatabricks/raw/drivers.json")



-- COMMAND ----------

select *from raw.drivers;

-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS raw.results;
CREATE TABLE IF NOT EXISTS raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/finaldatabricks/raw/results.json")

-- COMMAND ----------

select * from raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS raw.pit_stops;
CREATE TABLE IF NOT EXISTS raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/finaldatabricks/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS raw.lap_times;
CREATE TABLE IF NOT EXISTS raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/finaldatabricks/raw/lap_times")

-- COMMAND ----------

select * from raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS raw.qualifying;
CREATE TABLE IF NOT EXISTS raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/finaldatabricks/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM raw.qualifying

-- COMMAND ----------

DESC EXTENDED raw.qualifying;

-- COMMAND ----------



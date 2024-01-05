-- Databricks notebook source
use processed

-- COMMAND ----------

drop table if exists presentation.calculated_race_results;
create table presentation.calculated_race_results
using  parquet 
as 
select races.race_year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11-results.position as calculated_points
 from results
join drivers on (results.driver_id=drivers.driver_id)
join constructors on (results.constrcutor_id=constructors.constrcutor_id)
join races on (races.race_id=results.race_id)
where results.position<=10;

-- COMMAND ----------

select * from presentation.calculated_race_results

-- COMMAND ----------



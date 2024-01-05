-- Databricks notebook source
select team_name  ,
  count(1)as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
  from presentation.calculated_race_results
  group by team_name
  having count(1)>50
  order by avg_points desc;

-- COMMAND ----------

select team_name  ,
  count(1)as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
  from presentation.calculated_race_results
  where race_year between 2016 and 2020
  group by team_name
  having count(1)>100
  order by avg_points desc;

-- COMMAND ----------



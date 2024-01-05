-- Databricks notebook source
select driver_name,
  count(1)as total_races,
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points
  from presentation.calculated_race_results
  group by driver_name
  having count(1)>50
  order by avg_points desc;

-- COMMAND ----------



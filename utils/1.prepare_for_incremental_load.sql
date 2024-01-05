-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS processed
LOCATION "/mnt/finaldatabricks/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS presentation 
LOCATION "/mnt/finaldatabricks/presentation";

-- COMMAND ----------



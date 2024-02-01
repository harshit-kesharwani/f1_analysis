# Databricks notebook source
# MAGIC %sql
# MAGIC  
# MAGIC SET database_name.var = marketing;
# MAGIC SHOW TABLES in ${database_name.var};
# MAGIC
# MAGIC
# MAGIC SET database_name.dummy= marketing;
# MAGIC SHOW TABLES in ${database_name.dummy};
# MAGIC

# COMMAND ----------

employee_id=3248356

# COMMAND ----------

spark.sql(f"set employee_id.var={employee_id}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee where EMPLOYEE_ID=${employee_id.var}

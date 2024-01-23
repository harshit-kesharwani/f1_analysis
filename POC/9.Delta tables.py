# Databricks notebook source
df=spark.read.format('parquet').load('dbfs:/mnt/finaldatabricks/presentation/race_results')

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop schema if exists dempdelta cascade;
# MAGIC create schema dempdelta 
# MAGIC location 'dbfs:/mnt/finaldatabricks/dempdelta'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE dempdelta;

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.select("race_year","race_date","circuit_location","team","grid","race_id").filter("race_id==862")

# COMMAND ----------

df1.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dempdelta.t1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dempdelta.t1

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

df2=df.select("race_year","race_date",upper("circuit_location").alias("circuit_location"),"team","grid", "race_id").filter("race_id==862")

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.mode("overwrite").format("delta").option("overwriteschema",True).saveAsTable("dempdelta.t2")

# COMMAND ----------

spark.sql(""" 
merge into dempdelta.t1 a using dempdelta.t2  b on a.grid= b.grid and a.race_id=b.race_id
when MATCHED THEN 
update set a.circuit_location =b.circuit_location
when not matched then
insert(race_year,race_date, circuit_location, team,grid, race_id)values(race_year,race_date, circuit_location, team,grid, race_id)""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dempdelta.t1 

# COMMAND ----------

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy('race_year').saveAsTable("dempdelta.race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC Delta file format is also same as the parquet format, the only difference the two is that delta format stores the log files which allowss us to visit the history of the table.  In backend delta stores the file in the parquet along with logs in the json format. The snapshots of previous is stored. although logs can checked like when and what type of modificaiton has been performed can be checked for lifetime.

# COMMAND ----------


spark.sql("update  dempdelta.race_results set points =20- position")

# COMMAND ----------

# MAGIC %sql 
# MAGIC use dempdelta;
# MAGIC select * from race_results

# COMMAND ----------

spark.sql("delete from dempdelta.race_results where position>10 or position is  null ")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dempdelta.race_results where position is not  null 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --using the below sql statement we can check how the table has been partitioned 
# MAGIC SHOW PARTITIONS dempdelta.race_results

# COMMAND ----------

# MAGIC %sql 
# MAGIC --this command is used to check the historical logs of the delta tables when the table was modified.
# MAGIC desc History dempdelta.t1

# COMMAND ----------

# MAGIC %sql 
# MAGIC --we can check the data present in any of the version of the table using the below command.
# MAGIC select * from  dempdelta.t1 version as of 0

# COMMAND ----------

#we can also create the dataframe from the previous version of the table.
df2=spark.sql("""select * from demp_delta.t1 version as of 0""")

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  VACUUM demp_delta.t1
# MAGIC  --This command is use to delete the history of the table greater than 7 days

# COMMAND ----------

# MAGIC  %sql 
# MAGIC  VACUUM demp_delta.t1 RETAIN 0 HOURS
# MAGIC  --Here we are changing the hour 0 to decrease the retention  time.

# COMMAND ----------

spark.databricks.delta.retentionDurationCheck.enabled = false
#here we have done this as  per the recommendation after running the above cell

# COMMAND ----------

# MAGIC %sql 
# MAGIC --If we have deleted the latest version of data or we want to restore to previous version, we can simply do it using the below statement.
# MAGIC restore table demp_delta.t1 to version as of 2
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC --we can convert parquet to delta using the below statement
# MAGIC convert to delta demp_delta.t1
# MAGIC --Please note that the table written above is already in parquet format.

# COMMAND ----------

#Managed table will delete the data in s3 also if we delete the table
#External table will not delete the table even if we delete the table

# COMMAND ----------

# MAGIC %sql
# MAGIC --we can optimize the table by using zorder and optimize command, this will order the file as per the given field.
# MAGIC optimize demp_delta.t1
# MAGIC zorder by (employee_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC --if we drop the table, then folder will be also deleted.
# MAGIC -- we can add comment to table with below statement.
# MAGIC CREATE Table abc
# MAGIC comment 'hello'
# MAGIC PARTITIONED BY (city, date_of_birth)
# MAGIC location 'anc/d/'
# MAGIC as select ab, cd, ef from users

# COMMAND ----------

# MAGIC %sql 
# MAGIC --databricks allow adding constrains to table, there are two types of constrain not null constraints and check constrains.

# COMMAND ----------

# MAGIC %sql
# MAGIC --there are two types of cloning.
# MAGIC --Deep clone  cloned  meta data + original table data from source.
# MAGIC -- while in case of shallow command only delta logs are copied.
# MAGIC --please note that if make any changes on the cloned tables,it will NOT AFFECT THE ORIGINAL TABLES.

# COMMAND ----------

# MAGIC %sql
# MAGIC --There are three types of views in databricks.
# MAGIC --stored views i.e views are stored in storage and can be created using "create view" statement.
# MAGIC -- temp views are temporary view that last only till session end, sessions ends when we attach or deattach a notebook restart a cluster or install a new pyton package. created using "create temp view" --temp view can not be accessed outside the notebook. 
# MAGIC -- Global temporary view are the temp views that last till the cluster does not restarts.  "create global temp view <view_name>".
# MAGIC -- accessing data for global temp: "select * from global_temp.<view_name>"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Show tables in <database_name> --This command will help to see all the tables, temp, global, views in the database.

# COMMAND ----------

# MAGIC %sql 
# MAGIC --we can use wildcard character to read the list of files from a directory.
# MAGIC --select * from json.'file_path/*.join'
# MAGIC
# MAGIC --reading columns from nested json select customer_id, profile:first_name, profile:last_name, profile:address:country
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC   --Here we are applying a filter function to add a new column
# MAGIC   select order_id, books, filter(books,i->i.quatity>=)as multiple_copies
# MAGIC   from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC --we can apply specific tranformation to the table
# MAGIC select 
# MAGIC order_id, books, tranform(books, bcast(b.sutotal*0.8 as int)) as subtotal
# MAGIC from orders

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Here we are defing the UDF
# MAGIC create or replace function get_url(email string)
# MAGIC return string
# MAGIC return concact("https://www.",split(email ,"@")[1])
# MAGIC --Above we have defined the udf 

# COMMAND ----------

# MAGIC %sql
# MAGIC --we can use the UDF directly
# MAGIC select email, get_url(email)as domain
# MAGIC from customers;

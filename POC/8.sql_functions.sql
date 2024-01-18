-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

show databases

-- COMMAND ----------

use processed

-- COMMAND ----------

select *, concat(driverRef,"-",code) as new_code from processed.drivers 

-- COMMAND ----------

--select*, split(name, " " ) from processed.drivers
select*, split(name, " " )[0] first_name, split(name, " " )[1] last_name  from processed.drivers

-- COMMAND ----------

select *, current_timestamp from processed.drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy') from driversc

-- COMMAND ----------

select *, date_add(dob,1) from processed.drivers 
--Here we are increasing the date by 1 day

-- COMMAND ----------

select count(*) from drivers

-- COMMAND ----------

select max(dob) from drivers
--select name, max(dob) from drivers
--This will not work because we need a groupby clause to do so


-- COMMAND ----------

select * from drivers where dob='2000-05-11'

-- COMMAND ----------

select count(*)  from drivers
group by nationality='ans';
--Here it is giving the wrong output actually, it is not able to find any row in natiionality where the value is ans. so giv

-- COMMAND ----------

select * from drivers
where (nationality='British' and dob>'1990-01-01') or nationality='Indian';

-- COMMAND ----------

select nationality, count(*) as count from drivers
group by nationality
having count(*)>10
order by count  ;

-- COMMAND ----------

---Here is the demonstration of window function, here we have given the ranking based on DOB per country
select name, nationality, dob, rank() over(partition by nationality order by dob) as Rank from drivers

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet(f"{processed_path}/drivers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, concat_ws, lit
-- MAGIC
-- MAGIC def partial_mask_column(df, column_name, mask_char='*', mask_start=0, mask_length=None):
-- MAGIC     """
-- MAGIC     Partially masks the specified column in a PySpark DataFrame.
-- MAGIC
-- MAGIC     Args:
-- MAGIC         df (DataFrame): The input DataFrame.
-- MAGIC         column_name (str): The name of the column to be partially masked.
-- MAGIC         mask_char (str, optional): The character used for masking. Default is '*'.
-- MAGIC         mask_start (int, optional): The starting index from which masking begins. Default is 0.
-- MAGIC         mask_length (int, optional): The length of the substring to be masked.
-- MAGIC             If None, it masks the rest of the string. Default is None.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         DataFrame: A new DataFrame with the specified column partially masked.
-- MAGIC     """
-- MAGIC     if mask_length is None:
-- MAGIC         mask_length = lit(df.select(column_name).first()[0].__len__() - mask_start)
-- MAGIC
-- MAGIC     masked_column = concat_ws("", col(column_name).substr(1, mask_start),
-- MAGIC                                lit(mask_char * mask_length),
-- MAGIC                                col(column_name).substr(mask_start + mask_length + 1))
-- MAGIC
-- MAGIC     return df.withColumn(column_name, masked_column)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession
-- MAGIC spark = SparkSession.builder.appName("PartialMasking").getOrCreate()
-- MAGIC
-- MAGIC # Sample DataFrame
-- MAGIC data = [("John Doe", "123-45-6789"),
-- MAGIC         ("Jane Smith", "987-65-4321"),
-- MAGIC         ("Bob Johnson", "555-12-3456")]
-- MAGIC
-- MAGIC columns = ["Name", "SSN"]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Partially mask the "SSN" column
-- MAGIC masked_df = partial_mask_column(df, "SSN", mask_char='X', mask_start=7)
-- MAGIC
-- MAGIC # Show the result
-- MAGIC masked_df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, when, lit
-- MAGIC
-- MAGIC # Create a Spark session
-- MAGIC spark = SparkSession.builder.appName("ColumnMasking").getOrCreate()
-- MAGIC
-- MAGIC # Sample DataFrame
-- MAGIC data = [("Alice", 25),
-- MAGIC         ("Bob", 30),
-- MAGIC         ("Charlie", 35),
-- MAGIC         ("David", 40)]
-- MAGIC
-- MAGIC columns = ["Name", "Age"]
-- MAGIC df = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Define a function to mask a column
-- MAGIC def mask_column(df, column_to_mask, mask_character='*'):
-- MAGIC     return df.withColumn(column_to_mask, when(col(column_to_mask).isNotNull(), lit(mask_character * 5)).otherwise(col(column_to_mask)))
-- MAGIC
-- MAGIC # Mask the "Age" column
-- MAGIC masked_df = mask_column(df, "Age")
-- MAGIC
-- MAGIC # Show the resulting DataFrame
-- MAGIC masked_df.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df = spark.createDataFrame(
-- MAGIC     [(1, 'Aman', 27, 'aman_93@gmail.com', '9923150074'),
-- MAGIC      (2, 'Prateek', 28, 'prateek_gulati@reddit.com', '8727451936'),
-- MAGIC      (3, 'Rajat', 27, 'goyal.rajat@gmail.com', '9871288442')],
-- MAGIC     ['Customer_Number', 'Customer_Name', 'Customer_Age', 'Email', 'Mobile']
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumn('new_Email', F.regexp_replace('Email', '(?<!^).(?=.+.@)', '*'))
-- MAGIC df = df.withColumn('new_Mobile', F.regexp_replace('Mobile', '(?<!^).(?!$)', '*'))
-- MAGIC
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumn('Mobile', F.regexp_replace('Mobile', '^(.).', '*$1'))
-- MAGIC df.display()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.withColumn('Mobile', F.regexp_replace('moblie', '^.{5}', '*****'))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

--lEARNED A NEW METHOD TO GET THE INFO LIKE IS THE TABLE IS OF MANAGED TYPE OR EXTERNAL, this feature is only available in premium tier databricks 

select * from 
system.information_schema.tables
where  table_name ='results'

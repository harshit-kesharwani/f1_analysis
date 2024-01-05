# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql import *
from  pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pip install pyaes

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


import binascii



# Sample data
data = [(12000, 'empnameKey/fhci4=dnv73./xorb3f05'), (15000, 'anotherKey')]

# Create a DataFrame
df = spark.createDataFrame(data, ["plaintext", "key"])

# Define the encryption function
def encrypt(plaintext, vKey):
    if plaintext is None:
        return None
    plaintext_str = str(plaintext)  # Convert the integer to a string
    key = vKey.encode('utf-8')  # Convert the key to bytes
    aes = pyaes.AESModeOfOperationCTR(key)
    cipher_txt = aes.encrypt(plaintext_str)
    cipher_txt2 = binascii.hexlify(cipher_txt)
    return str(cipher_txt2.decode('utf-8'))

# Use the expr function to apply the encryption logic to the DataFrame
df_encrypted = df.withColumn(
    "encrypted_value",expr("encrypt(plaintext, key)")
)

# Show the result
df_encrypted.show()

# Stop the Spark session


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import pyaes
import binascii

# Initialize a Spark session
spark = SparkSession.builder.appName("EncryptionExample").getOrCreate()

# Sample data
data = [(12000, 'empnameKey/fhci4=dnv73./xorb3f05'), (15000, 'anotherKey')]

# Create a DataFrame
df = spark.createDataFrame(data, ["plaintext", "key"])

# Define the encryption function
def encrypt(plaintext, vKey):
    if plaintext is None:
        return None
    plaintext_str = str(plaintext)  # Convert the integer to a string
    key = vKey.encode('utf-8')  # Convert the key to bytes
    aes = pyaes.AESModeOfOperationCTR(key)
    cipher_txt = aes.encrypt(plaintext_str)
    cipher_txt2 = binascii.hexlify(cipher_txt)
    return str(cipher_txt2.decode('utf-8'))

# Create a UDF to apply the encryption function to a DataFrame column
encrypt_udf = udf(encrypt, StringType())

# Apply the UDF to encrypt the 'plaintext' column and create a new 'encrypted_value' column
df_encrypted = df.withColumn("encrypted_value", encrypt_udf(df["plaintext"], df["key"]))

# Show the result
df_encrypted.show()

# Stop the Spark session



# COMMAND ----------

import os

# Generate a random AES key
encryption_key = os.urandom(16)
encryption_key_hex = encryption_key.hex()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import pyaes
import binascii
import os

# Sample data
data = [("John", 25), ("Alice", 30), ("Bob", 22)]
columns = ["Name", "Age"]

# Create a PySpark DataFrame
spark = SparkSession.builder.appName("EncryptionExample").getOrCreate()
df = spark.createDataFrame(data, columns)

def encrypt(plaintext, vKey):
    if plaintext is None:
        return None
    plaintext_str = str(plaintext)  # Convert the integer to a string
    key = bytes.fromhex(vKey)  # Convert the key from hex to bytes
    aes = pyaes.AESModeOfOperationCTR(key)
    cipher_txt = aes.encrypt(plaintext_str.encode('utf-8'))
    cipher_txt2 = binascii.hexlify(cipher_txt)
    return cipher_txt2.decode('utf-8')

def encrypt_dataframe_columns(data_frame, columns_to_encrypt, encryption_key):
    # Create a copy of the original DataFrame to avoid modifying it in-place
    encrypted_df = data_frame

    for column in columns_to_encrypt:
        if column in encrypted_df.columns:
            encrypted_df = encrypted_df.withColumn(
                column,
                expr(f"UDF(cast({column} as string), '{encryption_key}')")
            )

    return encrypted_df

# Register the UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

udf_encrypt = udf(encrypt, StringType())

spark.udf.register("UDF", udf_encrypt)

# Columns to encrypt
columns_to_encrypt = ["Name", "Age"]

# Generate a random AES key
encryption_key = os.urandom(16).hex()

# Encrypt specified columns
encrypted_df = encrypt_dataframe_columns(df, columns_to_encrypt, encryption_key)

# Show the encrypted DataFrame
encrypted_df.show()

# Stop the SparkSession



# COMMAND ----------

utf8_char = chr(0xE3)
print(utf8_char)


# COMMAND ----------

form pyspark.sql.functions import StructType

# COMMAND ----------

# Sample data
data = [("John", 25), ("Alice", 30), ("Bob", 22)]
schema = StructType([
   StructField("name", StringType(), True),
   StructField("age", IntegerType(), True)])
columns = ["Name", "Age"]
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from spark.sql import mask

# COMMAND ----------

def masking(df):
    masked_df=df
    for num in df.columns:
        masked_df=masked_df.withColumn(num,mask(num))
            

# COMMAND ----------

df.createTempView("demo")


# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select Name, Age, mask( CAST(Age AS varchar(20))) as masked_age from demo
# MAGIC

# COMMAND ----------

new_df=sqlContext.sql("select Name, Age, mask( CAST(Age AS varchar(20))) as masked_age from demo")

# COMMAND ----------

new_df.display()

# COMMAND ----------

Create 

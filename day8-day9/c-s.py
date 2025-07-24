# from delta.tables import *
# from delta import configure_spark_with_delta_pip
# from pyspark.sql.functions import col
# import os

# delta_table_path = "C:/Users/sudarshan.zunja/Desktop/dE-tr/day8/tmp"

# builder = SparkSession.builder.appName("DeltaLake") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
#     .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# data = [(1, "Alice", "UK"), (2, "Bob", "Germany"), (3, "Carlos", "Spain")]

# df = spark.createDataFrame(data, ['user_id', 'name', 'country'])
# # df.write.parquet(delta_table_path)

# df.write.format("delta").mode("overwrite").save(delta_table_path)
# if not os.path.exists(os.path.join(delta_table_path, "_delta_log")):
#     raise Exception("Delta log not found. Delta table was not written properly.")

# spark.sql("CREATE TABLE IF NOT EXISTS customer_delta USING DELTA LOCATION 'C:/Users/sudarshan.zunja/Desktop/dE-tr/day8/tmp'")

# print("Delta table created successfully.")
# input()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip
import random
import time

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [
    (1, 'Alice', 'UK'),
    (2, 'Bob', 'Germany'),
    (3, 'Carlos', 'Spain')
]
 
df = spark.createDataFrame(data, ['user_id', 'name', 'country'])
 
df.write.mode("overwrite").parquet('/tmp/customer_parquet')
 
parquet_df = spark.read.parquet('/tmp/customer_parquet')
 
parquet_df.write.format("delta").mode("overwrite").save("/tmp/customer_delta")
 
spark.sql("DROP TABLE IF EXISTS customer_delta")
spark.sql("CREATE TABLE customer_delta USING DELTA LOCATION '/tmp/customer_delta'")
 
print("Original Delta Table:")
spark.sql("SELECT * FROM delta.`/tmp/customer_delta`").show()
 
spark.sql("DELETE FROM delta.`/tmp/customer_delta` WHERE user_id = 2")
 
print("After Deletion (Bob removed):")
spark.sql("SELECT * FROM delta.`/tmp/customer_delta`").show()
 
print("DESCRIBE HISTORY:")
spark.sql("DESCRIBE HISTORY delta.`/tmp/customer_delta`").show(truncate=False)
 
print("VERSION AS OF 0:")
spark.sql("SELECT * FROM delta.`/tmp/customer_delta` VERSION AS OF 0").show()
 
spark.sql("RESTORE TABLE customer_delta TO VERSION AS OF 0")
 
print("After RESTORE:")
spark.sql("SELECT * FROM customer_delta").show()
 
from delta.tables import *
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import broadcast
import time

delta_table_path = "C:/Users/sudarshan.zunja/Desktop/dE-tr/day8/regions"

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.csv("regions.csv", header=True, inferSchema=True)
df_2 = spark.read.csv("regions.csv", header=True, inferSchema=True)

start = time.time()
rdf = df_2.join(broadcast(df), "region")
et1 = time.time()-start
rdf.show()

start = time.time()
# rdd1 = df.rdd.map(lambda row: (row['region'], row))
# rdd2 = df_2.rdd.map(lambda row: (row['region'], row))
rdd1 = df_2.join(df, "region")
et2 = time.time()-start

print("et1: ", et1)
print("et2: ", et2)

spark.sql("CREATE TALBE SALES IF NOT EXISTS ()")

input()
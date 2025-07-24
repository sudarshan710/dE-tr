from delta.tables import *
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col

delta_table_path = "C:/Users/sudarshan.zunja/Desktop/dE-tr/day8/regions"

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.parquet("regions/part-00000-c490db71-79b8-480b-b629-c0eae18b6b0a-c000.snappy.parquet")

rdf = df.filter(col("region") == "Asia").count()
print(rdf)
input()
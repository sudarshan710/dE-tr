from pyspark.sql.functions import col
from delta.tables import *
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


data = [(1, "Alice"), (2, "Bob"), (3, "Carol")]
df = spark.createDataFrame(data, ["id", "name"])

delta_path = "output/"
df.write.format("delta").mode("overwrite").save(delta_path)

df2 = spark.createDataFrame([(4, "DAvid"), (5, "Eva")], ["id", "name"])
df2.write.format("delta").mode("append").save(delta_path)

print("Delta Table Content: ")
spark.read.format("delta").load(delta_path).show()

DeltaTable.forPath(spark, delta_path).vacuum(0.0)

log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
print(os.listdir(log_path))
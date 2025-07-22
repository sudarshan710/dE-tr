from delta.tables import *
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import udf, functools as F
import os
from pyspark.sql.types import StringType

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.json("emp.json", multiLine=True)

text_folder = "emp_files" 

def read_file_content(emp_name):
    first_name = emp_name.split()[0].lower()
    file_path = os.path.join(text_folder, f"{first_name}.txt")
    try:
        with open(file_path, "r") as f:
            content = f.read().strip()
        return content
    except Exception as e:
        return None

read_file_content_udf = udf(read_file_content, StringType())
result_df = df.withColumn("details", read_file_content_udf(F.col("empName")))

result_df.show(truncate=False)

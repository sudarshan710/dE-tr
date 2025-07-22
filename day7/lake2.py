from delta.tables import *
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.csv("sample_csv.csv", header=True, inferSchema=True)
schema = df.schema

o_path = "new_output/"
df.write.format("delta").mode("overwrite").save(o_path)

data2 = [(34, "DAvid", "pr", 345435)]

df2 = spark.createDataFrame(data2, schema)

df2.write.format("delta").mode("append").save(o_path)

print("Delta Table Content: ")
spark.read.format("delta").load(o_path).show()

# DeltaTable.forPath(spark, o_path).vacuum(0.0)

# log_path = os.path.join(o_path, "_delta_log")
# print("Files inside _delta_log:")
# print(os.listdir(log_path))
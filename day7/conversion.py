from delta.tables import *
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.json("emp.json", multiLine=True)
df.show()

txt = spark.read.text("nickname.txt").rdd.map(lambda r: r[0]).collect()  
broadcast_txt_lines = spark.sparkContext.broadcast(txt)

def find_details(emp_name):
    first_name = emp_name.split()[0].lower()
    matches = [line for line in broadcast_txt_lines.value if first_name in line.lower()]
    if matches:
        return " ".join(matches)
    else:
        return None
    
find_details_udf = udf(find_details, StringType())
result_df = df.withColumn("details", find_details_udf(col("empName")))

result_df.show(truncate=False)
result_df.write.mode("overwrite").json("result_json.json")
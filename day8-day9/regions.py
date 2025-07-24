from delta.tables import *
from delta import configure_spark_with_delta_pip

delta_table_path = "C:/Users/sudarshan.zunja/Desktop/dE-tr/day8/regions"

builder = SparkSession.builder.appName("DeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
df = spark.read.csv('regions.csv', header=True, inferSchema=True)

df_f = df.groupBy('region').count()
df_f.show()
df_f.write.format("delta").mode("overwrite").save(delta_table_path)

# spark.sql("DROP TABLE IF EXISTS regions_table")
# spark.sql(f"CREATE TABLE regions_table USING DELTA LOCATION '{delta_table_path}'")

# df.printSchema()
# df.show(5)

# spark.sql("""
#     OPTIMIZE regions_table
#     ZORDER BY region
# """)

# delta_table = DeltaTable.forPath(spark, "C:/Users/sudarshan.zunja/Desktop/dE-tr/day7/regions")
# delta_table.history(5).show(truncate=False)

input()
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("SquaredEg").getOrCreate()
df = spark.range(1, 100000).withColumn("squared", col("id")*col("id"))
df.groupBy((col("id")%10).alias("group")).count().show()
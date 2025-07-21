from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("rdd vs df").master("local[*]").getOrCreate()

start = time.time()
print("\n JSON as DF: ")
df = spark.read.option("multiline", "true").json("user_logs.json")
df = df.groupBy('user_id').avg('duration').show()

df_time = time.time() - start

start = time.time()
print("\n JSON as RDD: ")
rdd = spark.sparkContext.textFile("user_logs.json")
rdd = rdd.groupBy(lambda x: x[0])
for line in rdd.collect():
    print(line)

rdd_time = time.time() - start

print('\n Time taken for DF: ', df_time)
print('\n Time taken for RDD: ', rdd_time)
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("batch-stream-cont").getOrCreate()

df = spark.readStream.format("rate").option("rowPerSecond", 5).load()

jvm = spark._jvm

continuos_t = jvm.org.apache.spark.sql.streaming.Trigger.Continuous("1 second")

writer = df.writeStream \
         .format("console") \
         .outputMode("append") \
         .option("truncate", False)

writer._jwrite = writer._jwrite.trigger(continuos_t)

query = writer.start()
time.sleep(10)
query.stop()
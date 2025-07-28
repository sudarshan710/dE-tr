from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, split, when, col, size, window, to_timestamp, concat_ws, lit
import time

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
print("---------------------------------------------------------------------------------------")
start_ = time.time()
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

split_cols = split(lines.value, ",")

lines = lines.filter(size(split_cols) >= 4)

split_cols = split(lines.value, ",")


df = lines.select(
    to_timestamp(split_cols.getItem(0)).alias("timestamp"),
    split_cols.getItem(1).alias("terminal"),
    split_cols.getItem(2).alias("zone"),
    split_cols.getItem(3).cast("int").alias("slotID")
)

zone_counts = df.groupBy("zone").count()

zone_summary = zone_counts \
    .withColumn("status", when(col("count") > 80, "Occupied").otherwise("Available")) \
    .select(
        concat_ws(", ",
                  concat_ws(": ", lit("Zone"), col("zone")),
                  concat_ws(": ", lit("Count"), col("count").cast("string")),
                  concat_ws(": ", lit("Status"), col("status"))
        ).alias("summary")
    )

avg_summary = df \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(avg("slotID").alias("avg_slotID")) \
    .select(
        concat_ws(" : ",
                  concat_ws(" to ",
                            col("window.start").cast("string"),
                            col("window.end").cast("string")),
                  col("avg_slotID").cast("string")
        ).alias("summary")
    )

combined_summary = zone_summary.union(avg_summary)

query = combined_summary.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, monotonically_increasing_id

spark = SparkSession.builder.appName("SCD2_DimCustomer").getOrCreate()

source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Los Angeles", "Platinum")
], ["CustomerID", "Name", "Address", "LoyaltyTier"])

existing_df = source_df

joined_df = source_df.alias("src").join(
    existing_df.filter("IsCurrent = true").alias("tgt"),
    on="CustomerID",
    how="left"
)

changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("src.*")

expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("tgt.CustomerSK").withColumn("EndDate", current_date()) \
.withColumn("IsCurrent", lit(False))

new_version_df = changed_df \
    .withColumn("CustomerSK", monotonically_increasing_id()) \
    .withColumn("StartDate", current_date()) \
    .withColumn("EndDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))

final_df = existing_df.filter("IsCurrent = true") \
    .join(expired_df, "CustomerSK", "left_anti") \
    .unionByName(expired_df) \
    .unionByName(new_version_df)

final_df.write.format("delta").mode("overwrite").save("delta/dim_customer")
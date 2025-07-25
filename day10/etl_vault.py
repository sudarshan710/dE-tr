from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp, lit, row_number, desc

spark = SparkSession.builder \
        .appName("etl_vault") \
        .getOrCreate()

def createHub(deltaPath, tableName, busKey):
    raw_df = spark.read.format("delta").load(deltaPath)

    hub_df = raw_df.select(col(f"{busKey}")) \
                    .dropDuplicates([busKey]) \
                    .withColumn(f"{busKey}_HK", sha2(concat_ws("||", busKey), 256)) \
                    .withColumn("load_time", current_timestamp()) \
                    .withColumn("source", lit(tableName))
    
    hub_df.write.format("delta").mode("overwrite").save(f"delta/vault/hub_{tableName}")


def createLink(deltaPath, linkName, idList):
    raw_df = spark.read.format("delta").load(deltaPath)

    link_df = raw_df.select(*[col(c) for c in idList]) \
                    .withColumn(f"{linkName}_HK", sha2(concat_ws("||", *idList), 256)) \
                    .withColumn("load_time", current_timestamp()) 
    
    link_df.write.format("delta").mode("overwrite").save(f"delta/vault/{linkName}")


def createSatellite(deltaPath, tableName, attrList):
    raw_df = spark.read.format("delta").load(deltaPath)

    sat_df = raw_df.select(*[col(c) for c in attrList]) \
                    .withColumn(f"sat_{attrList[0]}_HK", sha2(concat_ws("||", attrList[0]), 256)) \
                    .withColumn(f"hash_diff_{tableName}", sha2(concat_ws("||", *[col(c) for c in attrList]), 256)) \
                    .withColumn("load_time", current_timestamp())
    
    sat_df.write.format("delta").mode("overwrite").save(f"delta/vault/sat_{tableName}")


def createPITCustomer(deltaPathVault, refDates):
    hubCustomer = spark.read.format("delta").load(deltaPathVault+"/hub_customers").alias("hub")
    satCustomer = spark.read.format("delta").load(deltaPathVault+"/sat_customers").alias("sat")

    hubCrossDates =  hubCustomer.crossJoin(refDates).alias("hubCross")

    print(satCustomer.show())
    filtered_df = hubCrossDates.join(
        satCustomer,
        (col("hubCross.customer_id_HK") == col("sat.sat_customer_id_HK")) &
        (col("sat.load_time") <= col("hubCross.refDate")),
        how="left"
    )
    print(filtered_df.show())
    # exit()
    winSpec = Window.partitionBy(col("hubCross.customer_id_HK"), col("hubCross.refDate"))\
                    .orderBy(desc(col("sat.load_time")))
    custWithDates = filtered_df.withColumn("row_num", row_number().over(winSpec))
    finalPITCust = custWithDates.filter(col("row_num") == 1).select(
        col("hubCross.customer_id_HK").alias("customer_id_HK"),
        col("hubCross.refDate").alias("refDate"),
        col("sat.customer_name"),
        col("sat.contact")
    )

    return finalPITCust


def createPITProduct(deltaPathVault, refDates):
    hubProducts = spark.read.format("delta").load(deltaPathVault+"/hub_products").alias("hub")
    satProducts = spark.read.format("delta").load(deltaPathVault+"/sat_products").alias("sat")

    hubCrossDates =  hubProducts.crossJoin(refDates).alias("hubCross")

    print(satProducts.show())
    filtered_df = hubCrossDates.join(
        satProducts,
        (col("hubCross.product_sku_HK") == col("sat.sat_product_sku_HK")) &
        (col("sat.load_time") <= col("hubCross.refDate")),
        how="left"
    )
    print(filtered_df.show())
    # exit()
    winSpec = Window.partitionBy(col("hubCross.product_sku_HK"), col("hubCross.refDate"))\
                    .orderBy(desc(col("sat.load_time")))
    custWithDates = filtered_df.withColumn("row_num", row_number().over(winSpec))
    finalPITProd = custWithDates.filter(col("row_num") == 1).select(
        col("hubCross.product_sku_HK").alias("product_sku_HK"),
        col("hubCross.refDate").alias("refDate"),
        col("sat.product_name"),
        col("sat.category"),
        col("sat.price")
    )

    return finalPITProd
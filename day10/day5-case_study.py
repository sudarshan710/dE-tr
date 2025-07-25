from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from etl_vault import createHub, createLink, createSatellite, createPITCustomer, createPITProduct

spark = SparkSession.builder \
        .appName("day5-cs") \
        .getOrCreate()

custPath = "data/customers.csv"
productsPath = "data/products.csv"
transactionsPath = "data/transactions.csv"

def ingest(path, tableName):
    raw_df = spark.read.csv(path, header=True)
    raw_df.write.format("delta").mode("overwrite").save(f"delta/raw/{tableName}")

ingest(custPath, "customers")
ingest(productsPath, "products")
ingest(transactionsPath, "transactions")


createHub("delta/raw/customers", "customers", "customer_id")
createHub("delta/raw/products", "products", "product_sku")

createLink("delta/raw/transactions", "link-transactions", ["transaction_id","customer_id","product_sku"])

createSatellite("delta/raw/customers", "customers", ["customer_id","customer_name","address","contact"])
createSatellite("delta/raw/products", "products", ["product_sku","product_name","category","price"])
createSatellite("delta/raw/transactions", "transactions", ["transaction_id","customer_id","product_sku","purchase_date","quantity","sales_amount"])

dates = [("2025-07-26",), ("2025-07-24",), ("2025-07-23",)]
refDates = spark.createDataFrame(dates, ["refDate"]).withColumn("refDate", to_timestamp("refDate"))

pitCustomer = createPITCustomer("delta/vault", refDates)
pitCustomer.show()

pitProducts = createPITProduct("delta/vault", refDates)
pitProducts.show()
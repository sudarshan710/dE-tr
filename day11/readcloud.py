from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readcloud").getOrCreate()

spark.conf.set("fs.azure.account.auth.type.blobstoragetestvikash551.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.blobstoragetestvikash551.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.blobstoragetestvikash551.dfs.core.windows.net", "sp=r&st=2025-07-28T12:07:57Z&se=2025-07-28T20:22:57Z&spr=https&sv=2024-11-04&sr=b&sig=BNODTdcmlThyGxGnoZ9ri1YMDvzD%2BRtJYXmAgSpv1bE%3D")    
spark.conf.set("spark.hadoop.fs.azure.account.hns.enabled", "true")

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load("abfss://csvfiles@blobstoragetestvikash551.dfs.core.windows.net/csvfiles/")

df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()
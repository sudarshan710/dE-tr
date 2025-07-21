from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('main file').master("local[*]").getOrCreate()

print("\n", spark.read.text("sample.txt").count(), "\n")

df = spark.read.csv("sample_csv.csv", header=True, inferSchema=True)
print("\n", df, df.show(), df.printSchema() ,"\n")
df.groupBy('dept').avg('salary').show()

df.show()

df = df.withColumn('tax_amount', df.salary*0.10)
df.show()

hod = spark.read.csv("hod.csv", header=True, inferSchema=True)

df = df.join(hod, on='dept')
df.show()

input()
spark.stop()
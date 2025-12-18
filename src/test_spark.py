from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYCTaxiTest").getOrCreate()

df = spark.read.parquet("/home/windfish/nyc-taxi-pipeline/data/raw/yellow_tripdata_2025-10.parquet")

df.printSchema()
df.show(5)

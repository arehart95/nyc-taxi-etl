from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName("NYCTaxiIngest") \
	.getOrCreate()

df = spark.read.parquet("/home/windfish/nyc-taxi-pipeline/data/raw/yellow_tripdata_2025-10.parquet")

df.write.mode("overwrite") \
	.parquet("/home/windfish/nyc-taxi-pipeline/data/processed/yellow_taxi")

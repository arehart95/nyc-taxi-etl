from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYCTaxiAnalytics").getOrCreate()

df = spark.read.parquet("/home/windfish/nyc-taxi-pipeline/data/processed/yellow_taxi_enriched")
df.createOrReplaceTempView("trips")

spark.sql("""
SELECT dropoff_zone, COUNT(*) AS trips
FROM trips
GROUP BY dropoff_zone
ORDER BY trips DESC
""").show()

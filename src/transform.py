from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

DIRECTORY = "/home/windfish/nyc-taxi-pipeline/"

spark = SparkSession.builder \
    .appName("NYCTaxiTransform") \
    .getOrCreate()

df = spark.read.parquet("/home/windfish/nyc-taxi-pipeline/data/processed/yellow_taxi")

zones = spark.read.csv(DIRECTORY + "data/raw/taxi_zone_lookup.csv", header="true", inferSchema="true")
# data quality rules
df = df.filter(col("trip_distance") > 0)
df = df.filter(col("fare_amount") > 0)

df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime")
     - unix_timestamp("tpep_pickup_datetime")) / 60
)

df = df.join(
    zones.select(col("LocationID").alias("PULocationID"), col("Zone").alias("pickup_zone")),
    on="PULocationID",
    how="left"
)

df = df.join(
    zones.select(col("LocationID").alias("DOLocationID"), col("Zone").alias("dropoff_zone")),
    on="DOLocationID",
    how="left"
)
df.write.mode("overwrite").parquet("/home/windfish/nyc-taxi-pipeline/data/processed/yellow_taxi_clean")
df.write.mode("overwrite").parquet(DIRECTORY + "data/processed/yellow_taxi_enriched")
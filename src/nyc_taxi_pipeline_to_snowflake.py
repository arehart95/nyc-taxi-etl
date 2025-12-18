import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pyarrow.parquet as pq
from dotenv import load_dotenv
load_dotenv()
# -------------------------
# CONFIG
# -------------------------
DIRECTORY = "/home/windfish/nyc-taxi-pipeline/"
RAW_TAXI_PATH = DIRECTORY + "data/raw/yellow_tripdata_2025-10.parquet"
ZONE_LOOKUP_PATH = DIRECTORY + "data/raw/taxi_zone_lookup.csv"
ENRICHED_OUTPUT_PATH = DIRECTORY + "data/processed/yellow_taxi_enriched"


SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "COMPUTE_WH",
    "database": "NYC_TAXI",
    "schema": "PUBLIC",
    "table": "TAXI_TRIPS"
}

# -------------------------
# SPARK SESSION
# -------------------------

spark = SparkSession.builder \
    .appName("NYCTaxiEndToEndPipeline") \
    .getOrCreate()

# -------------------------
# INGEST
# -------------------------

print("Reading raw taxi parquet...")
df = spark.read.parquet(RAW_TAXI_PATH)

# -------------------------
# CLEAN & TRANSFORM
# -------------------------

print("Cleaning and transforming data...")

df = df.filter(col("trip_distance") > 0)
df = df.filter(col("fare_amount") > 0)

df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp("tpep_dropoff_datetime") -
     unix_timestamp("tpep_pickup_datetime")) / 60
)

# -------------------------
# ZONE LOOKUP JOIN
# -------------------------

print("Joining zone lookup...")

zones = spark.read.csv(ZONE_LOOKUP_PATH, header=True, inferSchema=True)

df = df.join(
    zones.select(
        col("LocationID").alias("PULocationID"),
        col("Zone").alias("pickup_zone")
    ),
    on="PULocationID",
    how="left"
)

df = df.join(
    zones.select(
        col("LocationID").alias("DOLocationID"),
        col("Zone").alias("dropoff_zone")
    ),
    on="DOLocationID",
    how="left"
)

# -------------------------
# WRITE ENRICHED PARQUET
# -------------------------

print("Writing enriched parquet dataset...")
df.write.mode("overwrite").parquet(ENRICHED_OUTPUT_PATH)

# -------------------------
# LOAD TO SNOWFLAKE
# -------------------------

print("Loading data into Snowflake...")

# Read parquet into pandas (OK for 1 month of data)
table = pq.read_table(ENRICHED_OUTPUT_PATH)
pdf = table.to_pandas()

conn = snowflake.connector.connect(
    user=SNOWFLAKE_CONFIG["user"],
    password=SNOWFLAKE_CONFIG["password"],
    account=SNOWFLAKE_CONFIG["account"],
    warehouse=SNOWFLAKE_CONFIG["warehouse"],
    database=SNOWFLAKE_CONFIG["database"],
    schema=SNOWFLAKE_CONFIG["schema"]
)

success, nchunks, nrows, _ = write_pandas(
    conn,
    pdf,
    SNOWFLAKE_CONFIG["table"],
    auto_create_table=True
)

print(f"Loaded {nrows} rows into Snowflake table {SNOWFLAKE_CONFIG['table']}")

conn.close()
spark.stop()

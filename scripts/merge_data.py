import os
import sys

# Ensure root directory is on Python path so spark_util can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

import spark_util


def run_merge():
    print("=== Starting Step 1: Merge Flights Data ===")

    submit_args = (
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio-ml-workshop:9000 "
        "--conf spark.hadoop.fs.s3a.access.key=minio "
        "--conf spark.hadoop.fs.s3a.secret.key=minio123 "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.3.3"
    )

    spark = spark_util.getOrCreateSparkSession("Enrich flights data", submit_args)
    spark.sparkContext.setLogLevel("INFO")

    from pyspark.sql.functions import broadcast, col

    # 1. Read flights from PostgreSQL
    print("Reading flights table from PostgreSQL (pg-flights-data:5432)...")
    df_flights = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://pg-flights-data:5432/postgres") \
        .option("dbtable", "flights") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # 2. Read airport and airline lookup CSVs from MinIO
    print("Reading airlines and airports lookup datasets from MinIO...")
    df_airlines = spark.read.format("csv").option("header", "true").load("s3a://airport-data/airlines.csv")
    df_airports = spark.read.format("csv").option("header", "true").load("s3a://airport-data/airports.csv")

    # 3. Rename columns for clean join
    df_airlines = df_airlines.withColumnRenamed("IATA_CODE", "AL_IATA_CODE")
    df_o_airports = df_airports.selectExpr(
        "IATA_CODE as ORIG_IATA_CODE",
        "AIRPORT as ORIG_AIRPORT",
        "CITY as ORIG_CITY",
        "STATE as ORIG_STATE",
        "COUNTRY as ORIG_COUNTRY",
        "LATITUDE as ORIG_LATITUDE",
        "LONGITUDE as ORIG_LONGITUDE"
    )
    df_d_airports = df_airports.selectExpr(
        "IATA_CODE as DEST_IATA_CODE",
        "AIRPORT as DEST_AIRPORT",
        "CITY as DEST_CITY",
        "STATE as DEST_STATE",
        "COUNTRY as DEST_COUNTRY",
        "LATITUDE as DEST_LATITUDE",
        "LONGITUDE as DEST_LONGITUDE"
    )

    # 4. Join datasets
    print("Joining flights with airlines, origin airports, and destination airports...")
    df_flights = df_flights \
        .join(broadcast(df_airlines), col("airline") == col("AL_IATA_CODE")) \
        .join(broadcast(df_o_airports), col("origin_airport") == col("ORIG_IATA_CODE")) \
        .join(broadcast(df_d_airports), col("destination_airport") == col("DEST_IATA_CODE"))

    # 5. Write to MinIO S3A
    output_location = "s3a://flights-data/flights"
    print(f"Streaming write to {output_location} in Parquet format...")
    df_flights.write.mode("overwrite") \
        .option("header", "true") \
        .format("parquet").save(output_location)

    # 6. Verify count
    saved_count = spark.read.parquet(output_location).count()
    print(f"=== Merge Flights Data Completed Successfully! Saved records: {saved_count} ===")
    spark.stop()


if __name__ == "__main__":
    run_merge()

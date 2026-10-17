import os
import sys

# Ensure root directory is on Python path so spark_util can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

import spark_util


def run_clean():
    print("=== Starting Step 2: Clean Flights Data ===")

    submit_args = (
        "--conf spark.hadoop.fs.s3a.endpoint=http://minio-ml-workshop:9000 "
        "--conf spark.hadoop.fs.s3a.access.key=minio "
        "--conf spark.hadoop.fs.s3a.secret.key=minio123 "
        "--conf spark.hadoop.fs.s3a.path.style.access=true "
        "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
        "--conf spark.hadoop.fs.s3a.multipart.size=104857600 "
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.3.3"
    )

    spark = spark_util.getOrCreateSparkSession("Enrich flights data", submit_args)
    spark.sparkContext.setLogLevel("INFO")
    print("Spark context started.")

    from pyspark.sql.functions import array, col, explode, struct, lit, approx_count_distinct, udf

    data_location = "s3a://flights-data/flights/*.parquet"
    print(f"Reading merged dataset from {data_location}...")
    df_flights = spark.read.parquet(data_location)
    df_flights.printSchema()
    print(f"Raw flights count: {df_flights.count()}")

    # Drop unwanted columns
    df_clean = df_flights.drop("AL_IATA_CODE", "ORIG_IATA_CODE", "DEST_IATA_CODE")
    df_clean.printSchema()

    # Finding the columns with the same values throughout the dataset is an expensive operation.
    # Spark provides the approx_count_distinct() function, which is pretty fast.
    print("Finding and dropping constant single-value columns...")
    df_distinct = df_clean.agg(*(approx_count_distinct(col(c)).alias(c) for c in df_clean.columns))

    cols, dtypes = zip(*((c, t) for (c, t) in df_distinct.dtypes))

    kvs = explode(array([
        struct(lit(c).alias("column_name"), col(c).alias("distinct_count")) for c in cols
    ])).alias("kvs")

    distinct_count = df_distinct.select([kvs]).select(["kvs.column_name", "kvs.distinct_count"])

    uni_value_fields = distinct_count.filter(distinct_count.distinct_count == 1)
    uni_value_fields.show(50)

    col_names = [str(row.column_name) for row in uni_value_fields.select("column_name").collect()]
    print(f"Dropping columns with only 1 distinct value: {col_names}")
    df_clean = df_clean.drop(*col_names)
    df_clean.printSchema()

    # Create the label field that determines whether the flight is delayed or not.
    print("Calculating DELAYED target label (departure_delay >= 15 min)...")
    delay_threshold = 15

    @udf("integer")
    def is_delayed(departure_delay, cancelled):
        if cancelled == 1:
            return 0
        if departure_delay is not None and departure_delay >= delay_threshold:
            return 1
        return 0

    df_clean = df_clean.withColumn("DELAYED", is_delayed(df_clean.departure_delay, df_clean.cancelled))
    df_clean.select("month", "day", "flight_number", "departure_delay", "cancelled", "DELAYED").show(50)

    # Write the physical data to the same S3 bucket under the flights-clean path.
    output_location = "s3a://flights-data/flights-clean"
    print(f"Writing to {output_location} in Parquet format...")

    #df_flights.cache()
    df_clean.write.mode("overwrite") \
        .option("header", "true") \
        .format("parquet").save(output_location)

    final_count = df_clean.count()
    print(f"=== Clean Flights Data Completed Successfully! Final clean records: {final_count} ===")
    spark.stop()


if __name__ == "__main__":
    run_clean()

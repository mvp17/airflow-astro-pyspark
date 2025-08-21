from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

KAFKA_TOPIC = "iot_data"
KAFKA_BROKER = "kafka:9092"

spark = None
try:
    # Start Spark session
    spark = SparkSession.builder \
        .appName("KafkaToMinIO") \
        .getOrCreate()

    configure_minio(spark)
    log.info("Spark session created and MinIO configured.")

    # IoT schema
    iot_schema = StructType([
        StructField("device_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ]), True)
    ])

    # Read streaming data from Kafka
    log.info(f"Subscribing to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BROKER}")
    iot_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON payloads
    iot_df = iot_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), iot_schema).alias("data")) \
        .select("data.*")

    # Ensure MinIO bucket exists
    bucket_name = "iot.bucket"
    if not MINIO_CLIENT.bucket_exists(bucket_name):
        MINIO_CLIENT.make_bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' created.")
    else:
        log.info(f"Bucket '{bucket_name}' already exists.")

    output_path_json = f"s3a://{bucket_name}/iot_data/{CURRENT_DATE}/"
    # output_path_parquet = f"s3a://{bucket_name}/iot_data/{CURRENT_DATE}.parquet"

    log.info(f"Streaming JSON output to {output_path_json}")
    query_json = iot_df.writeStream \
        .format("json") \
        .option("path", output_path_json) \
        .option("checkpointLocation", "s3a://iot.bucket/checkpoints/json") \
        .outputMode("append") \
        .start()

    # log.info(f"Streaming Parquet output to {output_path_parquet}")
    # query_parquet = iot_df.writeStream \
    #     .format("parquet") \
    #     .option("path", output_path_parquet) \
    #     .option("checkpointLocation", "s3a://iot.bucket/checkpoints/parquet") \
    #     .trigger(processingTime="30 seconds") \
    #     .outputMode("append") \
    #     .start()

    # Keep running for a fixed time window (e.g. 20s for testing)
    query_json.awaitTermination()
    # query_parquet.awaitTermination(20)

    log.info("Kafka streaming job completed successfully.")

except Exception as e:
    log.error(f"Kafka ETL job failed: {e}", exc_info=True)
    raise
finally:
    if spark:
        spark.stop()
        log.info("Spark session stopped.")

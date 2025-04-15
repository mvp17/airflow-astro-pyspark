from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE


KAFKA_TOPIC = "iot_data"
KAFKA_BROKER = "kafka:9092"

spark = SparkSession.builder \
    .appName("KafkaToMinIO") \
    .getOrCreate()

configure_minio(spark)

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
iot_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data from Kafka
iot_df = iot_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), iot_schema).alias("data")) \
    .select("data.*")
    
# MinIO bucket setup
bucket_name = "iot.bucket"
if not MINIO_CLIENT.bucket_exists(bucket_name):
    MINIO_CLIENT.make_bucket(bucket_name)

output_path_json = f"s3a://{bucket_name}/iot_data/{CURRENT_DATE}.json"
output_path_parquet = f"s3a://{bucket_name}/iot_data/{CURRENT_DATE}.parquet"

# Write to MinIO (JSON)
query_json = iot_df.writeStream \
    .format("json") \
    .option("path", output_path_json) \
    .option("checkpointLocation", "s3a://iot.bucket/checkpoints/json") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

# Write to MinIO (Parquet)
query_parquet = iot_df.writeStream \
    .format("parquet") \
    .option("path", output_path_parquet) \
    .option("checkpointLocation", "s3a://iot.bucket/checkpoints/parquet") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

query_json.awaitTermination(20)
query_parquet.awaitTermination(20)

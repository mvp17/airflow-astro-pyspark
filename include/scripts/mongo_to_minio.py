# from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE
from pyspark.sql import functions as F
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

# Get Mongo URI from Airflow Variables
MONGO_URI = Variable.get("MONGO_URI", default_var=None)
if not MONGO_URI:
    raise ValueError("Airflow Variable 'MONGO_URI' is not set.")

spark = None
try:
    # Start Spark session
    spark = SparkSession.builder \
        .appName("MongoToMinIO") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .getOrCreate()

    configure_minio(spark)
    log.info("Spark session created and MinIO configured.")

    # Read data from MongoDB
    installations_df = spark.read.format("mongodb") \
        .option("database", "mydb") \
        .option("collection", "installations") \
        .load() \
        .withColumn("source", F.lit("installations"))

    products_df = spark.read.format("mongodb") \
        .option("database", "mydb") \
        .option("collection", "products") \
        .load() \
        .withColumn("source", F.lit("products"))

    log.info("MongoDB DataFrames loaded.")

    # Union datasets
    combined_df = installations_df.unionByName(products_df, allowMissingColumns=True)

    # Ensure MinIO bucket exists
    bucket_name = "mongo.bucket"
    if not MINIO_CLIENT.bucket_exists(bucket_name):
        MINIO_CLIENT.make_bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' created.")
    else:
        log.info(f"Bucket '{bucket_name}' already exists.")

    # Define JSON output path
    json_output_path = f"s3a://{bucket_name}/combined_df/{CURRENT_DATE}.json"
    # parquet_output_path = f"s3a://{bucket_name}/combined_df/{CURRENT_DATE}.parquet"

    # Write data
    log.info(f"Sending combined DataFrame to MinIO at: {json_output_path}")
    combined_df.coalesce(1).write.mode("overwrite").json(json_output_path)
    # combined_df.write.mode("overwrite").parquet(parquet_output_path)
    log.info("Data successfully written to MinIO.")

except Exception as e:
    log.error(f"Mongo ETL job failed: {e}", exc_info=True)
    raise
finally:
    if spark:
        spark.stop()
        log.info("Spark session stopped.")

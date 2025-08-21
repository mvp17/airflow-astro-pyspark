# from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE


log = LoggingMixin().log

MYSQL_URL = Variable.get("MYSQL_URL", default_var=None)
MYSQL_TABLE = Variable.get("MYSQL_TABLE", default_var=None)
MYSQL_USER = Variable.get("MYSQL_USER", default_var=None)
MYSQL_PASSWORD = Variable.get("MYSQL_PASS", default_var=None)

if not all([MYSQL_URL, MYSQL_TABLE, MYSQL_USER, MYSQL_PASSWORD]):
    raise ValueError("One or more MySQL Airflow Variables are missing.")

spark = None
try:
    # Start Spark session
    spark = SparkSession.builder \
        .appName("MySQLToMinIO") \
        .getOrCreate()

    configure_minio(spark)
    log.info("Spark session created and MinIO configured.")

    log.info(f"Connecting to MySQL at {MYSQL_URL}, table {MYSQL_TABLE}")

    # Read data from MySQL
    mysql_df = spark.read.format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    log.info("MySQL DataFrame loaded successfully.")

    # Ensure MinIO bucket exists
    bucket_name = "mysql.bucket"
    if not MINIO_CLIENT.bucket_exists(bucket_name):
        MINIO_CLIENT.make_bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' created.")
    else:
        log.info(f"Bucket '{bucket_name}' already exists.")

    # Define JSON output path
    json_output_path = f"s3a://{bucket_name}/mysql_data/{CURRENT_DATE}.json"
    # parquet_output_path = f"s3a://{bucket_name}/mysql_data/{CURRENT_DATE}.parquet"
    
    mysql_df.write.mode("overwrite").json(json_output_path)
    # mysql_df.write.mode("overwrite").parquet(parquet_output_path)
    log.info("MySQL data successfully written to MinIO.")
    
except Exception as e:
    log.error(f"MySQL ETL job failed: {e}", exc_info=True)
    raise
finally:
    if spark:
        spark.stop()
        log.info("Spark session stopped.")

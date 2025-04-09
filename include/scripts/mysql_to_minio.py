# from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE


MYSQL_URL = Variable.get("MYSQL_URL", default_var=None)
MYSQL_TABLE = Variable.get("MYSQL_TABLE", default_var=None)
MYSQL_USER = Variable.get("MYSQL_USER", default_var=None)
MYSQL_PASSWORD = Variable.get("MYSQL_PASS", default_var=None)

spark = SparkSession.builder \
        .getOrCreate()

configure_minio(spark)
    
# Read data from MySQL
mysql_df = spark.read.format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", MYSQL_TABLE) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

print("MySQL DataFrame loaded.")

bucket_name = "mysql.bucket"
if not MINIO_CLIENT.bucket_exists(bucket_name):
    MINIO_CLIENT.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created.")

json_output_path = f"s3a://{bucket_name}/mysql_data/{CURRENT_DATE}.json"
parquet_output_path = f"s3a://{bucket_name}/mysql_data/{CURRENT_DATE}.parquet"

mysql_df.write.mode("overwrite").json(json_output_path)
mysql_df.write.mode("overwrite").parquet(parquet_output_path)

print("MySQL data successfully written to MinIO.")

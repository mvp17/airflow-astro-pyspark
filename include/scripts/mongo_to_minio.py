# from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.models import Variable
from config import MINIO_CLIENT, configure_minio, CURRENT_DATE


# Get Mongo URI from Airflow Variable
MONGO_URI = Variable.get("MONGO_URI", default_var=None)

spark = SparkSession.builder \
    .appName("MongoToMinIO") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .getOrCreate()

configure_minio(spark)

# Read data from MongoDB
installations_df = spark.read.format("mongodb") \
    .option("database", "mydb") \
    .option("collection", "installations") \
    .load()

products_df = spark.read.format("mongodb") \
    .option("database", "mydb") \
    .option("collection", "products") \
    .load()

print("MongoDB DataFrames loaded.")

combined_df = installations_df.unionByName(products_df, allowMissingColumns=True)

bucket_name = "mongo.bucket"
if not MINIO_CLIENT.bucket_exists(bucket_name):
    MINIO_CLIENT.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created.")

json_output_path = f"s3a://{bucket_name}/combined_df/{CURRENT_DATE}.json"
# parquet_output_path = f"s3a://{bucket_name}/combined_df/{CURRENT_DATE}.parquet"

print("Sending combined DataFrame to MinIO...")
combined_df.write.mode("overwrite").json(json_output_path)
# combined_df.write.mode("overwrite").parquet(parquet_output_path)

print("Data successfully written to MinIO.")

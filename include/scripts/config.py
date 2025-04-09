from minio import Minio
from airflow.models import Variable
from datetime import datetime


MINIO_CLIENT_ENDPOINT="minio:9000"
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY", default_var=None)
MINIO_SEC_KEY = Variable.get("MINIO_SEC_KEY", default_var=None)

MINIO_CLIENT = Minio(
    MINIO_CLIENT_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SEC_KEY,
    secure=False
)

CURRENT_DATE = datetime.now().strftime("%Y%m%d")

def configure_minio(spark):
    """Configure Spark to use MinIO."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SEC_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
    hadoop_conf.set("fs.s3a.connection.timeout", "10000")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    hadoop_conf.set("fs.s3a.multipart.size", "104857600")  # 100 MB
    hadoop_conf.set("fs.s3a.multipart.threshold", "104857600")  # 100 MB

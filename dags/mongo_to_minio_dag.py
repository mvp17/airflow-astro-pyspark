from airflow.decorators import dag
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "mongo", "minio"]
)
def mongo_to_minio_dag():
    spark_task = SparkSubmitOperator(
        task_id="submit_mongo_to_minio_job",
        application="/usr/local/airflow/include/scripts/mongo_to_minio.py",
        jars=",".join([
            "include/jars/mongo-spark-connector_2.12-10.4.0.jar",
            "include/jars/bson-4.10.2.jar",
            "include/jars/mongodb-driver-core-4.10.2.jar",
            "include/jars/mongodb-driver-sync-4.10.2.jar",
            # jars for MinIO
            "include/jars/hadoop-aws-3.3.4.jar",
            "include/jars/aws-java-sdk-bundle-1.12.262.jar"
        ]),
        conn_id="my_spark_conn",  # your Spark connection
        verbose=True
    )

    spark_task

mongo_to_minio_dag()

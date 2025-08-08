from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyspark", "mysql", "minio"]
)
def mysql_to_minio_dag():
    spark_task = SparkSubmitOperator(
        task_id="submit_mysql_to_minio_job",
        application="/usr/local/airflow/include/scripts/mysql_to_minio.py",
        jars=",".join([
            "include/jars/mysql-connector-j-8.4.0.jar",
            # jars for MinIO
            "include/jars/hadoop-aws-3.3.4.jar",
            "include/jars/aws-java-sdk-bundle-1.12.262.jar"
        ]),
        conn_id="my_spark_conn",  # your Spark connection
        verbose=True
    )

    spark_task

mysql_to_minio_dag()
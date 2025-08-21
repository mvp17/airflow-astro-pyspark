from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "spark", "minio", "streaming"]
)
def kafka_to_minio_streaming_dag():
    run_kafka_job = SparkSubmitOperator(
        task_id="submit_kafka_streaming_to_minio",
        application="/usr/local/airflow/include/scripts/kafka_streaming_to_minio.py",
        conn_id="my_spark_conn",
        jars=",".join([
            "include/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar",
            "include/jars/kafka-clients-3.4.1.jar",
            "include/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar",
            "include/jars/hadoop-aws-3.3.4.jar",
            "include/jars/commons-pool2-2.11.1.jar",
            "include/jars/aws-java-sdk-bundle-1.12.262.jar"
        ]),
        verbose=True
    )

    run_kafka_job

kafka_to_minio_streaming_dag()

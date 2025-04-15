from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
import subprocess

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kafka", "producer", "iot"]
)
def kafka_producer_dag():

    @task
    def run_producer_script():
        """Run the kafka_producer.py script from the include folder"""
        result = subprocess.run(
            ['python', '/usr/local/airflow/include/kafka_producer.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Error running producer script: {result.stderr}")
        print("Producer script executed successfully.")
        print(result.stdout)

    # Task to run the producer
    run_producer_script()

kafka_producer_dag()

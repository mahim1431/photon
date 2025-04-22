from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import findspark
findspark.init()

def spark_task():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("AstronomerPySpark") \
        .master("local[*]") \
        .getOrCreate()

    data = [("Alice", 23), ("Bob", 34)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

    spark.stop()

with DAG(
    dag_id="pyspark_script_gui",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pyspark"],
) as dag:

    run_spark = PythonOperator(
        task_id="run_spark_job",
        python_callable=spark_task
    )

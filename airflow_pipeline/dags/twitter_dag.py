import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG 
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from os.path import join
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pathlib import Path

with DAG(dag_id = "TwitterDAG", start_date=days_ago(30), schedule_interval="@daily") as dag:
    BASE_FOLDER = join(
        str(Path("~/Documentos").expanduser()),
        "Desenvolvimento/Curso/Alura - Apache Airflow/Extração de dados/datalake/{stage}/twitter_datascience/{partition}",        
    )

    PARTITION_FOLDER_EXTRACT = "extract_date={{ datainterval_start.strftime('%Y-%m-%d') }}"
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    query = "datascience"
    twitter_operator = TwitterOperator(file_path=join(BASE_FOLDER.format(stage="bronze", partion=PARTITION_FOLDER_EXTRACT),
                                        "datascience_{{ ds_nodash }}.json"), 
                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                            query=query, 
                            task_id="twitter_datascience")
    
    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="src/spark/transformation.py",
                                            name="twitter_transformation",
                                            application_args=["--src", BASE_FOLDER.format(stage="bronze", partion=PARTITION_FOLDER_EXTRACT), 
                                                              "--dest", BASE_FOLDER.format(stage="silver", partion=""),  
                                                              "--process-date", "{{ ds }}"])
twitter_operator >> twitter_transform